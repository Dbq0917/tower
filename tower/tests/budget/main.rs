#![cfg(feature = "budget")]

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tower::budget::{
    BudgetLayer, BudgetRetry, BudgetRoute, Budgeted, HopKind, TimeBudget,
};
use tower::retry::Retry;
use tower::util::service_fn;
use tower::{Layer, Service, ServiceExt};

const TOTAL_BUDGET: Duration = Duration::from_millis(300);
const MIN_RESERVE: Duration = Duration::from_millis(40);
const BACKOFF: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
struct TestError(String);

impl TestError {
    fn new(message: impl Into<String>) -> Self {
        TestError(message.into())
    }

    fn from_boxed(error: Box<dyn Error + Send + Sync>) -> Self {
        TestError(error.to_string())
    }
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for TestError {}

#[derive(Debug, Clone)]
struct ChainRequest {
    budget: TimeBudget,
    counters: Arc<Counters>,
    fail_write: bool,
}

#[derive(Debug, Default)]
struct Counters {
    read_hop_calls: AtomicU32,
    db_writes: AtomicU32,
    downstream_read_calls: AtomicU32,
    // Failures left for the entry read. It lives behind the shared `Arc`
    // because tower replays the original request on every retry: state kept
    // only inside the service closure would be reset between attempts.
    entry_failures_left: AtomicU32,
}

impl Budgeted for ChainRequest {
    fn budget(&self) -> &TimeBudget {
        &self.budget
    }
}

fn three_hop_routes() -> Vec<BudgetRoute> {
    vec![
        BudgetRoute {
            hop: 0,
            kind: HopKind::Read,
        },
        BudgetRoute {
            hop: 1,
            kind: HopKind::Write,
        },
        BudgetRoute {
            hop: 2,
            kind: HopKind::Read,
        },
    ]
}

/// Wrap a budgeted service so its boxed errors become [`TestError`] again.
///
/// [`BudgetService`] erases the inner error type into a boxed error, which
/// would force the retry policy to reason about `dyn Error`. Normalizing back
/// to the test's cloneable error keeps the whole chain at one error type.
#[allow(clippy::type_complexity)]
fn normalize<S>(service: S) -> tower::util::MapErr<S, fn(Box<dyn Error + Send + Sync>) -> TestError>
where
    S: Service<ChainRequest, Response = &'static str, Error = Box<dyn Error + Send + Sync>>,
{
    tower::ServiceBuilder::new()
        .map_err(TestError::from_boxed as fn(_) -> TestError)
        .service(service)
}

fn build_chain() -> impl Service<
    ChainRequest,
    Response = &'static str,
    Error = TestError,
> + Clone {
    // Hop 2: a plain downstream read (idempotent, no retry policy here).
    let downstream = service_fn(|req: ChainRequest| async move {
        req.counters.downstream_read_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok::<_, TestError>("downstream-ok")
    });
    let downstream = normalize(BudgetLayer::new(2, HopKind::Read, MIN_RESERVE).layer(downstream));

    // Hop 1: the database write. Writes are never retried, so a failed write
    // cannot be amplified into multiple rows.
    let db_write = service_fn(|req: ChainRequest| async move {
        req.counters.db_writes.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        if req.fail_write {
            return Err(TestError::new("write failed"));
        }
        Ok("write-ok")
    });
    let db_write = normalize(BudgetLayer::new(1, HopKind::Write, MIN_RESERVE).layer(db_write));

    // Hop 0: entry read, retried while the shared budget still has time.
    let entry = service_fn(move |req: ChainRequest| {
        let downstream = downstream.clone();
        let db_write = db_write.clone();
        async move {
            req.counters.read_hop_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;

            let failures = &req.counters.entry_failures_left;
            let mut current = failures.load(Ordering::SeqCst);
            loop {
                if current == 0 {
                    break;
                }
                match failures.compare_exchange(
                    current,
                    current - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
            if current > 0 {
                return Err(TestError::new("transient read failure"));
            }

            let write_result = db_write.oneshot(req.clone()).await?;
            assert_eq!(write_result, "write-ok");

            let final_result = downstream.oneshot(req).await?;
            Ok(final_result)
        }
    });
    let entry = normalize(BudgetLayer::new(0, HopKind::Read, MIN_RESERVE).layer(entry));

    Retry::new(
        // Only transient read errors are retried. A failure propagated from the
        // write hop is not retryable, so retrying the entry read cannot replay
        // the write.
        BudgetRetry::<TestError>::with_classifier(
            0,
            HopKind::Read,
            3,
            BACKOFF,
            MIN_RESERVE,
            |error: &TestError| error.0 == "transient read failure",
        ),
        entry,
    )
}

fn new_request(entry_failures_left: u32, fail_write: bool) -> ChainRequest {
    ChainRequest {
        budget: TimeBudget::new(TOTAL_BUDGET, &three_hop_routes()),
        counters: Arc::new(Counters {
            entry_failures_left: AtomicU32::new(entry_failures_left),
            ..Counters::default()
        }),
        fail_write,
    }
}

#[tokio::test(start_paused = true)]
async fn budget_is_propagated_across_three_hops_and_retries() {
    let chain = build_chain();

    // The entry read fails twice, so it runs three times (two retries). Under
    // the old fixed-timeout design each attempt could spend the full timeout;
    // here every attempt draws from the same remaining balance.
    let request = new_request(2, false);
    let counters = request.counters.clone();
    let budget = request.budget.clone();

    let start = Instant::now();
    let response = chain.oneshot(request).await;
    let elapsed = start.elapsed();

    assert_eq!(response.expect("chain succeeds"), "downstream-ok");
    assert_eq!(counters.read_hop_calls.load(Ordering::SeqCst), 3);
    assert_eq!(counters.db_writes.load(Ordering::SeqCst), 1);
    assert_eq!(counters.downstream_read_calls.load(Ordering::SeqCst), 1);

    // The whole request stays inside the single shared deadline.
    assert!(
        elapsed <= TOTAL_BUDGET,
        "request spent {:?}, budget was {:?}",
        elapsed,
        TOTAL_BUDGET,
    );

    let snapshot = budget.snapshot();
    let used = snapshot.used;
    assert!(used > Duration::ZERO);
    assert!(
        snapshot.remaining + used <= TOTAL_BUDGET + Duration::from_millis(1),
        "spending must not exceed the starting budget"
    );

    // Every hop's budget is independently inspectable.
    assert_eq!(snapshot.hops.len(), 3);
    assert_eq!(snapshot.hops[0].hop, 0);
    assert_eq!(snapshot.hops[0].kind, HopKind::Read);
    assert_eq!(snapshot.hops[0].attempts, 3);
    assert_eq!(snapshot.hops[1].hop, 1);
    assert_eq!(snapshot.hops[1].kind, HopKind::Write);
    assert_eq!(snapshot.hops[1].attempts, 1);
    assert_eq!(snapshot.hops[2].hop, 2);
    assert_eq!(snapshot.hops[2].kind, HopKind::Read);
    assert_eq!(snapshot.hops[2].attempts, 1);

    println!(
        "three-hop chain: total budget {:?}, elapsed {:?}, used {:?}, remaining {:?}",
        TOTAL_BUDGET, elapsed, used, snapshot.remaining
    );
    for hop in &snapshot.hops {
        println!(
            "  hop {} ({:?}): granted {:?}, used {:?}, attempts {}",
            hop.hop, hop.kind, hop.granted, hop.used, hop.attempts
        );
    }
}

#[tokio::test(start_paused = true)]
async fn failed_db_write_is_not_retried() {
    let chain = build_chain();
    let request = new_request(0, true);
    let counters = request.counters.clone();

    let error = chain.oneshot(request).await.expect_err("write fails");
    println!("failed write produced error: {error}");
    assert!(
        error.to_string().contains("write failed"),
        "unexpected error: {}",
        error,
    );

    // Exactly one row write happened even though the write returned an error:
    // the write hop's policy refuses to retry a non-idempotent operation, and
    // the entry read's classifier does not treat the propagated write error as
    // retryable, so the write is never replayed.
    assert_eq!(counters.db_writes.load(Ordering::SeqCst), 1);
    assert_eq!(counters.read_hop_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn one_hop_cannot_consume_the_whole_budget() {
    // A single greedy hop that sleeps far longer than the whole budget. The
    // reserve for downstream hops caps its grant, so it times out early.
    let routes = vec![
        BudgetRoute {
            hop: 0,
            kind: HopKind::Read,
        },
        BudgetRoute {
            hop: 1,
            kind: HopKind::Read,
        },
    ];
    let budget = TimeBudget::new(TOTAL_BUDGET, &routes);

    let greedy = service_fn(|_req: ChainRequest| async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok::<_, TestError>("never")
    });
    let greedy = BudgetLayer::new(0, HopKind::Read, MIN_RESERVE).layer(greedy);

    let request = ChainRequest {
        budget: budget.clone(),
        counters: Arc::new(Counters::default()),
        fail_write: false,
    };

    let start = Instant::now();
    let error = greedy.oneshot(request).await.expect_err("times out");
    let elapsed = start.elapsed();

    assert!(error.to_string().contains("timed out"));
    let capped_grant = TOTAL_BUDGET - MIN_RESERVE;
    assert!(
        elapsed <= capped_grant + Duration::from_millis(5),
        "hop spent {:?}, grant was capped around {:?}",
        elapsed,
        capped_grant,
    );

    // The reserve for the downstream hop survived.
    assert!(budget.remaining() >= MIN_RESERVE - Duration::from_millis(5));
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_short_circuits_before_the_hop_runs() {
    let routes = three_hop_routes();
    let budget = TimeBudget::new(TOTAL_BUDGET, &routes);
    // Spend the entire budget before the chain runs.
    budget.record_attempt(tower::budget::Attempt {
        hop: 0,
        kind: HopKind::Read,
        retryable: true,
        elapsed: TOTAL_BUDGET,
    });

    let noop = service_fn(|_req: ChainRequest| async move { Ok::<_, TestError>("ok") });
    let noop = BudgetLayer::new(0, HopKind::Read, MIN_RESERVE).layer(noop);

    let request = ChainRequest {
        budget,
        counters: Arc::new(Counters::default()),
        fail_write: false,
    };

    let error = noop.oneshot(request).await.expect_err("no budget");
    assert!(error.to_string().contains("time budget exhausted"));
}

#[test]
fn retry_deducts_time_already_spent_from_the_shared_budget() {
    use tower::budget::Attempt;

    let routes = vec![
        BudgetRoute {
            hop: 0,
            kind: HopKind::Read,
        },
        BudgetRoute {
            hop: 1,
            kind: HopKind::Read,
        },
    ];
    let budget = TimeBudget::new(Duration::from_millis(100), &routes);

    // First attempt spends 40ms and fails.
    let first = budget.grant_for(0, Duration::from_millis(20)).unwrap();
    assert_eq!(first, Duration::from_millis(80));
    budget.record_attempt(Attempt {
        hop: 0,
        kind: HopKind::Read,
        retryable: true,
        elapsed: Duration::from_millis(40),
    });

    // The retry only sees the 60ms that is actually left; it cannot restart a
    // fresh 100ms (which is what made the old chain spend 3x the timeout).
    assert_eq!(budget.remaining(), Duration::from_millis(60));
    let second = budget.grant_for(0, Duration::from_millis(20)).unwrap();
    assert_eq!(second, Duration::from_millis(40));
    budget.record_attempt(Attempt {
        hop: 0,
        kind: HopKind::Read,
        retryable: true,
        elapsed: Duration::from_millis(30),
    });

    assert_eq!(budget.remaining(), Duration::from_millis(30));

    // The downstream reserve still protects the last hop.
    let downstream = budget.grant_for(1, Duration::from_millis(20)).unwrap();
    assert_eq!(downstream, Duration::from_millis(30));

    let snapshot = budget.snapshot();
    assert_eq!(snapshot.hops[0].attempts, 2);
    assert_eq!(snapshot.hops[0].used, Duration::from_millis(70));
    assert_eq!(snapshot.hops[1].attempts, 0);
}

#[tokio::test]
async fn real_time_comparison_against_fixed_timeout_chain() {
    use tower::retry::Policy;
    use tower::timeout::TimeoutLayer;

    // Each attempt takes 45ms. The fixed timeout lets every attempt run for the
    // full 50ms, so three attempts cost ~135ms even though the request's total
    // budget is only 100ms. The budgeted chain must stop at the deadline
    // instead of silently exceeding it.
    const PER_ATTEMPT: Duration = Duration::from_millis(45);
    const FIXED_TIMEOUT: Duration = Duration::from_millis(50);
    const MAX_ATTEMPTS: u32 = 3;
    const TOTAL: Duration = Duration::from_millis(100);

    // A read that always fails after PER_ATTEMPT. It is a named, cloneable
    // service because `Retry` stores the service in its response future.
    #[derive(Clone)]
    struct AlwaysFails;

    impl Service<ChainRequest> for AlwaysFails {
        type Response = &'static str;
        type Error = TestError;
        type Future =
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<&'static str, TestError>>>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ChainRequest) -> Self::Future {
            Box::pin(async move {
                tokio::time::sleep(PER_ATTEMPT).await;
                Err(TestError::new("boom"))
            })
        }
    }

    let always_fails = AlwaysFails;

    // Old shape: retry wraps a fixed timeout. Every attempt arms a brand-new
    // timeout, so the request is allowed up to MAX_ATTEMPTS * FIXED_TIMEOUT.
    #[derive(Clone)]
    struct FixedAttempts(u32);

    impl Policy<ChainRequest, &'static str, TestError> for FixedAttempts {
        type Future = std::future::Ready<()>;

        fn retry(
            &mut self,
            _req: &mut ChainRequest,
            result: &mut Result<&'static str, TestError>,
        ) -> Option<Self::Future> {
            if result.is_ok() {
                return None;
            }
            if self.0 == 0 {
                return None;
            }
            self.0 -= 1;
            Some(std::future::ready(()))
        }

        fn clone_request(&mut self, req: &ChainRequest) -> Option<ChainRequest> {
            Some(req.clone())
        }
    }

    let old_timed = TimeoutLayer::new(FIXED_TIMEOUT).layer(always_fails.clone());
    let old_timed = normalize(old_timed);
    let old_chain = Retry::new(FixedAttempts(MAX_ATTEMPTS - 1), old_timed);

    let old_request = ChainRequest {
        budget: TimeBudget::new(TOTAL, &three_hop_routes()),
        counters: Arc::new(Counters::default()),
        fail_write: false,
    };
    let start = Instant::now();
    let old_result = old_chain.oneshot(old_request).await;
    let old_elapsed = start.elapsed();
    assert!(old_result.is_err());

    // New shape: the budget is the single source of truth.
    let new_budget = TimeBudget::new(TOTAL, &three_hop_routes());
    let new_inner = BudgetLayer::new(0, HopKind::Read, Duration::from_millis(1)).layer(always_fails);
    let new_inner = normalize(new_inner);
    let new_chain = Retry::new(
        BudgetRetry::<TestError>::new(
            0,
            HopKind::Read,
            MAX_ATTEMPTS,
            Duration::ZERO,
            Duration::from_millis(1),
        ),
        new_inner,
    );

    let new_request = ChainRequest {
        budget: new_budget.clone(),
        counters: Arc::new(Counters::default()),
        fail_write: false,
    };
    let start = Instant::now();
    let new_result = new_chain.oneshot(new_request).await;
    let new_elapsed = start.elapsed();
    assert!(new_result.is_err());

    // The budgeted chain can never exceed its single deadline. The fixed chain
    // is permitted to spend roughly one timeout per attempt, so it overshoots.
    assert!(
        new_elapsed <= TOTAL + Duration::from_millis(10),
        "budgeted chain overshot deadline: {:?} vs {:?}",
        new_elapsed,
        TOTAL
    );
    assert!(
        old_elapsed > new_elapsed,
        "fixed chain {:?} should exceed budgeted chain {:?}",
        old_elapsed,
        new_elapsed
    );
    assert!(new_budget.remaining() < TOTAL);

    println!(
        "fixed-timeout chain elapsed {:?} vs budgeted chain elapsed {:?} (deadline {:?})",
        old_elapsed,
        new_elapsed,
        TOTAL
    );
}
