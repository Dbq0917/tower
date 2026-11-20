//! Service wrapper that spends a shared [`TimeBudget`] on every call.

use super::error::BudgetExhausted;
use super::{Budgeted, HopKind, TimeBudget};
use crate::timeout::error::Elapsed;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};
use tokio::time::Sleep;
use tower_service::Service;

/// Applies the shared request budget to every call of the inner service.
///
/// Before each call (including retries, which simply call the service again)
/// the service:
///
/// 1. grants itself a slice of the budget with
///    [`TimeBudget::grant_for`], reserving time for downstream hops, and
/// 2. arms a timeout for exactly that slice instead of a fixed duration.
///
/// When the call returns, the elapsed time is charged back to the budget with
/// [`TimeBudget::record_attempt`], so the next attempt sees a smaller balance.
#[derive(Debug, Clone)]
pub struct BudgetService<S> {
    inner: S,
    hop: usize,
    kind: HopKind,
    min_hop_reserve: Duration,
}

impl<S> BudgetService<S> {
    /// Wrap `inner` as hop `hop` of the chain.
    ///
    /// `kind` is recorded on every attempt and `min_hop_reserve` is the minimum
    /// amount kept available for each downstream hop.
    pub fn new(inner: S, hop: usize, kind: HopKind, min_hop_reserve: Duration) -> Self {
        BudgetService {
            inner,
            hop,
            kind,
            min_hop_reserve,
        }
    }

    /// Get a reference to the inner service.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S, Request> Service<Request> for BudgetService<S>
where
    S: Service<Request>,
    S::Error: Into<crate::BoxError>,
    Request: Budgeted,
{
    type Response = S::Response;
    type Error = crate::BoxError;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let budget = request.budget().clone();

        let granted = match budget.grant_for(self.hop, self.min_hop_reserve) {
            Ok(granted) => granted,
            Err(exhausted) => return ResponseFuture::exhausted(exhausted),
        };

        let started = Instant::now();
        let response = self.inner.call(request);
        let sleep = tokio::time::sleep(granted);

        ResponseFuture::running(
            response,
            sleep,
            budget,
            self.hop,
            self.kind,
            started,
        )
    }
}

pin_project! {
    #[project = Proj]
    #[derive(Debug)]
    enum State<F> {
        Exhausted {
            error: Option<BudgetExhausted>,
        },
        Running {
            #[pin]
            response: F,
            #[pin]
            sleep: Sleep,
            budget: TimeBudget,
            hop: usize,
            kind: HopKind,
            started: Instant,
        },
    }
}

pin_project! {
    /// Future returned by [`BudgetService`].
    #[derive(Debug)]
    pub struct ResponseFuture<F> {
        #[pin]
        state: State<F>,
    }
}

impl<F> ResponseFuture<F> {
    fn exhausted(error: BudgetExhausted) -> Self {
        ResponseFuture {
            state: State::Exhausted {
                error: Some(error),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn running(
        response: F,
        sleep: Sleep,
        budget: TimeBudget,
        hop: usize,
        kind: HopKind,
        started: Instant,
    ) -> Self {
        ResponseFuture {
            state: State::Running {
                response,
                sleep,
                budget,
                hop,
                kind,
                started,
            },
        }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
    E: Into<crate::BoxError>,
{
    type Output = Result<T, crate::BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        match this.state.as_mut().project() {
            Proj::Exhausted { error } => {
                let exhausted = error.take().expect("polled after completion");
                Poll::Ready(Err(exhausted.into()))
            }
            Proj::Running {
                response,
                sleep,
                budget,
                hop,
                kind,
                started,
            } => {
                if let Poll::Ready(result) = response.poll(cx) {
                    let elapsed = started.elapsed();
                    budget.record_attempt(super::Attempt {
                        hop: *hop,
                        kind: *kind,
                        retryable: kind.is_retryable(),
                        elapsed,
                    });
                    return Poll::Ready(result.map_err(Into::into));
                }

                ready!(sleep.poll(cx));

                let elapsed = started.elapsed();
                budget.record_attempt(super::Attempt {
                    hop: *hop,
                    kind: *kind,
                    retryable: kind.is_retryable(),
                    elapsed,
                });
                Poll::Ready(Err(Elapsed::new().into()))
            }
        }
    }
}
