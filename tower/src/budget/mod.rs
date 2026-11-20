//! Deadline-aware time budgets shared across every hop and retry in a call chain.
//!
//! The standalone [`timeout`](crate::timeout) and [`retry`](crate::retry)
//! middlewares each keep their own clock: a timeout starts a fresh
//! [`Duration`] on every call, and a retry re-arms that timeout on every
//! attempt. When they are chained, the attempts of one request can each run for
//! the full timeout, so a request that is retried twice is allowed to spend up
//! to three times the configured timeout.
//!
//! The types in this module make the *remaining* time explicit instead. A
//! single [`TimeBudget`] is created for the whole request and carried inside the
//! request itself (see [`Budgeted`]). Every hop calls
//! [`TimeBudget::grant_for`] before doing work, which:
//!
//! * hands back only the time still left before the shared deadline, and
//! * reserves a minimum slice for every downstream hop, so a single hop can
//!   never consume the budget needed by the rest of the chain.
//!
//! When a hop finishes (either successfully or with an error) it calls
//! [`TimeBudget::record_attempt`] with the elapsed time. Retries therefore do
//! not "recompute" a fresh budget: the next attempt, and the next hop, only see
//! whatever time the shared deadline still has.

mod error;
mod layer;
mod policy;
mod service;

use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use self::error::BudgetExhausted;
pub use self::layer::BudgetLayer;
pub use self::policy::{BudgetRetry, RetryAllErrors, RetryClassifier};
pub use self::service::BudgetService;

/// What kind of work a hop performs.
///
/// The kind decides how time spent by the hop is charged to the shared budget
/// and whether it is safe to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopKind {
    /// An idempotent read (for example an HTTP `GET` or a `SELECT`).
    ///
    /// Reads are safe to retry; each failed attempt is charged for the time it
    /// actually spent.
    Read,
    /// A non-idempotent write (for example an `INSERT`/`UPDATE` or an HTTP
    /// `POST` that creates a row).
    ///
    /// Retrying a write can duplicate the write, so the budget records the
    /// attempt as non-retryable. [`BudgetRetry`] never retries this kind.
    Write,
}

impl HopKind {
    /// Returns `true` if retrying this hop cannot duplicate side effects.
    pub fn is_retryable(self) -> bool {
        matches!(self, HopKind::Read)
    }
}

/// Static description of one hop in the planned call chain.
#[derive(Debug, Clone, Copy)]
pub struct BudgetRoute {
    /// Position of the hop, starting at zero for the entry point.
    pub hop: usize,
    /// What kind of work the hop performs.
    pub kind: HopKind,
}

/// Time actually spent on a single attempt of a hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attempt {
    /// Hop the attempt ran on.
    pub hop: usize,
    /// Kind of the hop.
    pub kind: HopKind,
    /// Whether this attempt is allowed to be retried.
    pub retryable: bool,
    /// Elapsed wall-clock time charged to the shared budget.
    pub elapsed: Duration,
}

/// A point-in-time view of a [`TimeBudget`], used for observability.
#[derive(Debug, Clone)]
pub struct BudgetSnapshot {
    /// Total budget the request started with.
    pub total: Duration,
    /// Time still available before the shared deadline.
    pub remaining: Duration,
    /// Sum of every attempt recorded so far.
    pub used: Duration,
    /// Per-hop grant and usage, indexed by hop position.
    pub hops: Vec<HopSnapshot>,
}

/// A point-in-time view of one hop's share of a [`TimeBudget`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HopSnapshot {
    /// Position of the hop.
    pub hop: usize,
    /// Kind of the hop.
    pub kind: HopKind,
    /// Time granted to the hop for its current attempt.
    pub granted: Duration,
    /// Time the hop has charged across all of its attempts.
    pub used: Duration,
    /// Number of attempts (initial call plus retries) recorded for the hop.
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy)]
struct HopState {
    kind: HopKind,
    granted: Duration,
    used: Duration,
    attempts: u32,
}

#[derive(Debug)]
struct Inner {
    total: Duration,
    remaining: Duration,
    hops: Vec<HopState>,
}

/// A shared, deadline-aware time budget for one request.
///
/// Cloning a `TimeBudget` clones the [`Arc`], so every middleware and hop sees
/// the same remaining balance. The budget is mutated through interior
/// mutability and does not require the request to be mutable.
#[derive(Debug, Clone)]
pub struct TimeBudget {
    inner: Arc<Mutex<Inner>>,
}

impl TimeBudget {
    /// Create a budget covering the given chain with `total` to spend.
    ///
    /// `routes` must list every hop in call order; the length of the route is
    /// used when reserving time for downstream hops.
    pub fn new(total: Duration, routes: &[BudgetRoute]) -> Self {
        let hops = routes
            .iter()
            .map(|route| HopState {
                kind: route.kind,
                granted: Duration::ZERO,
                used: Duration::ZERO,
                attempts: 0,
            })
            .collect();

        TimeBudget {
            inner: Arc::new(Mutex::new(Inner {
                total,
                remaining: total,
                hops,
            })),
        }
    }

    /// Grant time for one attempt at `hop`, reserving a minimum slice for each
    /// downstream hop.
    ///
    /// The grant is the smaller of:
    ///
    /// * the time still left before the shared deadline, and
    /// * whatever remains after reserving `min_hop_reserve` for every hop that
    ///   follows this one.
    ///
    /// Returns [`BudgetExhausted`] when no time is left, including the case
    /// where an earlier hop has already eaten the reserve meant for this hop.
    pub fn grant_for(
        &self,
        hop: usize,
        min_hop_reserve: Duration,
    ) -> Result<Duration, BudgetExhausted> {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;

        let hops_after = inner
            .hops
            .len()
            .checked_sub(hop + 1)
            .ok_or(BudgetExhausted::new(hop))?;
        let reserved = min_hop_reserve.saturating_mul(hops_after as u32);
        let available = inner
            .remaining
            .checked_sub(reserved)
            .unwrap_or(Duration::ZERO);

        if available.is_zero() {
            return Err(BudgetExhausted::new(hop));
        }

        inner.hops[hop].granted = available;
        Ok(available)
    }

    /// Charge `elapsed` time to `hop` after an attempt finished.
    ///
    /// The same amount is removed from the shared remaining balance, so later
    /// retries and downstream hops cannot spend it again.
    pub fn record_attempt(&self, attempt: Attempt) {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;

        inner.remaining = inner.remaining.saturating_sub(attempt.elapsed);
        let state = &mut inner.hops[attempt.hop];
        state.used += attempt.elapsed;
        state.attempts += 1;
    }

    /// Time still available before the shared deadline.
    pub fn remaining(&self) -> Duration {
        self.inner.lock().unwrap().remaining
    }

    /// Total budget the request started with.
    pub fn total(&self) -> Duration {
        self.inner.lock().unwrap().total
    }

    /// Snapshot the grant and usage of every hop.
    pub fn snapshot(&self) -> BudgetSnapshot {
        let inner = self.inner.lock().unwrap();
        let used = inner
            .hops
            .iter()
            .fold(Duration::ZERO, |acc, hop| acc + hop.used);
        let hops = inner
            .hops
            .iter()
            .enumerate()
            .map(|(hop, state)| HopSnapshot {
                hop,
                kind: state.kind,
                granted: state.granted,
                used: state.used,
                attempts: state.attempts,
            })
            .collect();

        BudgetSnapshot {
            total: inner.total,
            remaining: inner.remaining,
            used,
            hops,
        }
    }
}

/// Requests that carry a [`TimeBudget`] through the call chain.
///
/// Middleware such as [`BudgetService`] and [`BudgetRetry`] uses this trait to
/// reach the shared budget without knowing anything else about the request.
pub trait Budgeted {
    /// Shared time budget of the request.
    fn budget(&self) -> &TimeBudget;
}
