//! Retry policy that stays inside the shared [`TimeBudget`].

use super::{Budgeted, HopKind};
use crate::retry::Policy;
use std::time::Duration;
use tokio::time::Sleep;

/// Decides whether an error may trigger another attempt.
///
/// Not every failure of a read is safe to retry: a read that fails *because a
/// downstream write failed* must not retry, otherwise the write would run again
/// on the next attempt. Classifiers keep that decision next to the application's
/// error type instead of retrying every `Err` blindly.
pub trait RetryClassifier<E> {
    /// Returns `true` when the error is transient and safe to retry.
    fn is_retryable(&self, error: &E) -> bool;
}

impl<F, E> RetryClassifier<E> for F
where
    F: Fn(&E) -> bool,
{
    fn is_retryable(&self, error: &E) -> bool {
        (self)(error)
    }
}

/// Classifier that retries every error.
#[derive(Debug, Clone, Copy, Default)]
pub struct RetryAllErrors;

impl<E> RetryClassifier<E> for RetryAllErrors {
    fn is_retryable(&self, _error: &E) -> bool {
        true
    }
}

/// Retry policy whose attempts are bounded by the request's time budget.
///
/// The policy never owns a clock of its own. A retry is allowed only when:
///
/// * the hop is a [`HopKind::Read`] (a write is never retried, because retrying
///   a non-idempotent write can duplicate it),
/// * the error is classified as retryable (see [`RetryClassifier`]),
/// * the attempt budget `max_attempts` is not exhausted, and
/// * the shared [`TimeBudget`] still has at least `min_attempt_budget` left.
///
/// Time already spent is deducted by [`BudgetService`](super::BudgetService)
/// when each attempt finishes, so the remaining-budget check here automatically
/// accounts for every previous attempt on this hop.
#[derive(Clone)]
pub struct BudgetRetry<E> {
    hop: usize,
    kind: HopKind,
    max_attempts: u32,
    attempts: u32,
    backoff: Duration,
    min_attempt_budget: Duration,
    classifier: std::sync::Arc<dyn RetryClassifier<E> + Send + Sync>,
}

impl<E> std::fmt::Debug for BudgetRetry<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetRetry")
            .field("hop", &self.hop)
            .field("kind", &self.kind)
            .field("max_attempts", &self.max_attempts)
            .field("attempts", &self.attempts)
            .field("backoff", &self.backoff)
            .field("min_attempt_budget", &self.min_attempt_budget)
            .finish()
    }
}

impl<E> BudgetRetry<E> {
    /// Create a policy for hop `hop` that retries every error.
    ///
    /// `max_attempts` counts the initial call, so `max_attempts = 3` allows at
    /// most two retries. `backoff` is slept before every retry; the sleep
    /// consumes wall-clock time, so it naturally shrinks the remaining balance
    /// of the shared deadline. `min_attempt_budget` is the smallest remaining
    /// balance for which another attempt is worthwhile.
    pub fn new(
        hop: usize,
        kind: HopKind,
        max_attempts: u32,
        backoff: Duration,
        min_attempt_budget: Duration,
    ) -> Self
    where
        E: 'static,
    {
        Self::with_classifier(
            hop,
            kind,
            max_attempts,
            backoff,
            min_attempt_budget,
            RetryAllErrors,
        )
    }

    /// Create a policy with an explicit error [`RetryClassifier`].
    pub fn with_classifier<C>(
        hop: usize,
        kind: HopKind,
        max_attempts: u32,
        backoff: Duration,
        min_attempt_budget: Duration,
        classifier: C,
    ) -> Self
    where
        E: 'static,
        C: RetryClassifier<E> + Send + Sync + 'static,
    {
        BudgetRetry {
            hop,
            kind,
            max_attempts,
            attempts: 0,
            backoff,
            min_attempt_budget,
            classifier: std::sync::Arc::new(classifier),
        }
    }

    /// Number of attempts already made on this policy instance.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

impl<Req, Res, E> Policy<Req, Res, E> for BudgetRetry<E>
where
    Req: Budgeted + Clone,
{
    type Future = Sleep;

    fn retry(&mut self, req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future> {
        // Writes are not idempotent: retrying them multiplies the write.
        if !self.kind.is_retryable() {
            return None;
        }

        let error = match result {
            Ok(_) => return None,
            Err(error) => error,
        };

        // Errors surfaced by a non-retryable downstream hop (for example a
        // failed database write) must stop the chain instead of replaying it.
        if !self.classifier.is_retryable(error) {
            return None;
        }

        // `attempts` counts calls already made, including the one being
        // classified here. Consuming the last allowed attempt means no retry.
        self.attempts += 1;
        if self.attempts >= self.max_attempts {
            return None;
        }

        if req.budget().remaining() < self.min_attempt_budget {
            return None;
        }

        Some(tokio::time::sleep(self.backoff))
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}

