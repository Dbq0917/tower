//! Layer that wraps services in a [`BudgetService`].

use super::{BudgetService, HopKind};
use std::time::Duration;
use tower_layer::Layer;

/// Applies the shared request time budget to the wrapped service.
#[derive(Debug, Clone, Copy)]
pub struct BudgetLayer {
    hop: usize,
    kind: HopKind,
    min_hop_reserve: Duration,
}

impl BudgetLayer {
    /// Create a layer for hop `hop` of the chain.
    ///
    /// See [`BudgetService::new`] for the meaning of `kind` and
    /// `min_hop_reserve`.
    pub const fn new(hop: usize, kind: HopKind, min_hop_reserve: Duration) -> Self {
        BudgetLayer {
            hop,
            kind,
            min_hop_reserve,
        }
    }
}

impl<S> Layer<S> for BudgetLayer {
    type Service = BudgetService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BudgetService::new(inner, self.hop, self.kind, self.min_hop_reserve)
    }
}
