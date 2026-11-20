//! Error types for deadline-aware time budgets.

use std::{error, fmt};

/// Error returned when a hop has no time left in the shared [`TimeBudget`].
///
/// [`TimeBudget`]: super::TimeBudget
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetExhausted {
    hop: usize,
}

impl BudgetExhausted {
    pub(super) fn new(hop: usize) -> Self {
        BudgetExhausted { hop }
    }

    /// Position of the hop that could not be granted any time.
    pub fn hop(&self) -> usize {
        self.hop
    }
}

impl fmt::Display for BudgetExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "time budget exhausted before hop {}", self.hop)
    }
}

impl error::Error for BudgetExhausted {}
