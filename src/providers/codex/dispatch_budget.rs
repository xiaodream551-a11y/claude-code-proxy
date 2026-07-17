use std::sync::{Arc, Mutex};

pub(crate) const MAX_CODEX_MODEL_DISPATCHES: u32 = 4;
pub(crate) const MAX_CODEX_OAUTH_DISPATCHES: u32 = 2;
pub(crate) const MAX_CODEX_TOTAL_DISPATCHES: u32 = 6;
pub(crate) const CODEX_DISPATCH_BUDGET_DETAIL: &str = "codex_dispatch_budget_exhausted";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexDispatchKind {
    Model,
    OAuthToken,
}

impl std::fmt::Display for CodexDispatchKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Model => f.write_str("model"),
            Self::OAuthToken => f.write_str("OAuth token"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CodexDispatchSnapshot {
    pub(crate) model: u32,
    pub(crate) oauth: u32,
    pub(crate) total: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodexModelDispatchReservation {
    pub(crate) attempt: u32,
}

#[derive(Debug)]
pub(crate) struct CodexDispatchBudgetExceeded {
    pub(crate) kind: CodexDispatchKind,
    pub(crate) snapshot: CodexDispatchSnapshot,
}

impl std::fmt::Display for CodexDispatchBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Codex {} dispatch budget exhausted (model {}/{}, OAuth {}/{}, total {}/{})",
            self.kind,
            self.snapshot.model,
            MAX_CODEX_MODEL_DISPATCHES,
            self.snapshot.oauth,
            MAX_CODEX_OAUTH_DISPATCHES,
            self.snapshot.total,
            MAX_CODEX_TOTAL_DISPATCHES,
        )
    }
}

impl std::error::Error for CodexDispatchBudgetExceeded {}

#[derive(Clone, Default)]
pub(crate) struct CodexDispatchBudget {
    state: Arc<Mutex<CodexDispatchSnapshot>>,
}

impl CodexDispatchBudget {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reserve_model(
        &self,
    ) -> Result<CodexModelDispatchReservation, CodexDispatchBudgetExceeded> {
        let snapshot = self.reserve(CodexDispatchKind::Model)?;
        Ok(CodexModelDispatchReservation {
            attempt: snapshot.model,
        })
    }

    pub(crate) fn reserve_oauth(&self) -> Result<u32, CodexDispatchBudgetExceeded> {
        Ok(self.reserve(CodexDispatchKind::OAuthToken)?.oauth)
    }

    pub(crate) fn can_reserve_model(&self) -> bool {
        let snapshot = self.snapshot();
        snapshot.model < MAX_CODEX_MODEL_DISPATCHES && snapshot.total < MAX_CODEX_TOTAL_DISPATCHES
    }

    pub(crate) fn snapshot(&self) -> CodexDispatchSnapshot {
        *self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn reserve(
        &self,
        kind: CodexDispatchKind,
    ) -> Result<CodexDispatchSnapshot, CodexDispatchBudgetExceeded> {
        let mut snapshot = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let category_available = match kind {
            CodexDispatchKind::Model => snapshot.model < MAX_CODEX_MODEL_DISPATCHES,
            CodexDispatchKind::OAuthToken => snapshot.oauth < MAX_CODEX_OAUTH_DISPATCHES,
        };
        if !category_available || snapshot.total >= MAX_CODEX_TOTAL_DISPATCHES {
            return Err(CodexDispatchBudgetExceeded {
                kind,
                snapshot: *snapshot,
            });
        }

        match kind {
            CodexDispatchKind::Model => snapshot.model += 1,
            CodexDispatchKind::OAuthToken => snapshot.oauth += 1,
        }
        snapshot.total += 1;
        Ok(*snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn concurrent_reservations_preserve_all_three_hard_caps() {
        let budget = CodexDispatchBudget::new();
        let barrier = Arc::new(Barrier::new(65));
        let mut workers = Vec::new();

        for index in 0..64 {
            let budget = budget.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                if index % 2 == 0 {
                    budget.reserve_model().is_ok()
                } else {
                    budget.reserve_oauth().is_ok()
                }
            }));
        }
        barrier.wait();

        let successful = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|reserved| *reserved)
            .count();
        assert_eq!(successful, MAX_CODEX_TOTAL_DISPATCHES as usize);
        assert_eq!(
            budget.snapshot(),
            CodexDispatchSnapshot {
                model: MAX_CODEX_MODEL_DISPATCHES,
                oauth: MAX_CODEX_OAUTH_DISPATCHES,
                total: MAX_CODEX_TOTAL_DISPATCHES,
            }
        );
    }

    #[test]
    fn category_exhaustion_does_not_consume_total_capacity() {
        let budget = CodexDispatchBudget::new();
        for expected in 1..=MAX_CODEX_OAUTH_DISPATCHES {
            assert_eq!(budget.reserve_oauth().unwrap(), expected);
        }

        let error = budget.reserve_oauth().unwrap_err();
        assert_eq!(error.kind, CodexDispatchKind::OAuthToken);
        assert_eq!(error.snapshot.total, MAX_CODEX_OAUTH_DISPATCHES);
        assert_eq!(budget.reserve_model().unwrap().attempt, 1);
    }
}
