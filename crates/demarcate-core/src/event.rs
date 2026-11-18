use std::sync::Arc;

use crate::BoxFuture;
use crate::error::BoxError;
use crate::synchronization::{self, CompletionStatus, TransactionSynchronization};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransactionPhase {
    BeforeCommit,
    #[default]
    AfterCommit,
    AfterRollback,
    AfterCompletion,
}

type Listener = Box<dyn Fn() -> BoxFuture<'static, Result<(), BoxError>> + Send + Sync>;

struct TransactionalListenerSynchronization {
    phase: TransactionPhase,
    listener: Listener,
    order: i32,
}

impl TransactionSynchronization for TransactionalListenerSynchronization {
    fn order(&self) -> i32 {
        self.order
    }

    fn before_commit(&self, _read_only: bool) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            if self.phase == TransactionPhase::BeforeCommit {
                (self.listener)().await?;
            }
            Ok(())
        })
    }

    fn after_completion(&self, status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            let run = match self.phase {
                TransactionPhase::BeforeCommit => false,
                TransactionPhase::AfterCommit => status == CompletionStatus::Committed,
                TransactionPhase::AfterRollback => status == CompletionStatus::RolledBack,
                TransactionPhase::AfterCompletion => true,
            };
            if run {
                (self.listener)().await?;
            }
            Ok(())
        })
    }
}

pub async fn on_transaction<F, Fut>(
    phase: TransactionPhase,
    fallback_execution: bool,
    listener: F,
) -> Result<bool, BoxError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    synchronization::check_open()?;
    if synchronization::is_synchronization_active() && synchronization::is_actual_transaction_active() {
        let listener: Listener = Box::new(move || Box::pin(listener()));
        synchronization::register_synchronization(Arc::new(TransactionalListenerSynchronization {
            phase,
            listener,
            order: i32::MAX,
        }))?;
        return Ok(true);
    }
    if fallback_execution {
        if phase == TransactionPhase::AfterRollback {
            tracing::warn!("Processing event after rollback as a fallback execution on AFTER_ROLLBACK phase");
        }
        listener().await?;
        return Ok(true);
    }
    tracing::debug!("No transaction is active - skipping the transactional listener");
    Ok(false)
}
