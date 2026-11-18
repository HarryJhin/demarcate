use std::sync::Arc;

use crate::BoxFuture;
use crate::error::BoxError;
use crate::synchronization::{self, CompletionStatus, TransactionSynchronization};

/// The point of a transaction at which a listener registered with [`on_transaction`] runs.
///
/// The default is [`TransactionPhase::AfterCommit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransactionPhase {
    /// Before the commit. The listener runs inside the transaction, and an error from it rolls the
    /// transaction back.
    BeforeCommit,
    /// After a successful commit.
    ///
    /// The transaction is over when the listener runs. Data access in the listener is not committed
    /// unless it runs in a [`Propagation::RequiresNew`](crate::Propagation::RequiresNew) transaction.
    /// An error of the listener is logged and does not reach the caller.
    #[default]
    AfterCommit,
    /// After a rollback. An error of the listener is logged and does not reach the caller.
    AfterRollback,
    /// After the transaction completed, whatever the outcome. An error of the listener is logged and
    /// does not reach the caller.
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

/// Registers a listener that runs at the given phase of the current transaction.
///
/// Returns `true` when the listener was registered or run. Returns `false` when no transaction
/// with an active synchronization scope is running and `fallback_execution` is `false`; the
/// listener is then discarded.
///
/// With `fallback_execution` set to `true`, the listener runs immediately in that case.
///
/// The listener belongs to the transaction of the calling future. When that transaction rolls
/// back, an [`AfterCommit`](TransactionPhase::AfterCommit) listener does not run.
///
/// # Errors
///
/// Returns the error of the listener when it ran as a fallback, or an error when the transaction
/// that the calling future joined has already completed.
pub async fn on_transaction<F, Fut>(
    phase: TransactionPhase,
    fallback_execution: bool,
    listener: F,
) -> Result<bool, BoxError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    synchronization::check_access()?;
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
            tracing::warn!("Running an AfterRollback listener as a fallback although no transaction rolled back");
        }
        listener().await?;
        return Ok(true);
    }
    tracing::debug!("No transaction is active - skipping the transactional listener");
    Ok(false)
}
