use std::any::{Any, TypeId, type_name};
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;

use demarcate_core::{CatchUnwind, PlatformTransactionManager, TransactionError, TransactionStatus, synchronization};

use crate::attribute::TransactionAttribute;
use crate::registry::{MethodRollbackEvent, registry};

pub struct TransactionInfo {
    pub attribute: TransactionAttribute,
    pub joinpoint_identification: String,
    pub status: Arc<dyn TransactionStatus>,
}

impl TransactionInfo {
    pub fn has_transaction(&self) -> bool {
        self.status.has_transaction()
    }
}

pub fn current_transaction_info() -> Option<Arc<TransactionInfo>> {
    synchronization::call_attachment().and_then(|attachment| attachment.downcast::<TransactionInfo>().ok())
}

pub fn current_transaction_status() -> Result<Arc<dyn TransactionStatus>, TransactionError> {
    current_transaction_info().map(|info| info.status.clone()).ok_or_else(|| {
        TransactionError::NoTransaction(
            "No transaction aspect-managed TransactionStatus in scope. current_transaction_status() works only \
             inside a #[transactional] function"
                .into(),
        )
    })
}

pub async fn invoke_within_transaction<T, E, Fut>(
    attribute: TransactionAttribute,
    method_identification: &str,
    invocation: Fut,
) -> Result<T, E>
where
    E: From<TransactionError> + 'static,
    Fut: Future<Output = Result<T, E>>,
{
    let manager = registry().determine_transaction_manager(&attribute)?;
    invoke_with_manager(manager, attribute, method_identification, invocation).await
}

pub async fn invoke_with_manager<T, E, Fut>(
    manager: Arc<dyn PlatformTransactionManager>,
    attribute: TransactionAttribute,
    method_identification: &str,
    invocation: Fut,
) -> Result<T, E>
where
    E: From<TransactionError> + 'static,
    Fut: Future<Output = Result<T, E>>,
{
    let invocation = Box::pin(invocation);
    let invoked = Box::pin(invoke(manager, attribute, method_identification, invocation));
    Box::pin(synchronization::call_scope(invoked)).await
}

async fn invoke<T, E, Fut>(
    manager: Arc<dyn PlatformTransactionManager>,
    mut attribute: TransactionAttribute,
    method_identification: &str,
    invocation: Pin<Box<Fut>>,
) -> Result<T, E>
where
    E: From<TransactionError> + 'static,
    Fut: Future<Output = Result<T, E>>,
{
    let joinpoint_identification = attribute.descriptor.clone().unwrap_or_else(|| method_identification.to_owned());
    if attribute.definition.name.is_none() {
        attribute.definition.name = Some(joinpoint_identification.clone());
    }

    let status = manager.transaction(Some(&attribute.definition)).await?;
    let info = Arc::new(TransactionInfo { attribute, joinpoint_identification, status: status.clone() });
    let restore = synchronization::attach_to_call(info.clone());

    let outcome = CatchUnwind::new(invocation).await;

    match outcome {
        Err(panic) => {
            registry().publish_rollback(&MethodRollbackEvent {
                method: info.joinpoint_identification.clone(),
                failure: "panic",
                transaction: status.clone(),
            });
            if let Err(error) = manager.rollback(status.as_ref()).await {
                tracing::error!(%error, "Application panic overridden by rollback error");
            }
            drop(restore);
            resume_unwind(panic)
        }
        Ok(Err(application)) => {
            let rollback =
                info.attribute.rollback_on_type(Some(&application as &dyn Any), TypeId::of::<E>(), type_name::<E>());
            let completed = if rollback {
                registry().publish_rollback(&MethodRollbackEvent {
                    method: info.joinpoint_identification.clone(),
                    failure: type_name::<E>(),
                    transaction: status.clone(),
                });
                manager.rollback(status.as_ref()).await
            } else {
                manager.commit(status.as_ref()).await
            };
            drop(restore);
            match completed {
                Ok(()) => Err(application),
                Err(error) => {
                    tracing::error!(%error, "Application exception overridden by transaction completion error");
                    Err(E::from(error))
                }
            }
        }
        Ok(Ok(value)) => {
            drop(restore);
            manager.commit(status.as_ref()).await?;
            Ok(value)
        }
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`#[transactional]` needs this function to return `Result<T, E>` where `E: From<demarcate::TransactionError>`, but it returns `{Self}`",
    label = "not a `Result` whose error converts from `TransactionError`",
    note = "on an `impl` block `#[transactional]` applies to every `async fn` in the block; move a method that is not transactional to a separate `impl` block without the attribute",
    note = "implement `From<demarcate::TransactionError>` for the error type so that a failure to begin, commit or roll back can be returned"
)]
pub trait TransactionalReturn {}

impl<T, E: From<TransactionError>> TransactionalReturn for Result<T, E> {}

#[doc(hidden)]
pub const fn assert_transactional_return<R: TransactionalReturn>() {}
