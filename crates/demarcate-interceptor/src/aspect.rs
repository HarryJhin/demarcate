use std::any::{TypeId, type_name};
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use demarcate_core::{PlatformTransactionManager, TransactionError, TransactionStatus, synchronization};

use crate::attribute::TransactionAttribute;
use crate::registry::registry;

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

tokio::task_local! {
    static INFO: RefCell<Vec<Arc<TransactionInfo>>>;
}

pub fn current_transaction_info() -> Option<Arc<TransactionInfo>> {
    INFO.try_with(|stack| stack.borrow().last().cloned()).ok().flatten()
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

struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        let _ = INFO.try_with(|stack| stack.borrow_mut().pop());
    }
}

struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::thread::Result<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match catch_unwind(AssertUnwindSafe(|| self.0.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
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
    Box::pin(synchronization::ensure_scope(async move {
        if INFO.try_with(|_| ()).is_ok() { invoked.await } else { INFO.scope(RefCell::new(Vec::new()), invoked).await }
    }))
    .await
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
    let joinpoint_identification =
        attribute.descriptor.clone().unwrap_or_else(|| method_identification.to_owned());
    if attribute.definition.name.is_none() {
        attribute.definition.name = Some(joinpoint_identification.clone());
    }

    let status = manager.transaction(Some(&attribute.definition)).await?;
    let rollback = attribute.rollback_on_type(TypeId::of::<E>(), type_name::<E>());
    INFO.with(|stack| {
        stack.borrow_mut().push(Arc::new(TransactionInfo {
            attribute,
            joinpoint_identification,
            status: status.clone(),
        }))
    });
    let restore = Restore;

    let outcome = CatchUnwind(invocation).await;

    match outcome {
        Err(panic) => {
            if let Err(error) = manager.rollback(status.as_ref()).await {
                tracing::error!(%error, "Application panic overridden by rollback error");
            }
            drop(restore);
            resume_unwind(panic)
        }
        Ok(Err(application)) => {
            let completed = if rollback {
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
