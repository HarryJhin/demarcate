use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::BoxFuture;
use crate::definition::Isolation;
use crate::error::{BoxError, TransactionError};
use crate::status::Savepoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    Committed,
    RolledBack,
    Unknown,
}

pub trait TransactionSynchronization: Send + Sync {
    fn order(&self) -> i32 {
        i32::MAX
    }
    fn suspend(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async { Ok(()) })
    }
    fn resume(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async { Ok(()) })
    }
    fn resume_abandoned(&self) {}
    fn flush(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
    fn savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
    fn savepoint_rollback<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
    fn before_commit(&self, _read_only: bool) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
    fn before_completion(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
    fn after_commit(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
    fn after_completion(&self, _status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ResourceHolder: Any + Send + Sync {
    fn reset(&self);
    fn unbound(&self);
    fn is_void(&self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceKey {
    Pointer(usize),
    Name(String),
}

impl ResourceKey {
    pub fn of<T: ?Sized>(resource: &Arc<T>) -> Self {
        Self::Pointer(Arc::as_ptr(resource) as *const () as usize)
    }
    pub fn named(name: impl Into<String>) -> Self {
        Self::Name(name.into())
    }
}

#[derive(Clone)]
pub enum BoundResource {
    Value(Arc<dyn Any + Send + Sync>),
    Holder(Arc<dyn ResourceHolder>),
}

impl BoundResource {
    pub fn value<T: Any + Send + Sync>(value: Arc<T>) -> Self {
        Self::Value(value)
    }
    pub fn holder<T: ResourceHolder>(holder: Arc<T>) -> Self {
        Self::Holder(holder)
    }
    pub fn is_void(&self) -> bool {
        matches!(self, Self::Holder(holder) if holder.is_void())
    }
    pub fn downcast<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let any: Arc<dyn Any + Send + Sync> = match self {
            Self::Value(value) => value.clone(),
            Self::Holder(holder) => holder.clone(),
        };
        any.downcast::<T>().ok()
    }
}

#[derive(Default)]
struct State {
    resources: HashMap<ResourceKey, BoundResource>,
    synchronizations: Option<Vec<Arc<dyn TransactionSynchronization>>>,
    current_transaction_name: Option<String>,
    current_transaction_read_only: bool,
    current_transaction_isolation_level: Option<Isolation>,
    actual_transaction_active: bool,
    scopes: Vec<u64>,
    poisoned: Vec<u64>,
    next_scope: u64,
    polling: Vec<u64>,
    current: Option<u64>,
}

tokio::task_local! {
    static STATE: RefCell<State>;
}

const NO_CONTEXT: &str = "No transaction context is bound to the current task. Call this inside a \
     #[transactional] function or TransactionTemplate::execute, or wrap the future in \
     demarcate_core::synchronization::scope. A task created with tokio::spawn does not inherit the \
     context of the task that spawned it; use demarcate_core::synchronization::spawn and open a new \
     transaction inside it";

fn foreign(state: &State) -> bool {
    state.polling.last().is_some_and(|innermost| state.current != Some(*innermost))
}

fn read<R>(default: R, f: impl FnOnce(&State) -> R) -> R {
    STATE
        .try_with(|state| {
            let state = state.borrow();
            if foreign(&state) { None } else { Some(f(&state)) }
        })
        .ok()
        .flatten()
        .unwrap_or(default)
}

fn write<R>(f: impl FnOnce(&mut State) -> Result<R, TransactionError>) -> Result<R, TransactionError> {
    STATE
        .try_with(|state| {
            let mut state = state.borrow_mut();
            if foreign(&state) {
                return Err(TransactionError::InvalidState(CONCURRENT.into()));
            }
            f(&mut state)
        })
        .unwrap_or_else(|_| Err(TransactionError::InvalidState(NO_CONTEXT.into())))
}

fn bind(state: &mut State, key: ResourceKey, value: BoundResource) -> Option<BoundResource> {
    state.resources.insert(key, value).filter(|old| !old.is_void())
}

fn unbind(state: &mut State, key: &ResourceKey) -> Option<BoundResource> {
    state.resources.remove(key).filter(|value| !value.is_void())
}

pub fn is_context_active() -> bool {
    STATE.try_with(|_| ()).is_ok()
}

pub async fn scope<F: Future>(future: F) -> F::Output {
    STATE.scope(RefCell::new(State::default()), future).await
}

pub(crate) const CONCURRENT: &str = "Another transactional call is already running on this task. Two \
     transactional calls were started concurrently on one task (for example with join! or select!), and one \
     task has one transaction context. Await transactional calls one after another, or run each in its own task \
     with demarcate_core::synchronization::spawn";

pub fn check_boundary_entry() -> Result<(), TransactionError> {
    let concurrent = STATE.try_with(|state| foreign(&state.borrow())).unwrap_or(false);
    if concurrent { Err(TransactionError::InvalidState(CONCURRENT.into())) } else { Ok(()) }
}

pub struct BoundaryScope<F> {
    boundary: Option<u64>,
    future: std::pin::Pin<Box<F>>,
}

pub fn boundary_scope<F: Future>(boundary: Option<u64>, future: F) -> BoundaryScope<F> {
    if let Some(id) = boundary {
        let _ = STATE.try_with(|state| state.borrow_mut().polling.push(id));
    }
    BoundaryScope { boundary, future: Box::pin(future) }
}

impl<F: Future> Future for BoundaryScope<F> {
    type Output = F::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let Some(id) = self.boundary else {
            return self.future.as_mut().poll(cx);
        };
        let previous = STATE.try_with(|state| state.borrow_mut().current.replace(id)).ok().flatten();
        struct Reset(Option<u64>);
        impl Drop for Reset {
            fn drop(&mut self) {
                let _ = STATE.try_with(|state| state.borrow_mut().current = self.0);
            }
        }
        let _reset = Reset(previous);
        self.future.as_mut().poll(cx)
    }
}

impl<F> Drop for BoundaryScope<F> {
    fn drop(&mut self) {
        if let Some(id) = self.boundary {
            let _ = STATE.try_with(|state| state.borrow_mut().polling.retain(|polling| *polling != id));
        }
    }
}

pub(crate) const OUT_OF_ORDER: &str = "Transaction boundaries on this task completed out of order. Two \
     transactional calls ran concurrently on one task (for example with join! or select!), so they shared one \
     task-bound transaction context. The transaction was rolled back. Await transactional calls one after \
     another, or run each in its own task with demarcate_core::synchronization::spawn";

pub(crate) fn enter_boundary() -> Option<u64> {
    STATE
        .try_with(|state| {
            let mut state = state.borrow_mut();
            state.next_scope += 1;
            let id = state.next_scope;
            state.scopes.push(id);
            id
        })
        .ok()
}

pub(crate) fn boundary_in_order(id: Option<u64>) -> bool {
    let Some(id) = id else { return true };
    read(true, |state| state.scopes.last() == Some(&id) && !state.poisoned.contains(&id))
}

pub(crate) fn exit_boundary(id: Option<u64>) {
    let Some(id) = id else { return };
    let _ = STATE.try_with(|state| {
        let mut state = state.borrow_mut();
        state.poisoned.retain(|poisoned| *poisoned != id);
        if let Some(position) = state.scopes.iter().position(|scope| *scope == id) {
            let later: Vec<u64> = state.scopes.drain(position..).skip(1).collect();
            state.scopes.extend(later.iter().copied());
            state.poisoned.extend(later);
        }
    });
}

pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(scope(future))
}

pub async fn ensure_scope<F: Future>(future: F) -> F::Output {
    if is_context_active() { future.await } else { scope(future).await }
}

pub fn resource_keys() -> Vec<ResourceKey> {
    read(Vec::new(), |state| state.resources.keys().cloned().collect())
}

pub fn has_resource(key: &ResourceKey) -> bool {
    resource(key).is_some()
}

pub fn resource(key: &ResourceKey) -> Option<BoundResource> {
    STATE
        .try_with(|state| {
            let mut state = state.borrow_mut();
            if foreign(&state) {
                return None;
            }
            let value = state.resources.get(key).cloned()?;
            if value.is_void() {
                state.resources.remove(key);
                return None;
            }
            Some(value)
        })
        .ok()
        .flatten()
}

pub fn bind_resource(key: ResourceKey, value: BoundResource) -> Result<(), TransactionError> {
    write(|state| match bind(state, key.clone(), value) {
        Some(_) => Err(TransactionError::InvalidState(format!("Already value for key [{key:?}] bound to task"))),
        None => Ok(()),
    })
}

pub fn bind_synchronized_resource(key: ResourceKey, value: BoundResource) -> Result<(), TransactionError> {
    write(|state| {
        if state.synchronizations.is_none() {
            return Err(TransactionError::InvalidState("Transaction synchronization is not active".into()));
        }
        let old_value = bind(state, key.clone(), value.clone());
        let synchronization: Arc<dyn TransactionSynchronization> =
            Arc::new(SynchronizedResource { key, value, old_value });
        state.synchronizations.as_mut().expect("checked above").push(synchronization);
        Ok(())
    })
}

pub fn unbind_resource(key: &ResourceKey) -> Result<BoundResource, TransactionError> {
    write(|state| {
        unbind(state, key)
            .ok_or_else(|| TransactionError::InvalidState(format!("No value for key [{key:?}] bound to task")))
    })
}

pub fn unbind_resource_if_possible(key: &ResourceKey) -> Option<BoundResource> {
    write(|state| Ok(unbind(state, key))).ok().flatten()
}

pub fn is_synchronization_active() -> bool {
    read(false, |state| state.synchronizations.is_some())
}

pub fn init_synchronization() -> Result<(), TransactionError> {
    write(|state| {
        if state.synchronizations.is_some() {
            return Err(TransactionError::InvalidState(
                "Cannot activate transaction synchronization - already active".into(),
            ));
        }
        state.synchronizations = Some(Vec::new());
        Ok(())
    })
}

pub fn register_synchronization(
    synchronization: Arc<dyn TransactionSynchronization>,
) -> Result<(), TransactionError> {
    write(|state| {
        let synchronizations = state
            .synchronizations
            .as_mut()
            .ok_or_else(|| TransactionError::InvalidState("Transaction synchronization is not active".into()))?;
        if !synchronizations.iter().any(|existing| Arc::ptr_eq(existing, &synchronization)) {
            synchronizations.push(synchronization);
        }
        Ok(())
    })
}

pub fn synchronizations() -> Result<Vec<Arc<dyn TransactionSynchronization>>, TransactionError> {
    write(|state| {
        let mut synchronizations = state
            .synchronizations
            .clone()
            .ok_or_else(|| TransactionError::InvalidState("Transaction synchronization is not active".into()))?;
        synchronizations.sort_by_key(|synchronization| synchronization.order());
        Ok(synchronizations)
    })
}

pub fn clear_synchronization() -> Result<(), TransactionError> {
    write(|state| {
        if state.synchronizations.take().is_none() {
            return Err(TransactionError::InvalidState(
                "Cannot deactivate transaction synchronization - not active".into(),
            ));
        }
        Ok(())
    })
}

pub fn set_current_transaction_name(name: Option<String>) {
    let _ = write(|state| {
        state.current_transaction_name = name;
        Ok(())
    });
}

pub fn current_transaction_name() -> Option<String> {
    read(None, |state| state.current_transaction_name.clone())
}

pub fn set_current_transaction_read_only(read_only: bool) {
    let _ = write(|state| {
        state.current_transaction_read_only = read_only;
        Ok(())
    });
}

pub fn is_current_transaction_read_only() -> bool {
    read(false, |state| state.current_transaction_read_only)
}

pub fn set_current_transaction_isolation_level(isolation_level: Option<Isolation>) {
    let _ = write(|state| {
        state.current_transaction_isolation_level = isolation_level;
        Ok(())
    });
}

pub fn current_transaction_isolation_level() -> Option<Isolation> {
    read(None, |state| state.current_transaction_isolation_level)
}

pub fn set_actual_transaction_active(active: bool) {
    let _ = write(|state| {
        state.actual_transaction_active = active;
        Ok(())
    });
}

pub fn is_actual_transaction_active() -> bool {
    read(false, |state| state.actual_transaction_active)
}

pub fn clear() {
    let _ = write(|state| {
        state.synchronizations = None;
        state.current_transaction_name = None;
        state.current_transaction_read_only = false;
        state.current_transaction_isolation_level = None;
        state.actual_transaction_active = false;
        Ok(())
    });
}

struct SynchronizedResource {
    key: ResourceKey,
    value: BoundResource,
    old_value: Option<BoundResource>,
}

impl TransactionSynchronization for SynchronizedResource {
    fn suspend(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async {
            write(|state| {
                unbind(state, &self.key);
                Ok(())
            })
        })
    }
    fn resume(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async {
            write(|state| match bind(state, self.key.clone(), self.value.clone()) {
                Some(_) => Err(TransactionError::InvalidState(format!(
                    "Unexpected value for key [{:?}] bound on resume",
                    self.key
                ))),
                None => Ok(()),
            })
        })
    }
    fn resume_abandoned(&self) {
        let _ = write(|state| {
            bind(state, self.key.clone(), self.value.clone());
            Ok(())
        });
    }
    fn after_completion(&self, _status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async {
            write(|state| {
                unbind(state, &self.key);
                if let Some(old_value) = &self.old_value {
                    bind(state, self.key.clone(), old_value.clone());
                }
                Ok(())
            })?;
            Ok(())
        })
    }
}

fn callback_error(error: BoxError) -> TransactionError {
    match error.downcast::<TransactionError>() {
        Ok(error) => *error,
        Err(error) => TransactionError::Other(error),
    }
}

pub async fn trigger_flush() -> Result<(), TransactionError> {
    if is_synchronization_active() {
        for synchronization in synchronizations()? {
            synchronization.flush().await.map_err(TransactionError::Other)?;
        }
    }
    Ok(())
}

pub(crate) async fn trigger_savepoint(savepoint: &Savepoint) -> Result<(), TransactionError> {
    if is_synchronization_active() {
        for synchronization in synchronizations()? {
            synchronization.savepoint(savepoint).await.map_err(TransactionError::Other)?;
        }
    }
    Ok(())
}

pub(crate) async fn trigger_savepoint_rollback(savepoint: &Savepoint) -> Result<(), TransactionError> {
    if is_synchronization_active() {
        for synchronization in synchronizations()? {
            synchronization.savepoint_rollback(savepoint).await.map_err(TransactionError::Other)?;
        }
    }
    Ok(())
}

pub async fn trigger_before_commit(read_only: bool) -> Result<(), TransactionError> {
    for synchronization in synchronizations()? {
        synchronization.before_commit(read_only).await.map_err(callback_error)?;
    }
    Ok(())
}

pub async fn trigger_before_completion() -> Result<(), TransactionError> {
    for synchronization in synchronizations()? {
        if let Err(error) = synchronization.before_completion().await {
            tracing::error!(%error, "TransactionSynchronization.beforeCompletion threw exception");
        }
    }
    Ok(())
}

pub async fn trigger_after_commit() -> Result<(), TransactionError> {
    invoke_after_commit(&synchronizations()?).await
}

pub async fn invoke_after_commit(
    synchronizations: &[Arc<dyn TransactionSynchronization>],
) -> Result<(), TransactionError> {
    for synchronization in synchronizations {
        synchronization.after_commit().await.map_err(callback_error)?;
    }
    Ok(())
}

pub async fn trigger_after_completion(completion_status: CompletionStatus) -> Result<(), TransactionError> {
    let synchronizations = synchronizations()?;
    invoke_after_completion(&synchronizations, completion_status).await;
    Ok(())
}

pub async fn invoke_after_completion(
    synchronizations: &[Arc<dyn TransactionSynchronization>],
    completion_status: CompletionStatus,
) {
    for synchronization in synchronizations {
        if let Err(error) = synchronization.after_completion(completion_status).await {
            tracing::error!(%error, "TransactionSynchronization.afterCompletion threw exception");
        }
    }
}
