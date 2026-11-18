use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

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

#[derive(Default, Clone)]
struct State {
    resources: HashMap<ResourceKey, BoundResource>,
    synchronizations: Option<Vec<Arc<dyn TransactionSynchronization>>>,
    current_transaction_name: Option<String>,
    current_transaction_read_only: bool,
    current_transaction_isolation_level: Option<Isolation>,
    actual_transaction_active: bool,
}

type Shared = Arc<Mutex<State>>;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.try_lock() {
        Ok(guard) => guard,
        Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        Err(TryLockError::WouldBlock) => panic!(
            "re-entrant access to the transaction context: a resource holder or a synchronization called \
             back into demarcate_core::synchronization while the context was being updated"
        ),
    }
}

#[doc(hidden)]
pub struct Context {
    state: Mutex<Shared>,
    frames: Mutex<Vec<(u64, Shared)>>,
    next_frame: AtomicU64,
    attachment: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    parent: Option<Arc<Context>>,
}

impl Context {
    fn root() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Arc::default()),
            frames: Mutex::new(Vec::new()),
            next_frame: AtomicU64::new(0),
            attachment: Mutex::new(None),
            parent: None,
        })
    }

    fn child(parent: Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(parent.shared()),
            frames: Mutex::new(Vec::new()),
            next_frame: AtomicU64::new(0),
            attachment: Mutex::new(None),
            parent: Some(parent),
        })
    }

    fn shared(&self) -> Shared {
        lock(&self.state).clone()
    }

    fn detach(self: &Arc<Self>) -> Frame {
        let id = self.next_frame.fetch_add(1, Ordering::SeqCst);
        let mut state = lock(&self.state);
        let fork = Arc::new(Mutex::new(lock(&state).clone()));
        let previous = std::mem::replace(&mut *state, fork);
        lock(&self.frames).push((id, previous));
        Frame { context: self.clone(), id }
    }

    fn reattach(&self, id: u64) {
        let discarded = {
            let mut frames = lock(&self.frames);
            let Some(position) = frames.iter().position(|(frame, _)| *frame == id) else { return };
            let mut later = frames.split_off(position);
            let (_, previous) = later.remove(0);
            let fork = std::mem::replace(&mut *lock(&self.state), previous);
            (fork, later)
        };
        drop(discarded);
    }

    fn is_top(&self, id: u64) -> bool {
        lock(&self.frames).last().is_some_and(|(frame, _)| *frame == id)
    }
}

pub(crate) struct Frame {
    context: Arc<Context>,
    id: u64,
}

impl Frame {
    pub(crate) fn is_top(&self) -> bool {
        self.context.is_top(self.id)
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        self.context.reattach(self.id);
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<Context>>> = const { RefCell::new(None) };
}

fn current() -> Option<Arc<Context>> {
    CURRENT.try_with(|current| current.borrow().clone()).ok().flatten()
}

struct Scoped<F> {
    context: Arc<Context>,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        struct Restore(Option<Arc<Context>>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let _ = CURRENT.try_with(|current| *current.borrow_mut() = self.0.take());
            }
        }
        let entered = Some(self.context.clone());
        let _restore = Restore(CURRENT.try_with(|current| current.replace(entered)).ok().flatten());
        self.future.as_mut().poll(cx)
    }
}

const NO_CONTEXT: &str = "No transaction context is bound to the current future. Call this inside a \
     #[transactional] function or TransactionTemplate::execute, or wrap the future in \
     demarcate_core::synchronization::scope. A spawned task does not inherit the context of the task that \
     spawned it; open a new transaction inside it";

fn read<R>(default: R, f: impl FnOnce(&State) -> R) -> R {
    match current() {
        Some(context) => f(&lock(&context.shared())),
        None => default,
    }
}

fn write<R>(f: impl FnOnce(&mut State) -> Result<R, TransactionError>) -> Result<R, TransactionError> {
    match current() {
        Some(context) => f(&mut lock(&context.shared())),
        None => Err(TransactionError::InvalidState(NO_CONTEXT.into())),
    }
}

fn bind(state: &mut State, key: ResourceKey, value: BoundResource) -> Option<BoundResource> {
    if let Some(existing) = state.resources.get(&key).filter(|existing| !existing.is_void()) {
        return Some(existing.clone());
    }
    state.resources.insert(key, value);
    None
}

fn unbind(state: &mut State, key: &ResourceKey) -> Option<BoundResource> {
    state.resources.remove(key).filter(|value| !value.is_void())
}

pub fn is_context_active() -> bool {
    current().is_some()
}

pub async fn scope<F: Future>(future: F) -> F::Output {
    if is_context_active() { future.await } else { Scoped { context: Context::root(), future: Box::pin(future) }.await }
}

#[doc(hidden)]
pub fn call_scope<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let context = match current() {
        Some(parent) => Context::child(parent),
        None => Context::root(),
    };
    Scoped { context, future: Box::pin(future) }
}

pub(crate) fn detach() -> Option<Frame> {
    current().map(|context| context.detach())
}

#[doc(hidden)]
pub struct CallAttachment(Option<Arc<Context>>);

impl Drop for CallAttachment {
    fn drop(&mut self) {
        if let Some(context) = &self.0 {
            let detached = lock(&context.attachment).take();
            drop(detached);
        }
    }
}

#[doc(hidden)]
pub fn attach_to_call(attachment: Arc<dyn Any + Send + Sync>) -> CallAttachment {
    let context = current();
    if let Some(context) = &context {
        let previous = lock(&context.attachment).replace(attachment);
        drop(previous);
    }
    CallAttachment(context)
}

#[doc(hidden)]
pub fn call_attachment() -> Option<Arc<dyn Any + Send + Sync>> {
    let mut context = current();
    while let Some(candidate) = context {
        if let Some(attachment) = lock(&candidate.attachment).clone() {
            return Some(attachment);
        }
        context = candidate.parent.clone();
    }
    None
}

pub(crate) const OUT_OF_ORDER: &str = "Transactions on this context completed out of order. A transaction \
     obtained from PlatformTransactionManager::transaction was completed while a later one on the same context was \
     still open. The transaction was rolled back. Complete transactions in the reverse order of their creation, or \
     use #[transactional] or TransactionTemplate, which give every call its own context";

#[cfg(feature = "tokio")]
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(Scoped { context: Context::root(), future: Box::pin(future) })
}

pub fn resource_keys() -> Vec<ResourceKey> {
    read(Vec::new(), |state| state.resources.keys().cloned().collect())
}

pub fn has_resource(key: &ResourceKey) -> bool {
    resource(key).is_some()
}

pub fn resource(key: &ResourceKey) -> Option<BoundResource> {
    let context = current()?;
    let shared = context.shared();
    let mut state = lock(&shared);
    let value = state.resources.get(key).cloned()?;
    if value.is_void() {
        let removed = state.resources.remove(key);
        drop(state);
        drop(removed);
        return None;
    }
    Some(value)
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

pub fn register_synchronization(synchronization: Arc<dyn TransactionSynchronization>) -> Result<(), TransactionError> {
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
    let cleared = write(|state| {
        state.synchronizations.take().ok_or_else(|| {
            TransactionError::InvalidState("Cannot deactivate transaction synchronization - not active".into())
        })
    })?;
    drop(cleared);
    Ok(())
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
        let cleared = state.synchronizations.take();
        state.current_transaction_name = None;
        state.current_transaction_read_only = false;
        state.current_transaction_isolation_level = None;
        state.actual_transaction_active = false;
        Ok(cleared)
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
