use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::BoxFuture;
use crate::driver::TransactionDriver;
use crate::error::TransactionError;
use crate::manager::SuspendedResourcesHolder;
use crate::synchronization;

/// An opaque handle to a savepoint, created and understood by one driver.
pub type Savepoint = Arc<dyn Any + Send + Sync>;

/// The state of one transactional call: whether it runs in a transaction, whether it started
/// that transaction, and whether the transaction must roll back.
///
/// This is the read side of [`TransactionStatus`], and the view that a
/// [`TransactionExecutionListener`](crate::TransactionExecutionListener) receives.
pub trait TransactionExecution: Send + Sync {
    /// Returns the name of the transaction, or an empty string when it has none.
    fn transaction_name(&self) -> String {
        String::new()
    }
    /// Returns whether the call runs in an actual transaction.
    ///
    /// This is `false` for a call that runs without a transaction, for example with
    /// [`Propagation::Supports`](crate::Propagation::Supports) and no caller transaction.
    fn has_transaction(&self) -> bool {
        true
    }
    /// Returns whether the call started the transaction, as opposed to joining the transaction of its
    /// caller.
    ///
    /// Only the call that started a transaction commits or rolls it back.
    fn is_new_transaction(&self) -> bool {
        true
    }
    /// Returns whether the call runs in a nested transaction.
    fn is_nested(&self) -> bool {
        false
    }
    /// Returns whether the transaction was defined as read-only.
    fn is_read_only(&self) -> bool {
        false
    }
    /// Marks the transaction of this call so that it rolls back instead of committing.
    ///
    /// Use this to roll back without returning an error. The function then returns normally and the
    /// rollback happens when the call would have committed. In a nested call only the work since the
    /// savepoint is rolled back. In a call that joined a transaction the whole transaction is marked,
    /// and the call that started it gets
    /// [`TransactionError::UnexpectedRollback`] when it tries to commit.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction has already completed. The default implementation
    /// always returns an error.
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        Err(TransactionError::InvalidState("This transaction status does not support set_rollback_only".into()))
    }
    /// Returns whether the transaction is marked rollback-only, by this call or by another call that
    /// joined the same transaction.
    fn is_rollback_only(&self) -> bool {
        false
    }
    /// Returns whether the transaction was committed, rolled back, or abandoned by a cancelled call.
    fn is_completed(&self) -> bool {
        false
    }
}

/// Programmatic control of savepoints in the current transaction.
///
/// A savepoint allows a part of a transaction to be rolled back. Prefer
/// [`Propagation::Nested`](crate::Propagation::Nested), which manages the savepoint for the
/// duration of a call.
///
/// Use these methods from one future at a time, and release or roll back a savepoint in the
/// function that created it.
pub trait SavepointManager: Send + Sync {
    /// Creates a savepoint.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::NestedNotSupported`] when the call runs without a transaction or
    /// the driver does not support savepoints. The PostgreSQL driver returns
    /// [`TransactionError::CannotCreateTransaction`] when the transaction is marked rollback-only.
    fn create_savepoint(&self) -> BoxFuture<'_, Result<Savepoint, TransactionError>>;
    /// Rolls the transaction back to the given savepoint.
    ///
    /// The savepoint is not released by this; call
    /// [`release_savepoint`](SavepointManager::release_savepoint) afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::NestedNotSupported`] when the transaction does not support
    /// savepoints, and [`TransactionError::System`] when the database rejects the statement.
    fn rollback_to_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>>;
    /// Releases the given savepoint. The work since the savepoint stays part of the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::NestedNotSupported`] when the transaction does not support
    /// savepoints, and [`TransactionError::System`] when the database rejects the statement.
    fn release_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>>;
}

/// The handle to one transactional call.
///
/// A status is returned by
/// [`PlatformTransactionManager::transaction`](crate::PlatformTransactionManager::transaction)
/// and passed back to `commit` or `rollback`. Inside a `#[transactional]` function or a
/// [`TransactionTemplate`](crate::TransactionTemplate) action it is available for
/// [`set_rollback_only`](TransactionExecution::set_rollback_only) and for savepoints.
pub trait TransactionStatus: TransactionExecution + SavepointManager + Any {
    /// Returns whether the call holds a savepoint, that is whether it runs as a nested transaction on
    /// a savepoint.
    fn has_savepoint(&self) -> bool {
        false
    }
    /// Flushes pending changes of the underlying session to the database, if the driver has such a
    /// session. Does nothing for a call that runs without a transaction.
    ///
    /// # Errors
    ///
    /// Returns the error of the driver or of a synchronization that failed to flush.
    fn flush(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async { Ok(()) })
    }
    #[doc(hidden)]
    fn abandon(&self) {}
    /// Returns the status as [`Any`], so that a transaction manager can recognize its own statuses.
    fn as_any(&self) -> &dyn Any;
}

/// A [`TransactionStatus`] that is not backed by a transaction manager.
///
/// It records the rollback-only flag and the completed flag and does nothing else. It serves
/// custom transaction managers and tests. Savepoints are not supported.
pub struct SimpleTransactionStatus {
    new_transaction: bool,
    rollback_only: AtomicBool,
    completed: AtomicBool,
}

impl SimpleTransactionStatus {
    /// Creates a status that reports a new transaction.
    pub fn new() -> Self {
        Self::with_new_transaction(true)
    }

    /// Creates a status that reports a new transaction, or a joined one when `new_transaction` is
    /// `false`.
    pub fn with_new_transaction(new_transaction: bool) -> Self {
        Self { new_transaction, rollback_only: AtomicBool::new(false), completed: AtomicBool::new(false) }
    }

    /// Marks the transaction as completed.
    pub fn set_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }
}

impl Default for SimpleTransactionStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionExecution for SimpleTransactionStatus {
    fn is_new_transaction(&self) -> bool {
        self.new_transaction
    }
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        if self.is_completed() {
            return Err(TransactionError::InvalidState("Transaction completed".into()));
        }
        self.rollback_only.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn is_rollback_only(&self) -> bool {
        self.rollback_only.load(Ordering::SeqCst)
    }
    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

impl SavepointManager for SimpleTransactionStatus {
    fn create_savepoint(&self) -> BoxFuture<'_, Result<Savepoint, TransactionError>> {
        Box::pin(async { Err(not_supported()) })
    }
    fn rollback_to_savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async { Err(not_supported()) })
    }
    fn release_savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async { Err(not_supported()) })
    }
}

impl TransactionStatus for SimpleTransactionStatus {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn not_supported() -> TransactionError {
    TransactionError::NestedNotSupported("This transaction does not support savepoints".into())
}

/// The [`TransactionStatus`] of a [`TransactionManager`](crate::TransactionManager).
///
/// It holds the transaction object of the driver, the resources of a transaction that this call
/// suspended, and the savepoint of a nested call.
///
/// When the call is cancelled before the status completed, the transaction it started is rolled
/// back. A transaction it had joined is marked rollback-only, unless
/// [`ManagerSettings::global_rollback_on_participation_failure`](crate::ManagerSettings::global_rollback_on_participation_failure)
/// is `false`.
pub struct ManagedTransactionStatus<D: TransactionDriver> {
    driver: Arc<D>,
    transaction_name: Option<String>,
    transaction: Option<D::Transaction>,
    new_transaction: bool,
    new_synchronization: bool,
    nested: bool,
    read_only: bool,
    suspended_resources: Mutex<Option<SuspendedResourcesHolder<D>>>,
    rollback_only: AtomicBool,
    completed: AtomicBool,
    savepoint: Mutex<Option<Savepoint>>,
    frame: Mutex<Option<synchronization::Frame>>,
    abandoned: AtomicBool,
    rollback_only_when_abandoned: bool,
}

impl<D: TransactionDriver> ManagedTransactionStatus<D> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        driver: Arc<D>,
        transaction_name: Option<String>,
        transaction: Option<D::Transaction>,
        new_transaction: bool,
        new_synchronization: bool,
        nested: bool,
        read_only: bool,
        suspended_resources: Option<SuspendedResourcesHolder<D>>,
        rollback_only_when_abandoned: bool,
    ) -> Self {
        Self {
            driver,
            transaction_name,
            transaction,
            new_transaction,
            new_synchronization,
            nested,
            read_only,
            suspended_resources: Mutex::new(suspended_resources),
            rollback_only: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            savepoint: Mutex::new(None),
            frame: Mutex::new(None),
            abandoned: AtomicBool::new(false),
            rollback_only_when_abandoned,
        }
    }

    /// Returns the transaction object of the driver, or `None` when the call runs without a
    /// transaction.
    pub fn transaction(&self) -> Option<&D::Transaction> {
        self.transaction.as_ref()
    }

    /// Returns whether this call opened the synchronization scope, and therefore triggers the
    /// synchronization callbacks when it completes.
    pub fn is_new_synchronization(&self) -> bool {
        self.new_synchronization
    }

    /// Returns whether this call marked the transaction rollback-only through
    /// [`set_rollback_only`](TransactionExecution::set_rollback_only).
    pub fn is_local_rollback_only(&self) -> bool {
        self.rollback_only.load(Ordering::SeqCst)
    }

    /// Returns whether the transaction is marked rollback-only by any call that joined it.
    pub fn is_global_rollback_only(&self) -> bool {
        self.transaction.as_ref().is_some_and(|transaction| self.driver.is_rollback_only(transaction))
    }

    pub(crate) fn set_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn set_frame(&self, frame: Option<synchronization::Frame>) {
        *self.frame.lock().expect("frame lock") = frame;
    }

    pub(crate) fn release_frame(&self) {
        let frame = self.frame.lock().expect("frame lock").take();
        drop(frame);
    }

    fn abandon_transaction(&self) {
        if self.is_completed() || self.abandoned.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(transaction) = &self.transaction {
            if let Some(savepoint) = self.held_savepoint() {
                self.driver.abandon_savepoint(transaction, &savepoint);
            }
            if self.new_transaction {
                self.driver.abandon(transaction);
            } else if self.rollback_only_when_abandoned
                && let Err(error) = self.driver.set_rollback_only_on_abandon(transaction)
            {
                tracing::debug!(%error, "Could not mark the abandoned participating transaction as rollback-only");
            }
        }
        let suspended = self.take_suspended_resources();
        drop(suspended);
        self.set_completed();
        self.release_frame();
    }

    pub(crate) fn has_suspended_resources(&self) -> bool {
        self.suspended_resources.lock().expect("suspended resources lock").is_some()
    }

    pub(crate) fn is_in_order(&self) -> bool {
        self.frame.lock().expect("frame lock").as_ref().is_none_or(synchronization::Frame::is_top)
    }

    pub(crate) fn take_suspended_resources(&self) -> Option<SuspendedResourcesHolder<D>> {
        self.suspended_resources.lock().expect("suspended resources lock").take()
    }

    fn held_savepoint(&self) -> Option<Savepoint> {
        self.savepoint.lock().expect("savepoint lock").clone()
    }

    fn set_savepoint(&self, savepoint: Option<Savepoint>) {
        *self.savepoint.lock().expect("savepoint lock") = savepoint;
    }

    pub(crate) async fn create_and_hold_savepoint(&self) -> Result<(), TransactionError> {
        let transaction = self.require_savepoint_transaction()?;
        let savepoint = self.driver.create_savepoint(transaction).await?;
        self.driver.own_savepoint(transaction, &savepoint);
        synchronization::trigger_savepoint(&savepoint).await?;
        self.set_savepoint(Some(savepoint));
        Ok(())
    }

    pub(crate) async fn rollback_to_held_savepoint(&self) -> Result<(), TransactionError> {
        let savepoint = self.held_savepoint().ok_or_else(|| {
            TransactionError::Usage(
                "Cannot roll back to savepoint - no savepoint associated with current transaction".into(),
            )
        })?;
        synchronization::trigger_savepoint_rollback(&savepoint).await?;
        let transaction = self.require_savepoint_transaction()?;
        self.driver.rollback_to_savepoint(transaction, &savepoint).await?;
        self.driver.release_savepoint(transaction, &savepoint).await?;
        self.set_savepoint(None);
        Ok(())
    }

    pub(crate) async fn release_held_savepoint(&self) -> Result<(), TransactionError> {
        let savepoint = self.held_savepoint().ok_or_else(|| {
            TransactionError::Usage(
                "Cannot release savepoint - no savepoint associated with current transaction".into(),
            )
        })?;
        self.driver.release_savepoint(self.require_savepoint_transaction()?, &savepoint).await?;
        self.set_savepoint(None);
        Ok(())
    }

    fn require_savepoint_transaction(&self) -> Result<&D::Transaction, TransactionError> {
        self.transaction.as_ref().ok_or_else(|| {
            TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())
        })
    }
}

impl<D: TransactionDriver> TransactionExecution for ManagedTransactionStatus<D> {
    fn transaction_name(&self) -> String {
        self.transaction_name.clone().unwrap_or_default()
    }
    fn has_transaction(&self) -> bool {
        self.transaction.is_some()
    }
    fn is_new_transaction(&self) -> bool {
        self.has_transaction() && self.new_transaction
    }
    fn is_nested(&self) -> bool {
        self.nested
    }
    fn is_read_only(&self) -> bool {
        self.read_only
    }
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        if self.is_completed() {
            return Err(TransactionError::InvalidState("Transaction completed".into()));
        }
        self.rollback_only.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn is_rollback_only(&self) -> bool {
        self.is_local_rollback_only() || self.is_global_rollback_only()
    }
    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

impl<D: TransactionDriver> SavepointManager for ManagedTransactionStatus<D> {
    fn create_savepoint(&self) -> BoxFuture<'_, Result<Savepoint, TransactionError>> {
        Box::pin(async move {
            let savepoint = self.driver.create_savepoint(self.require_savepoint_transaction()?).await?;
            synchronization::trigger_savepoint(&savepoint).await?;
            Ok(savepoint)
        })
    }
    fn rollback_to_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move {
            synchronization::trigger_savepoint_rollback(savepoint).await?;
            self.driver.rollback_to_savepoint(self.require_savepoint_transaction()?, savepoint).await
        })
    }
    fn release_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move { self.driver.release_savepoint(self.require_savepoint_transaction()?, savepoint).await })
    }
}

impl<D: TransactionDriver> TransactionStatus for ManagedTransactionStatus<D> {
    fn has_savepoint(&self) -> bool {
        self.held_savepoint().is_some()
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async move {
            match self.transaction.as_ref() {
                Some(transaction) => self.driver.flush(transaction).await,
                None => Ok(()),
            }
        })
    }
    fn abandon(&self) {
        self.abandon_transaction();
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<D: TransactionDriver> Drop for ManagedTransactionStatus<D> {
    fn drop(&mut self) {
        self.abandon_transaction();
    }
}

#[doc(hidden)]
pub struct AbandonOnDrop(pub Arc<dyn TransactionStatus>);

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        self.0.abandon();
    }
}
