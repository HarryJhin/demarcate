use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use demarcate_core::{ResourceHolder, TransactionError, synchronization};

use crate::ConnectionBackend;

const BESIDE_SAVEPOINT: &str = "A savepoint of another call is open on the connection of this transaction. A \
     nested transaction (propagation = Nested) runs on a savepoint, and until it completes the connection belongs to \
     that call: a future beside it (for example in join! or select!) can neither query nor open a savepoint of its \
     own. Await the nested call before the other work, or give the other work its own transaction with \
     propagation = RequiresNew";

struct Slot<C> {
    leased: AtomicBool,
    connection: Mutex<Option<C>>,
}

pub(crate) struct Lease<C> {
    slot: Arc<Slot<C>>,
    connection: Option<C>,
}

impl<C> Deref for Lease<C> {
    type Target = Option<C>;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<C> DerefMut for Lease<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<C> Drop for Lease<C> {
    fn drop(&mut self) {
        *self.slot.connection.lock().expect("connection lock") = self.connection.take();
        self.slot.leased.store(false, Ordering::SeqCst);
    }
}

/// The connection of one transaction, or of one synchronization scope without a transaction,
/// bound to the transaction context under the key of its [`ConnectionSource`](crate::ConnectionSource).
///
/// It keeps the connection, the rollback-only mark, the deadline and the open savepoints.
/// Application code does not use it; it is public for code that inspects the context.
pub struct ConnectionHolder<B: ConnectionBackend> {
    slot: Arc<Slot<B::Connection>>,
    synchronized_with_transaction: AtomicBool,
    rollback_only_depth: Mutex<Option<usize>>,
    aborted_depth: Mutex<Option<usize>>,
    savepoints: Mutex<Vec<(String, Option<u64>)>>,
    transaction_active: AtomicBool,
    deadline: Mutex<Option<Instant>>,
    savepoint_counter: AtomicU32,
    reference_count: AtomicUsize,
}

impl<B: ConnectionBackend> ConnectionHolder<B> {
    pub(crate) fn new(connection: B::Connection) -> Self {
        Self {
            slot: Arc::new(Slot { leased: AtomicBool::new(false), connection: Mutex::new(Some(connection)) }),
            synchronized_with_transaction: AtomicBool::new(false),
            rollback_only_depth: Mutex::new(None),
            aborted_depth: Mutex::new(None),
            savepoints: Mutex::new(Vec::new()),
            transaction_active: AtomicBool::new(false),
            deadline: Mutex::new(None),
            savepoint_counter: AtomicU32::new(0),
            reference_count: AtomicUsize::new(0),
        }
    }
    /// Returns whether the connection is held for the duration of a transaction or a synchronization
    /// scope.
    pub fn is_synchronized_with_transaction(&self) -> bool {
        self.synchronized_with_transaction.load(Ordering::SeqCst)
    }
    /// Records whether the connection is held for the duration of a transaction or a synchronization
    /// scope.
    pub fn set_synchronized_with_transaction(&self, value: bool) {
        self.synchronized_with_transaction.store(value, Ordering::SeqCst);
    }
    /// Returns whether the connection runs a transaction that the transaction manager started.
    pub fn is_transaction_active(&self) -> bool {
        self.transaction_active.load(Ordering::SeqCst)
    }
    pub(crate) fn set_transaction_active(&self, value: bool) {
        self.transaction_active.store(value, Ordering::SeqCst);
    }
    /// Marks the transaction rollback-only.
    ///
    /// The mark remembers how many open savepoints the calling future is inside of. A rollback to
    /// one of those savepoints removes it; a rollback to any other savepoint keeps it.
    pub fn set_rollback_only(&self) {
        let depth = {
            let savepoints = self.savepoints.lock().expect("savepoints lock");
            savepoints.iter().take_while(|(_, owner)| owner.is_none_or(synchronization::is_within_context)).count()
        };
        let mut marked = self.rollback_only_depth.lock().expect("rollback-only lock");
        *marked = Some(marked.map_or(depth, |existing| existing.min(depth)));
    }
    /// Removes the rollback-only mark.
    pub fn reset_rollback_only(&self) {
        *self.rollback_only_depth.lock().expect("rollback-only lock") = None;
    }
    /// Returns whether the transaction is marked rollback-only.
    pub fn is_rollback_only(&self) -> bool {
        self.rollback_only_depth.lock().expect("rollback-only lock").is_some()
    }
    /// Records that a statement failed with an error that can have left the transaction unable to
    /// commit. The commit then asks the backend whether the transaction is still usable.
    pub fn mark_aborted(&self) {
        let depth = self.savepoints.lock().expect("savepoints lock").len();
        let mut aborted = self.aborted_depth.lock().expect("aborted lock");
        *aborted = Some(aborted.map_or(depth, |existing| existing.min(depth)));
    }
    pub(crate) fn is_aborted(&self) -> bool {
        self.aborted_depth.lock().expect("aborted lock").is_some()
    }
    pub(crate) fn check_savepoint_owner(&self) -> Result<(), TransactionError> {
        let beside = self
            .savepoints
            .lock()
            .expect("savepoints lock")
            .iter()
            .any(|(_, owner)| owner.is_some_and(|owner| !synchronization::is_within_context(owner)));
        if beside { Err(TransactionError::InvalidState(BESIDE_SAVEPOINT.into())) } else { Ok(()) }
    }
    pub(crate) fn savepoint_created(&self, name: &str) {
        self.savepoints.lock().expect("savepoints lock").push((name.to_owned(), None));
    }
    pub(crate) fn savepoint_owned(&self, name: &str) {
        let owner = synchronization::context_id();
        if let Some(entry) =
            self.savepoints.lock().expect("savepoints lock").iter_mut().find(|(existing, _)| existing == name)
        {
            entry.1 = owner;
        }
    }
    pub(crate) fn set_rollback_only_for_good(&self) {
        *self.rollback_only_depth.lock().expect("rollback-only lock") = Some(0);
    }
    pub(crate) fn savepoint_rolled_back(&self, name: &str) {
        let Some(index) = self.savepoint_index(name) else { return };
        for marked in [&self.rollback_only_depth, &self.aborted_depth] {
            let mut marked = marked.lock().expect("mark lock");
            if marked.is_some_and(|depth| depth > index) {
                *marked = None;
            }
        }
    }
    pub(crate) fn savepoint_gone(&self, name: &str) {
        if let Some(index) = self.savepoint_index(name) {
            self.savepoints.lock().expect("savepoints lock").truncate(index);
        }
    }
    fn savepoint_index(&self, name: &str) -> Option<usize> {
        self.savepoints.lock().expect("savepoints lock").iter().position(|(existing, _)| existing == name)
    }
    /// Sets the deadline of the transaction to the given duration from now.
    pub fn set_timeout(&self, timeout: Duration) {
        *self.deadline.lock().expect("deadline lock") = Some(Instant::now() + timeout);
    }
    /// Returns whether the transaction has a deadline.
    pub fn has_timeout(&self) -> bool {
        self.deadline.lock().expect("deadline lock").is_some()
    }
    /// Returns the time until the deadline, or `None` when there is no deadline.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::TimedOut`] and marks the transaction rollback-only when the
    /// deadline has passed.
    pub fn time_to_live(&self) -> Result<Option<Duration>, TransactionError> {
        let Some(deadline) = *self.deadline.lock().expect("deadline lock") else {
            return Ok(None);
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.set_rollback_only();
            return Err(TransactionError::TimedOut("Transaction timed out: deadline has passed".into()));
        }
        Ok(Some(remaining))
    }
    /// Counts one more handle to the connection in use.
    pub fn requested(&self) {
        self.reference_count.fetch_add(1, Ordering::SeqCst);
    }
    /// Counts one handle to the connection as returned.
    pub fn released(&self) {
        let mut current = self.reference_count.load(Ordering::SeqCst);
        while current > 0 {
            match self.reference_count.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
    /// Returns whether a handle to the connection is in use.
    pub fn is_open(&self) -> bool {
        self.reference_count.load(Ordering::SeqCst) > 0
    }
    pub(crate) fn next_savepoint_name(&self) -> String {
        format!("SAVEPOINT_{}", self.savepoint_counter.fetch_add(1, Ordering::SeqCst) + 1)
    }
    pub(crate) fn clear(&self) {
        self.set_synchronized_with_transaction(false);
        self.reset_rollback_only();
        *self.aborted_depth.lock().expect("aborted lock") = None;
        self.savepoints.lock().expect("savepoints lock").clear();
        *self.deadline.lock().expect("deadline lock") = None;
        self.set_transaction_active(false);
        self.savepoint_counter.store(0, Ordering::SeqCst);
    }
    fn try_lease(&self) -> Option<Lease<B::Connection>> {
        self.slot.leased.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).ok()?;
        let connection = self.slot.connection.lock().expect("connection lock").take();
        Some(Lease { slot: self.slot.clone(), connection })
    }
    pub(crate) async fn release_connection(&self) {
        match self.try_lease() {
            Some(mut connection) => {
                connection.take();
            }
            None => tracing::error!(
                "A TransactionalConnection outlived its transaction; the connection is released when that handle is dropped"
            ),
        }
    }
    pub(crate) fn lock(&self) -> Result<Lease<B::Connection>, TransactionError> {
        self.try_lease().ok_or_else(|| {
            TransactionError::InvalidState(
                "The connection of the current transaction is already in use. A \
                 TransactionalConnection returned by acquire() is still alive, or two queries of one transaction \
                 are awaited concurrently (join!, select!). Drop the connection before calling another \
                 transactional function, and await the queries of one transaction one after another"
                    .into(),
            )
        })
    }

    pub(crate) fn abandon(&self) {
        self.set_transaction_active(false);
        self.clear();
        if let Some(mut connection) = self.try_lease() {
            connection.take();
        }
    }
}

impl<B: ConnectionBackend> ResourceHolder for ConnectionHolder<B> {
    fn reset(&self) {
        self.clear();
        self.reference_count.store(0, Ordering::SeqCst);
    }
    fn unbound(&self) {}
    fn is_void(&self) -> bool {
        false
    }
}
