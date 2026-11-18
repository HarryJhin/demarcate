use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::TransactionError;

#[derive(Default)]
struct State {
    synchronized_with_transaction: bool,
    rollback_only: bool,
    deadline: Option<Instant>,
    reference_count: i64,
    void: bool,
}

/// The state that every resource holder needs: the rollback-only flag, the deadline of the
/// transaction and a count of the handles in use.
///
/// A driver embeds this in the holder that it binds to the transaction context, for example a
/// connection holder, and delegates to it.
#[derive(Default)]
pub struct ResourceHolderSupport {
    state: Mutex<State>,
}

impl ResourceHolderSupport {
    /// Creates the state of a holder that takes part in no transaction.
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("resource holder lock")
    }

    /// Records whether the resource is bound for the duration of a transaction or a synchronization
    /// scope.
    pub fn set_synchronized_with_transaction(&self, synchronized_with_transaction: bool) {
        self.state().synchronized_with_transaction = synchronized_with_transaction;
    }

    /// Returns whether the resource is bound for the duration of a transaction or a synchronization
    /// scope.
    pub fn is_synchronized_with_transaction(&self) -> bool {
        self.state().synchronized_with_transaction
    }

    /// Marks the transaction of this resource rollback-only.
    pub fn set_rollback_only(&self) {
        self.state().rollback_only = true;
    }

    /// Removes the rollback-only mark, for example after a rollback to a savepoint undid the work
    /// that caused it.
    pub fn reset_rollback_only(&self) {
        self.state().rollback_only = false;
    }

    /// Returns whether the transaction of this resource is marked rollback-only.
    pub fn is_rollback_only(&self) -> bool {
        self.state().rollback_only
    }

    /// Sets the deadline of the transaction to the given duration from now.
    pub fn set_timeout(&self, timeout: Duration) {
        self.state().deadline = Some(Instant::now() + timeout);
    }

    /// Returns whether the transaction has a deadline.
    pub fn has_timeout(&self) -> bool {
        self.state().deadline.is_some()
    }

    /// Returns the deadline of the transaction.
    pub fn deadline(&self) -> Option<Instant> {
        self.state().deadline
    }

    /// Returns the time until the deadline, rounded up to whole seconds.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::TimedOut`] and marks the transaction rollback-only when the
    /// deadline has passed, and an error when no deadline is set.
    pub fn time_to_live_in_seconds(&self) -> Result<i32, TransactionError> {
        let millis = self.time_to_live_in_millis()?;
        let seconds = (millis as f64 / 1000.0).ceil() as i32;
        self.check_transaction_timeout(seconds <= 0)?;
        Ok(seconds)
    }

    /// Returns the time until the deadline in milliseconds.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::TimedOut`] and marks the transaction rollback-only when the
    /// deadline has passed, and an error when no deadline is set.
    pub fn time_to_live_in_millis(&self) -> Result<i64, TransactionError> {
        let deadline = self
            .deadline()
            .ok_or_else(|| TransactionError::InvalidState("No timeout specified for this resource holder".into()))?;
        let now = Instant::now();
        let time_to_live =
            if deadline > now { (deadline - now).as_millis() as i64 } else { -((now - deadline).as_millis() as i64) };
        self.check_transaction_timeout(time_to_live <= 0)?;
        Ok(time_to_live)
    }

    fn check_transaction_timeout(&self, deadline_reached: bool) -> Result<(), TransactionError> {
        if deadline_reached {
            self.set_rollback_only();
            return Err(TransactionError::TimedOut("Transaction timed out: deadline has been reached".into()));
        }
        Ok(())
    }

    /// Counts one more handle to the resource in use.
    pub fn requested(&self) {
        self.state().reference_count += 1;
    }

    /// Counts one handle to the resource as returned.
    pub fn released(&self) {
        self.state().reference_count -= 1;
    }

    /// Returns whether a handle to the resource is in use.
    pub fn is_open(&self) -> bool {
        self.state().reference_count > 0
    }

    /// Clears the transactional state: the synchronization flag, the rollback-only mark and the
    /// deadline.
    pub fn clear(&self) {
        let mut state = self.state();
        state.synchronized_with_transaction = false;
        state.rollback_only = false;
        state.deadline = None;
    }

    /// Clears the transactional state and the count of handles in use.
    pub fn reset(&self) {
        self.clear();
        self.state().reference_count = 0;
    }

    /// Records that the resource was unbound from the context.
    pub fn unbound(&self) {
        self.state().void = true;
    }

    /// Returns whether the resource was unbound and must not be used any more.
    pub fn is_void(&self) -> bool {
        self.state().void
    }
}
