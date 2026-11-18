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

#[derive(Default)]
pub struct ResourceHolderSupport {
    state: Mutex<State>,
}

impl ResourceHolderSupport {
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("resource holder lock")
    }

    pub fn set_synchronized_with_transaction(&self, synchronized_with_transaction: bool) {
        self.state().synchronized_with_transaction = synchronized_with_transaction;
    }

    pub fn is_synchronized_with_transaction(&self) -> bool {
        self.state().synchronized_with_transaction
    }

    pub fn set_rollback_only(&self) {
        self.state().rollback_only = true;
    }

    pub fn reset_rollback_only(&self) {
        self.state().rollback_only = false;
    }

    pub fn is_rollback_only(&self) -> bool {
        self.state().rollback_only
    }

    pub fn set_timeout(&self, timeout: Duration) {
        self.state().deadline = Some(Instant::now() + timeout);
    }

    pub fn has_timeout(&self) -> bool {
        self.state().deadline.is_some()
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.state().deadline
    }

    pub fn time_to_live_in_seconds(&self) -> Result<i32, TransactionError> {
        let millis = self.time_to_live_in_millis()?;
        let seconds = (millis as f64 / 1000.0).ceil() as i32;
        self.check_transaction_timeout(seconds <= 0)?;
        Ok(seconds)
    }

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

    pub fn requested(&self) {
        self.state().reference_count += 1;
    }

    pub fn released(&self) {
        self.state().reference_count -= 1;
    }

    pub fn is_open(&self) -> bool {
        self.state().reference_count > 0
    }

    pub fn clear(&self) {
        let mut state = self.state();
        state.synchronized_with_transaction = false;
        state.rollback_only = false;
        state.deadline = None;
    }

    pub fn reset(&self) {
        self.clear();
        self.state().reference_count = 0;
    }

    pub fn unbound(&self) {
        self.state().void = true;
    }

    pub fn is_void(&self) -> bool {
        self.state().void
    }
}
