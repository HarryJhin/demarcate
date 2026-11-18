use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use demarcate_core::{ResourceHolder, Savepoint, TransactionError};

use super::connection::{ConnectionHandle, SharedConnection, SimpleConnectionHandle, SqlError};

pub const SAVEPOINT_NAME_PREFIX: &str = "SAVEPOINT_";

#[derive(Default)]
struct Inner {
    synchronized_with_transaction: bool,
    rollback_only: bool,
    deadline: Option<Instant>,
    reference_count: i64,
    void: bool,
    connection_handle: Option<Arc<dyn ConnectionHandle>>,
    current_connection: Option<SharedConnection>,
    transaction_active: bool,
    savepoints_supported: Option<bool>,
    savepoint_counter: u32,
}

pub struct ConnectionHolder {
    inner: Mutex<Inner>,
}

impl ConnectionHolder {
    pub fn new(connection: SharedConnection) -> Self {
        Self::with_handle(Arc::new(SimpleConnectionHandle::new(connection)))
    }

    pub fn with_handle(connection_handle: Arc<dyn ConnectionHandle>) -> Self {
        Self { inner: Mutex::new(Inner { connection_handle: Some(connection_handle), ..Inner::default() }) }
    }

    pub fn with_transaction_active(connection: SharedConnection, transaction_active: bool) -> Self {
        let holder = Self::new(connection);
        holder.set_transaction_active(transaction_active);
        holder
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("connection holder lock")
    }

    pub fn connection_handle(&self) -> Option<Arc<dyn ConnectionHandle>> {
        self.inner().connection_handle.clone()
    }

    pub fn has_connection(&self) -> bool {
        self.inner().connection_handle.is_some()
    }

    pub fn set_transaction_active(&self, transaction_active: bool) {
        self.inner().transaction_active = transaction_active;
    }

    pub fn is_transaction_active(&self) -> bool {
        self.inner().transaction_active
    }

    pub fn set_connection(&self, connection: Option<SharedConnection>) {
        let mut inner = self.inner();
        if let Some(current) = inner.current_connection.take()
            && let Some(handle) = &inner.connection_handle
        {
            handle.release_connection(&current);
        }
        inner.connection_handle =
            connection.map(|connection| Arc::new(SimpleConnectionHandle::new(connection)) as Arc<dyn ConnectionHandle>);
    }

    pub fn connection(&self) -> Result<SharedConnection, TransactionError> {
        let mut inner = self.inner();
        let handle = inner
            .connection_handle
            .clone()
            .ok_or_else(|| TransactionError::InvalidState("Active Connection is required".into()))?;
        Ok(inner.current_connection.get_or_insert_with(|| handle.connection()).clone())
    }

    pub async fn supports_savepoints(&self) -> Result<bool, SqlOrState> {
        if let Some(supported) = self.inner().savepoints_supported {
            return Ok(supported);
        }
        let connection = self.connection().map_err(SqlOrState::State)?;
        let supported = connection.lock().await.supports_savepoints().await.map_err(SqlOrState::Sql)?;
        self.inner().savepoints_supported = Some(supported);
        Ok(supported)
    }

    pub async fn create_savepoint(&self) -> Result<Savepoint, SqlOrState> {
        let name = {
            let mut inner = self.inner();
            inner.savepoint_counter += 1;
            format!("{SAVEPOINT_NAME_PREFIX}{}", inner.savepoint_counter)
        };
        let connection = self.connection().map_err(SqlOrState::State)?;
        let savepoint = connection.lock().await.set_savepoint(&name).await.map_err(SqlOrState::Sql)?;
        Ok(savepoint)
    }

    pub fn set_synchronized_with_transaction(&self, synchronized_with_transaction: bool) {
        self.inner().synchronized_with_transaction = synchronized_with_transaction;
    }

    pub fn is_synchronized_with_transaction(&self) -> bool {
        self.inner().synchronized_with_transaction
    }

    pub fn set_rollback_only(&self) {
        self.inner().rollback_only = true;
    }

    pub fn reset_rollback_only(&self) {
        self.inner().rollback_only = false;
    }

    pub fn is_rollback_only(&self) -> bool {
        self.inner().rollback_only
    }

    pub fn set_timeout(&self, timeout: Duration) {
        self.inner().deadline = Some(Instant::now() + timeout);
    }

    pub fn has_timeout(&self) -> bool {
        self.inner().deadline.is_some()
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.inner().deadline
    }

    pub fn time_to_live_in_seconds(&self) -> Result<i32, TransactionError> {
        let millis = self.time_to_live_in_millis()?;
        let seconds = (millis as f64 / 1000.0).ceil() as i32;
        self.check_transaction_timeout(seconds <= 0)?;
        Ok(seconds)
    }

    pub fn time_to_live_in_millis(&self) -> Result<i64, TransactionError> {
        let deadline = self.deadline().ok_or_else(|| {
            TransactionError::InvalidState("No timeout specified for this resource holder".into())
        })?;
        let now = Instant::now();
        let time_to_live = if deadline > now {
            (deadline - now).as_millis() as i64
        } else {
            -((now - deadline).as_millis() as i64)
        };
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
        self.inner().reference_count += 1;
    }

    pub fn released(&self) {
        let mut inner = self.inner();
        inner.reference_count -= 1;
        if inner.reference_count <= 0
            && let Some(current) = inner.current_connection.take()
            && let Some(handle) = &inner.connection_handle
        {
            handle.release_connection(&current);
        }
    }

    pub fn is_open(&self) -> bool {
        self.inner().reference_count > 0
    }

    pub fn clear(&self) {
        let mut inner = self.inner();
        inner.synchronized_with_transaction = false;
        inner.rollback_only = false;
        inner.deadline = None;
        inner.transaction_active = false;
        inner.savepoints_supported = None;
        inner.savepoint_counter = 0;
    }
}

#[derive(Debug)]
pub enum SqlOrState {
    Sql(SqlError),
    State(TransactionError),
}

impl ResourceHolder for ConnectionHolder {
    fn reset(&self) {
        self.clear();
        self.inner().reference_count = 0;
    }
    fn unbound(&self) {
        self.inner().void = true;
    }
    fn is_void(&self) -> bool {
        self.inner().void
    }
}
