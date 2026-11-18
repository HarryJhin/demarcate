use std::any::Any;
use std::fmt;
use std::sync::Arc;

use dtx_core::{BoxError, BoxFuture, Isolation, Savepoint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorKind {
    General,
    FeatureNotSupported,
    Timeout,
}

#[derive(Debug)]
pub struct SqlError {
    pub kind: SqlErrorKind,
    pub message: String,
    pub sql_state: Option<String>,
    pub source: Option<BoxError>,
}

impl SqlError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { kind: SqlErrorKind::General, message: message.into(), sql_state: None, source: None }
    }
    pub fn with_kind(mut self, kind: SqlErrorKind) -> Self {
        self.kind = kind;
        self
    }
    pub fn with_sql_state(mut self, sql_state: impl Into<String>) -> Self {
        self.sql_state = Some(sql_state.into());
        self
    }
    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.source = Some(source.into());
        self
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn std::error::Error + 'static))
    }
}

pub type SqlResult<T> = Result<T, SqlError>;

pub trait Connection: Send + 'static {
    fn auto_commit(&mut self) -> BoxFuture<'_, SqlResult<bool>>;
    fn set_auto_commit(&mut self, auto_commit: bool) -> BoxFuture<'_, SqlResult<()>>;
    fn commit(&mut self) -> BoxFuture<'_, SqlResult<()>>;
    fn rollback(&mut self) -> BoxFuture<'_, SqlResult<()>>;
    fn is_read_only(&mut self) -> BoxFuture<'_, SqlResult<bool>>;
    fn set_read_only(&mut self, read_only: bool) -> BoxFuture<'_, SqlResult<()>>;
    fn transaction_isolation(&mut self) -> BoxFuture<'_, SqlResult<Isolation>>;
    fn set_transaction_isolation(&mut self, isolation: Isolation) -> BoxFuture<'_, SqlResult<()>>;
    fn supports_savepoints(&mut self) -> BoxFuture<'_, SqlResult<bool>>;
    fn set_savepoint<'a>(&'a mut self, name: &'a str) -> BoxFuture<'a, SqlResult<Savepoint>>;
    fn rollback_to_savepoint<'a>(&'a mut self, savepoint: &'a Savepoint) -> BoxFuture<'a, SqlResult<()>>;
    fn release_savepoint<'a>(&'a mut self, savepoint: &'a Savepoint) -> BoxFuture<'a, SqlResult<()>>;
    fn create_statement(&mut self) -> BoxFuture<'_, SqlResult<Box<dyn Statement>>>;
    fn prepare_statement<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, SqlResult<Box<dyn Statement>>>;
    fn warnings(&mut self) -> BoxFuture<'_, SqlResult<Vec<String>>>;
    fn clear_warnings(&mut self) -> BoxFuture<'_, SqlResult<()>>;
    fn close(&mut self) -> BoxFuture<'_, SqlResult<()>>;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait Statement: Send {
    fn execute_update<'a>(&'a mut self, sql: &'a str) -> BoxFuture<'a, SqlResult<u64>>;
    fn set_query_timeout(&mut self, seconds: i32) -> BoxFuture<'_, SqlResult<()>>;
    fn close(&mut self) -> BoxFuture<'_, SqlResult<()>>;
}

pub type SharedConnection = Arc<tokio::sync::Mutex<Box<dyn Connection>>>;

pub fn shared(connection: impl Connection) -> SharedConnection {
    Arc::new(tokio::sync::Mutex::new(Box::new(connection)))
}

pub trait DataSource: Send + Sync + 'static {
    fn connection(&self) -> BoxFuture<'_, SqlResult<SharedConnection>>;

    fn should_close(&self, _connection: &SharedConnection) -> bool {
        true
    }
}

pub trait ConnectionHandle: Send + Sync {
    fn connection(&self) -> SharedConnection;

    fn release_connection(&self, _connection: &SharedConnection) {}
}

pub struct SimpleConnectionHandle {
    connection: SharedConnection,
}

impl SimpleConnectionHandle {
    pub fn new(connection: SharedConnection) -> Self {
        Self { connection }
    }
}

impl ConnectionHandle for SimpleConnectionHandle {
    fn connection(&self) -> SharedConnection {
        self.connection.clone()
    }
}
