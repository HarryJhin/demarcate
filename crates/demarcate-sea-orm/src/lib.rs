//! Transaction management for SeaORM.
//!
//! [`TransactionalDatabase`] wraps a [`DatabaseConnection`] and implements
//! [`ConnectionTrait`], so that entity operations and statements run on the connection of the
//! current transaction without a transaction argument:
//! `Entity::find().all(&database)`.
//!
//! The database is the one of the wrapped connection: PostgreSQL, MySQL, MariaDB or SQLite.
//! Applications normally depend on the `demarcate` crate with the `sea-orm` feature.

#![warn(missing_docs)]

use std::time::Duration;

use demarcate_connection::{ConnectionBackend, ConnectionDriver, ConnectionHandle, ConnectionSource};
use demarcate_core::{
    Isolation, ManagerSettings, ResourceKey, TransactionDefinition, TransactionError, TransactionManager,
};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, ExecResult, IsolationLevel,
    QueryResult, RuntimeErr, Statement, TransactionTrait, sqlx,
};

pub use demarcate_connection::CONNECTION_SYNCHRONIZATION_ORDER;

#[doc(hidden)]
pub enum Conn {
    Plain(DatabaseConnection),
    Transaction(DatabaseTransaction),
}

impl Conn {
    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        match self {
            Self::Plain(connection) => connection.execute_unprepared(sql).await,
            Self::Transaction(transaction) => transaction.execute_unprepared(sql).await,
        }
    }
}

/// The [`ConnectionBackend`] for SeaORM.
///
/// The isolation level and the read-only flag are passed to SeaORM when the transaction begins.
/// On SQLite only `Isolation::Default` and `Isolation::Serializable` are accepted, and the
/// read-only flag is not enforced.
pub struct SeaOrmBackend {
    connection: DatabaseConnection,
}

fn database_error(error: &DbErr) -> Option<&sqlx::Error> {
    match error {
        DbErr::Conn(RuntimeErr::SqlxError(error))
        | DbErr::Exec(RuntimeErr::SqlxError(error))
        | DbErr::Query(RuntimeErr::SqlxError(error)) => Some(error),
        _ => None,
    }
}

fn sqlstate(error: &DbErr) -> Option<String> {
    database_error(error)?.as_database_error()?.code().map(|code| code.into_owned())
}

impl ConnectionBackend for SeaOrmBackend {
    type Connection = Conn;
    type Error = DbErr;

    const NAME: &'static str = "demarcate-sea-orm";

    const UNUSABLE_MESSAGE: &'static str = "Transaction rolled back because a statement in it failed. PostgreSQL aborts a transaction \
         when a statement fails, and the failure was not returned from the transactional function. Return the \
         error, or run the statement that may fail in a nested transaction (propagation = Nested)";

    fn validate(&self, definition: &TransactionDefinition) -> Result<(), TransactionError> {
        match (self.connection.get_database_backend(), definition.isolation) {
            (DbBackend::Sqlite, Isolation::Default | Isolation::Serializable) => Ok(()),
            (DbBackend::Sqlite, other) => Err(TransactionError::InvalidIsolationLevel(format!(
                "SQLite runs every transaction serializable and does not support the isolation level {other:?}. Use \
                 Isolation::Default or Isolation::Serializable"
            ))),
            _ => Ok(()),
        }
    }

    async fn acquire(&self) -> Result<Conn, DbErr> {
        Ok(Conn::Plain(self.connection.clone()))
    }

    async fn begin(&self, definition: &TransactionDefinition) -> Result<Conn, DbErr> {
        if self.connection.get_database_backend() == DbBackend::Sqlite {
            return Ok(Conn::Transaction(self.connection.begin().await?));
        }
        let isolation = match definition.isolation {
            Isolation::Default => None,
            Isolation::ReadUncommitted => Some(IsolationLevel::ReadUncommitted),
            Isolation::ReadCommitted => Some(IsolationLevel::ReadCommitted),
            Isolation::RepeatableRead => Some(IsolationLevel::RepeatableRead),
            Isolation::Serializable => Some(IsolationLevel::Serializable),
        };
        let access = definition.read_only.then_some(AccessMode::ReadOnly);
        Ok(Conn::Transaction(self.connection.begin_with_config(isolation, access).await?))
    }

    fn in_transaction(connection: &Conn) -> bool {
        matches!(connection, Conn::Transaction(_))
    }

    async fn commit(&self, connection: Conn) -> Result<(), DbErr> {
        match connection {
            Conn::Transaction(transaction) => transaction.commit().await,
            Conn::Plain(_) => Ok(()),
        }
    }

    async fn rollback(&self, connection: Conn) -> Result<(), DbErr> {
        match connection {
            Conn::Transaction(transaction) => transaction.rollback().await,
            Conn::Plain(_) => Ok(()),
        }
    }

    async fn execute(&self, connection: &mut Conn, statement: &str) -> Result<(), DbErr> {
        connection.execute_unprepared(statement).await.map(|_| ())
    }

    async fn apply_timeout(&self, connection: &mut Conn, remaining: Duration) -> Result<(), DbErr> {
        if self.connection.get_database_backend() != DbBackend::Postgres {
            return Ok(());
        }
        let millis = remaining.as_millis().max(1);
        connection
            .execute_unprepared(&format!("select set_config('statement_timeout', '{millis}', true)"))
            .await
            .map(|_| ())
    }

    fn taints_transaction(&self, error: &DbErr) -> bool {
        self.connection.get_database_backend() == DbBackend::Postgres
            && database_error(error).is_some_and(|error| error.as_database_error().is_some())
    }

    fn ends_transaction(&self, error: &DbErr) -> bool {
        self.connection.get_database_backend() == DbBackend::MySql
            && sqlstate(error).is_some_and(|code| code == "40001")
    }

    async fn is_usable(&self, connection: &mut Conn) -> bool {
        self.connection.get_database_backend() != DbBackend::Postgres
            || connection.execute_unprepared("select 1").await.is_ok()
    }

    fn release_failure_is_ignorable(&self, error: &DbErr) -> bool {
        let Some(database) = database_error(error).and_then(|error| error.as_database_error()) else {
            return false;
        };
        let code = database.code();
        match self.connection.get_database_backend() {
            DbBackend::Postgres => code.is_some_and(|code| code == "3B001"),
            DbBackend::MySql => {
                code.is_some_and(|code| code == "42000") && database.message().contains("does not exist")
            }
            DbBackend::Sqlite => database.message().contains("no such savepoint"),
            _ => false,
        }
    }
}

/// The connection of one transaction, or of one synchronization scope without a transaction,
/// bound to the transaction context under the key of its [`TransactionalDatabase`].
pub type ConnectionHolder = demarcate_connection::ConnectionHolder<SeaOrmBackend>;

/// A SeaORM connection that runs every query on the connection of the current transaction.
///
/// Pass a reference to it wherever SeaORM takes a [`ConnectionTrait`]:
/// `Entity::find().all(&database)`, `model.insert(&database)`. Inside a transaction the query
/// runs in that transaction. Outside of one it runs on the wrapped connection in autocommit
/// mode, unless this value was made with
/// [`transaction_required`](TransactionalDatabase::transaction_required).
///
/// The queries of one transaction must be awaited one after another. Awaiting two of them at
/// once returns an error; it does not open a second connection.
///
/// A transaction with a timeout checks its deadline whenever a query starts: a query that starts
/// after the deadline fails with [`TransactionError::TimedOut`]. On PostgreSQL the server also
/// cancels a statement that is running at the deadline.
///
/// On PostgreSQL a statement that fails inside a transaction is recorded, and the commit returns
/// [`TransactionError::UnexpectedRollback`] if the server has aborted the transaction. On MySQL
/// and MariaDB a deadlock (SQLSTATE `40001`) ends the transaction on the server; every query
/// that follows in that transaction fails with an error instead of running outside it.
///
/// An error of the transaction infrastructure raised during a query arrives as
/// `DbErr::Conn(RuntimeErr::SqlxError(_))` that holds a `sqlx::Error::Configuration` with the
/// [`TransactionError`]; [`transaction_error`] returns it.
///
/// Do not begin a transaction on the wrapped connection with SeaORM's own `begin` or
/// `transaction` inside a transactional function: that transaction is a separate one.
///
/// Cloning is cheap; clones share the connection and the transaction.
#[derive(Clone, Debug)]
pub struct TransactionalDatabase {
    source: ConnectionSource<SeaOrmBackend>,
    backend: DbBackend,
}

/// Returns the error of the transaction infrastructure that a query through
/// [`TransactionalDatabase`] failed with, or `None` when the error has another cause.
pub fn transaction_error(error: &DbErr) -> Option<&TransactionError> {
    match error {
        DbErr::Conn(RuntimeErr::SqlxError(error)) => match &**error {
            sqlx::Error::Configuration(error) => error.downcast_ref::<TransactionError>(),
            _ => None,
        },
        _ => None,
    }
}

fn into_db_err(error: TransactionError) -> DbErr {
    DbErr::Conn(RuntimeErr::SqlxError(std::sync::Arc::new(sqlx::Error::Configuration(Box::new(error)))))
}

impl TransactionalDatabase {
    /// Wraps a SeaORM connection.
    ///
    /// Create one `TransactionalDatabase` for each `DatabaseConnection` and clone it. Two values
    /// created from the same connection do not see each other's transactions.
    pub fn new(connection: DatabaseConnection) -> Self {
        let backend = connection.get_database_backend();
        Self { source: ConnectionSource::new(SeaOrmBackend { connection }), backend }
    }

    /// Returns a connection that refuses to run a query outside a transaction.
    ///
    /// Give it to a repository whose functions must always run inside a transaction. A call
    /// without one returns [`TransactionError::NoTransaction`] instead of running in autocommit mode.
    pub fn transaction_required(&self) -> Self {
        Self { source: self.source.transaction_required(), backend: self.backend }
    }

    /// Returns whether this connection refuses to run a query outside a transaction.
    pub fn is_transaction_required(&self) -> bool {
        self.source.is_transaction_required()
    }

    /// Returns the SeaORM connection. A query on it does not take part in any transaction.
    pub fn inner(&self) -> &DatabaseConnection {
        &self.source.backend().connection
    }

    /// Returns the key under which the connection of a transaction is bound to the context.
    pub fn resource_key(&self) -> &ResourceKey {
        self.source.resource_key()
    }

    /// Returns whether a connection of this value is bound to the context of the calling future.
    pub fn is_connection_transactional(&self) -> bool {
        self.source.is_connection_transactional()
    }

    async fn acquire(&self) -> Result<ConnectionHandle<SeaOrmBackend>, DbErr> {
        self.source.acquire().await.map_err(into_db_err)
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for TransactionalDatabase {
    fn get_database_backend(&self) -> DbBackend {
        self.backend
    }

    async fn execute_raw(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        let connection = self.acquire().await?;
        let result = match &*connection {
            Conn::Plain(plain) => plain.execute_raw(stmt).await,
            Conn::Transaction(transaction) => transaction.execute_raw(stmt).await,
        };
        connection.statement_finished(result.as_ref().err());
        result
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        let connection = self.acquire().await?;
        let result = connection.execute_unprepared(sql).await;
        connection.statement_finished(result.as_ref().err());
        result
    }

    async fn query_one_raw(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        let connection = self.acquire().await?;
        let result = match &*connection {
            Conn::Plain(plain) => plain.query_one_raw(stmt).await,
            Conn::Transaction(transaction) => transaction.query_one_raw(stmt).await,
        };
        connection.statement_finished(result.as_ref().err());
        result
    }

    async fn query_all_raw(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        let connection = self.acquire().await?;
        let result = match &*connection {
            Conn::Plain(plain) => plain.query_all_raw(stmt).await,
            Conn::Transaction(transaction) => transaction.query_all_raw(stmt).await,
        };
        connection.statement_finished(result.as_ref().err());
        result
    }

    fn support_returning(&self) -> bool {
        self.inner().support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.inner().is_mock_connection()
    }
}

#[doc(hidden)]
pub type SeaOrmTransactionObject = demarcate_connection::ConnectionTransactionObject<SeaOrmBackend>;

/// The [`TransactionDriver`](demarcate_core::TransactionDriver) for SeaORM.
///
/// A nested transaction is a savepoint on the connection of the outer transaction. While it
/// runs, that connection belongs to the nested call, and a future beside it that uses the
/// connection gets an error.
pub type SeaOrmTransactionDriver = ConnectionDriver<SeaOrmBackend>;

/// The transaction manager for SeaORM.
pub type SeaOrmTransactionManager = TransactionManager<SeaOrmTransactionDriver>;

/// Creates a transaction manager for the given connection, with nested transactions allowed.
///
/// Register it once at startup:
/// `demarcate::registry().set_default(Arc::new(manager(database.clone())))`.
pub fn manager(database: TransactionalDatabase) -> SeaOrmTransactionManager {
    demarcate_connection::manager(database.source)
}

/// Creates a transaction manager for the given connection with the given settings.
pub fn manager_with_settings(database: TransactionalDatabase, settings: ManagerSettings) -> SeaOrmTransactionManager {
    demarcate_connection::manager_with_settings(database.source, settings)
}
