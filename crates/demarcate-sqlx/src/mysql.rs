use std::time::Duration;

use demarcate_core::{Isolation, TransactionDefinition, TransactionError};
use std::ops::{Deref, DerefMut};

use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Executor, MySql, MySqlConnection, MySqlPool};

const UNUSABLE_MESSAGE: &str = "Transaction rolled back because a statement in it failed and the failure was not \
     returned from the transactional function";

fn validate(_definition: &TransactionDefinition) -> Result<(), TransactionError> {
    Ok(())
}

/// A connection with a transaction that this driver started.
///
/// sqlx's own `Transaction` cannot be used here: when a `START TRANSACTION` of sqlx-mysql is
/// cancelled, the connection goes back to the pool with the transaction open. This type closes
/// its connection when it is dropped before the transaction has completed.
#[doc(hidden)]
pub struct Started {
    connection: Option<PoolConnection<MySql>>,
    open: bool,
}

impl Started {
    async fn finish(mut self, statement: &'static str) -> Result<(), sqlx::Error> {
        (&mut *self).execute(statement).await?;
        self.open = false;
        Ok(())
    }
}

impl Deref for Started {
    type Target = MySqlConnection;
    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().expect("connection present")
    }
}

impl DerefMut for Started {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().expect("connection present")
    }
}

impl Drop for Started {
    fn drop(&mut self) {
        if self.open
            && let Some(connection) = self.connection.take()
        {
            drop(connection.detach());
        }
    }
}

async fn begin(pool: &MySqlPool, definition: &TransactionDefinition) -> Result<Started, sqlx::Error> {
    let mut started = Started { connection: Some(pool.acquire().await?), open: true };
    let isolation = match definition.isolation {
        Isolation::Default => None,
        Isolation::ReadUncommitted => Some("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"),
        Isolation::ReadCommitted => Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
        Isolation::RepeatableRead => Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        Isolation::Serializable => Some("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    };
    if let Some(statement) = isolation {
        (&mut *started).execute(statement).await?;
    }
    let statement = if definition.read_only { "START TRANSACTION READ ONLY" } else { "START TRANSACTION" };
    (&mut *started).execute(statement).await?;
    Ok(started)
}

async fn commit(started: Started) -> Result<(), sqlx::Error> {
    started.finish("COMMIT").await
}

async fn rollback(started: Started) -> Result<(), sqlx::Error> {
    started.finish("ROLLBACK").await
}

async fn execute(connection: &mut MySqlConnection, statement: &str) -> Result<(), sqlx::Error> {
    connection.execute(AssertSqlSafe(statement.to_owned())).await.map(|_| ())
}

async fn apply_timeout(_connection: &mut MySqlConnection, _remaining: Duration) -> Result<(), sqlx::Error> {
    Ok(())
}

fn taints_transaction(_error: &sqlx::Error) -> bool {
    false
}

// A deadlock (1213, SQLSTATE 40001) rolls the whole transaction back. So does MariaDB's error
// 1020 "Record has changed since last read", which innodb_snapshot_isolation raises.
fn ends_transaction(error: &sqlx::Error) -> bool {
    let Some(database) = error.as_database_error() else {
        return false;
    };
    let number = database.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>().map(|error| error.number());
    database.code().is_some_and(|code| code == "40001") || matches!(number, Some(1213 | 1020))
}

async fn is_usable(_connection: &mut MySqlConnection) -> bool {
    true
}

fn release_failure_is_ignorable(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some_and(|database| {
        database.code().is_some_and(|code| code == "42000") && database.message().contains("does not exist")
    })
}

sqlx_driver! {
    database: MySql,
    pool: MySqlPool,
    connection: MySqlConnection,
    transaction: Started,
    backend: MySqlBackend,
    object: MySqlTransactionObject,
    driver: MySqlTransactionDriver,
    manager: MySqlTransactionManager,
    name: "demarcate-sqlx-mysql",
    database_name: "MySQL and MariaDB",
    backend_doc: "The isolation level is set with `SET TRANSACTION ISOLATION LEVEL` before the transaction starts, and a
read-only transaction starts with `START TRANSACTION READ ONLY`, so the server rejects a write in it.",
    pool_doc: "A transaction with a timeout checks its deadline whenever a query starts: a query that starts
after the deadline fails with [`TransactionError::TimedOut`]. A
query that is running at the deadline is not interrupted. The commit does not check the deadline.

When a statement fails with a deadlock (SQLSTATE `40001`), or on MariaDB with error 1020
\"Record has changed since last read\", the server has rolled the whole transaction back and the
connection is in autocommit mode. Every query that follows in that
transaction fails with an error instead of running outside the transaction, and the transaction
cannot commit.

Note that a statement that changes the schema, such as `CREATE TABLE`, commits the transaction on
the server. The statements before it stay committed, the statements after it run in autocommit
mode, and a rollback undoes nothing.

Do not begin a transaction on a connection of this pool with sqlx's own `begin` inside a
transactional function: MySQL commits the running transaction when another one begins.",
    acquire_note: "Note that a statement that fails on the returned connection is not recorded, so a deadlock on
it is not detected.",
}
