use std::time::Duration;

use demarcate_core::{Isolation, TransactionDefinition, TransactionError};
use sqlx::{AssertSqlSafe, Executor, MySql, MySqlConnection, MySqlPool, SqlStr, Transaction};

const UNUSABLE_MESSAGE: &str = "Transaction rolled back because a statement in it failed and the failure was not \
     returned from the transactional function";

fn validate(_definition: &TransactionDefinition) -> Result<(), TransactionError> {
    Ok(())
}

async fn begin(
    pool: &MySqlPool,
    definition: &TransactionDefinition,
) -> Result<Transaction<'static, MySql>, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    let isolation = match definition.isolation {
        Isolation::Default => None,
        Isolation::ReadUncommitted => Some("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"),
        Isolation::ReadCommitted => Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
        Isolation::RepeatableRead => Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        Isolation::Serializable => Some("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    };
    if let Some(statement) = isolation {
        connection.execute(statement).await?;
    }
    let statement = if definition.read_only { "START TRANSACTION READ ONLY" } else { "START TRANSACTION" };
    Transaction::begin(connection, Some(SqlStr::from_static(statement))).await
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

fn ends_transaction(error: &sqlx::Error) -> bool {
    error.as_database_error().and_then(|database| database.code()).is_some_and(|code| code == "40001")
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

When a statement fails with a deadlock (SQLSTATE `40001`), the server has rolled the whole
transaction back and the connection is in autocommit mode. Every query that follows in that
transaction fails with an error instead of running outside the transaction, and the transaction
cannot commit.

Note that a statement that changes the schema, such as `CREATE TABLE`, commits the transaction on
the server.",
    acquire_note: "Note that a statement that fails on the returned connection is not recorded, so a deadlock on
it is not detected.",
}
