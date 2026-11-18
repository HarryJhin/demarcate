// The data access of the conformance tests for a sqlx driver. Included inside `mod db`, which
// defines `Raw`, `TransactionalPool` and `placeholders`.

use demarcate::TransactionError;
use futures_util::TryStreamExt;
use sqlx::{AssertSqlSafe, Row};

pub type Error = sqlx::Error;

pub fn transactional(raw: &Raw) -> TransactionalPool {
    TransactionalPool::new(raw.clone())
}

pub async fn raw_execute(raw: &Raw, sql: &str) {
    sqlx::query(AssertSqlSafe(sql.to_owned())).execute(raw).await.unwrap();
}

pub async fn raw_values(raw: &Raw, sql: &str) -> Vec<String> {
    let rows = sqlx::query(AssertSqlSafe(sql.to_owned())).fetch_all(raw).await.unwrap();
    rows.iter().map(|row| row.get::<String, _>(0)).collect()
}

pub async fn insert(pool: &TransactionalPool, table: &str, value: &str) -> Result<(), Error> {
    sqlx::query(AssertSqlSafe(placeholders(&format!("insert into {table} values($1)"))))
        .bind(value.to_owned())
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
    let rows = sqlx::query(AssertSqlSafe(format!("select v from {table} order by v"))).fetch_all(pool).await?;
    Ok(rows.iter().map(|row| row.get::<String, _>(0)).collect())
}

pub async fn stream_values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
    let mut values = Vec::new();
    let mut rows = sqlx::query(AssertSqlSafe(format!("select v from {table} order by v"))).fetch(pool);
    while let Some(row) = rows.try_next().await? {
        values.push(row.get::<String, _>(0));
    }
    Ok(values)
}

#[allow(dead_code)]
pub async fn increment_marker(pool: &TransactionalPool, table: &str, value: &str) -> Result<(), Error> {
    sqlx::query(AssertSqlSafe(placeholders(&format!("update {table} set marker = marker + 1 where v = $1"))))
        .bind(value.to_owned())
        .execute(pool)
        .await
        .map(|_| ())
}

pub fn transaction_error(error: &Error) -> Option<&TransactionError> {
    match error {
        sqlx::Error::Configuration(error) => error.downcast_ref::<TransactionError>(),
        _ => None,
    }
}

pub fn is_database_error(error: &Error) -> bool {
    error.as_database_error().is_some()
}

#[allow(dead_code)]
pub fn sqlstate(error: &Error) -> Option<String> {
    error.as_database_error().and_then(|database| database.code()).map(|code| code.into_owned())
}
