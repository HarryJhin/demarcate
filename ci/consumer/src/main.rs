use std::sync::Arc;

use demarcate::postgres::{TransactionalPool, manager};
use demarcate::transactional;

#[derive(Debug)]
enum Error {
    Transaction(demarcate::TransactionError),
    Sql(sqlx::Error),
}

impl From<demarcate::TransactionError> for Error {
    fn from(error: demarcate::TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transaction(error) => write!(f, "{error}"),
            Self::Sql(error) => write!(f, "{error}"),
        }
    }
}

struct Orders {
    pool: TransactionalPool,
}

impl Orders {
    #[transactional]
    pub async fn place(&self, item: &str) -> Result<(), Error> {
        self.insert(item).await?;
        self.audit("placed").await
    }

    #[transactional(propagation = RequiresNew)]
    async fn audit(&self, event: &str) -> Result<(), Error> {
        sqlx::query("insert into audit values($1)").bind(event.to_owned()).execute(&self.pool).await?;
        Ok(())
    }

    async fn insert(&self, item: &str) -> Result<(), Error> {
        sqlx::query("insert into orders values($1)").bind(item.to_owned()).execute(&self.pool).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pg_pool = sqlx::PgPool::connect(&url).await?;
    sqlx::query("create table if not exists orders(item text)").execute(&pg_pool).await?;
    sqlx::query("create table if not exists audit(event text)").execute(&pg_pool).await?;

    let pool = TransactionalPool::new(pg_pool);
    demarcate::registry().set_default(Arc::new(manager(pool.clone())));

    Orders { pool }.place("book").await
}
