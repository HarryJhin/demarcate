use demarcate::postgres::TransactionalPool;
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

impl std::error::Error for Error {}

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
        sqlx::query("insert into quickstart_audit values($1)").bind(event.to_owned()).execute(&self.pool).await?;
        Ok(())
    }

    async fn insert(&self, item: &str) -> Result<(), Error> {
        sqlx::query("insert into quickstart_orders values($1)").bind(item.to_owned()).execute(&self.pool).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = demarcate::postgres::install_from_env().await?;
    sqlx::query("create table if not exists quickstart_orders(item text)").execute(&pool).await?;
    sqlx::query("create table if not exists quickstart_audit(event text)").execute(&pool).await?;

    Orders { pool }.place("book").await?;
    Ok(())
}
