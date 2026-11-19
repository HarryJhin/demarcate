// The default manager is one per process, so this file holds one test.

use demarcate::sqlite::{TransactionalPool, install};
use demarcate::{InstallError, TransactionError, transactional};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[derive(Debug)]
#[allow(dead_code)]
enum Error {
    Transaction(TransactionError),
    Sql(sqlx::Error),
    Rejected,
}

impl From<TransactionError> for Error {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

struct Notes {
    pool: TransactionalPool,
}

impl Notes {
    #[transactional]
    async fn add(&self, note: &str, reject: bool) -> Result<(), Error> {
        sqlx::query("insert into notes values($1)").bind(note.to_owned()).execute(&self.pool).await?;
        if reject { Err(Error::Rejected) } else { Ok(()) }
    }
}

async fn database(name: &str) -> SqlitePool {
    let path = std::env::temp_dir().join(format!("demarcate-install-default-{name}-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let options = SqliteConnectOptions::new().filename(&path).create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(options).await.unwrap();
    sqlx::query("create table notes(v text)").execute(&pool).await.unwrap();
    pool
}

async fn notes(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar("select v from notes order by v").fetch_all(pool).await.unwrap()
}

#[tokio::test]
async fn install_registers_the_default_manager_once() {
    let first = database("first").await;
    let second = database("second").await;

    let before = Notes { pool: TransactionalPool::new(first.clone()) }.add("early", false).await;
    assert!(
        matches!(&before, Err(Error::Transaction(TransactionError::InvalidState(message))) if message.contains("install(pool)")),
        "{before:?}"
    );
    assert!(notes(&first).await.is_empty());

    let service = Notes { pool: install(first.clone()).unwrap() };
    service.add("kept", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));
    assert_eq!(notes(&first).await, ["kept"]);

    assert_eq!(install(second.clone()).map(|_| ()), Err(InstallError::DefaultAlreadyInstalled));
    service.add("later", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));
    assert_eq!(notes(&first).await, ["kept", "later"]);
    assert!(notes(&second).await.is_empty());
}
