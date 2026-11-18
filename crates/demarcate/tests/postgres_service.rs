use std::sync::Arc;

use demarcate::postgres::{TransactionalPool, manager};
use demarcate::{TransactionError, current_transaction_status, registry, synchronization, transactional};
use sqlx::{AssertSqlSafe, Row};

#[derive(Debug)]
enum ServiceError {
    Transaction(TransactionError),
    Sql(#[allow(dead_code)] sqlx::Error),
    Rejected,
}

impl From<TransactionError> for ServiceError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<sqlx::Error> for ServiceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

struct Repository {
    pool: TransactionalPool,
    table: String,
}

impl Repository {
    async fn insert(&self, value: &str) -> Result<(), ServiceError> {
        sqlx::query(AssertSqlSafe(format!("insert into {} values($1)", self.table)))
            .bind(value.to_owned())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn insert_strict(&self, value: &str) -> Result<(), ServiceError> {
        sqlx::query(AssertSqlSafe(format!("insert into {} values($1)", self.table)))
            .bind(value.to_owned())
            .execute(&self.pool.transaction_required())
            .await?;
        Ok(())
    }

    async fn stream_values(&self) -> Result<Vec<String>, ServiceError> {
        use futures_util::TryStreamExt;
        let mut rows = sqlx::query(AssertSqlSafe(format!("select v from {} order by v", self.table))).fetch(&self.pool);
        let mut values = Vec::new();
        while let Some(row) = rows.try_next().await? {
            values.push(row.get::<String, _>(0));
        }
        Ok(values)
    }

    async fn read_only_setting(&self) -> Result<String, ServiceError> {
        Ok(sqlx::query("show transaction_read_only").fetch_one(&self.pool).await?.get(0))
    }

    async fn rows(&self) -> Vec<String> {
        sqlx::query(AssertSqlSafe(format!("select v from {} order by v", self.table)))
            .fetch_all(self.pool.inner())
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>(0))
            .collect()
    }
}

struct Service {
    repository: Repository,
}

impl Service {
    #[transactional]
    async fn register(&self, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(value).await
    }

    #[transactional]
    async fn register_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(value).await?;
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn register_two(&self, first: &str, second: &str) -> Result<(), ServiceError> {
        self.register(first).await?;
        self.register_then_fail(second).await
    }

    #[transactional(propagation = RequiresNew)]
    async fn audit(&self, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(value).await
    }

    #[transactional]
    async fn register_with_audit_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(value).await?;
        self.audit("audit").await?;
        Err(ServiceError::Rejected)
    }

    #[transactional(read_only)]
    async fn read_only_setting(&self) -> Result<String, ServiceError> {
        self.repository.read_only_setting().await
    }

    #[transactional]
    async fn register_many_and_read(&self, values: &[&str]) -> Result<Vec<String>, ServiceError> {
        for value in values {
            self.repository.insert_strict(value).await?;
        }
        self.repository.stream_values().await
    }

    #[transactional(propagation = Mandatory)]
    async fn requires_caller_transaction(&self, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(value).await
    }

    #[transactional]
    async fn mark_rollback_only(&self, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(value).await?;
        current_transaction_status()?.set_rollback_only()?;
        Ok(())
    }
}

async fn service() -> Option<Arc<Service>> {
    let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
    let connections = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
    sqlx::query("drop table if exists demarcate_service").execute(&connections).await.unwrap();
    sqlx::query("create table demarcate_service(v text)").execute(&connections).await.unwrap();
    let pool = TransactionalPool::new(connections);
    registry().set_default(Arc::new(manager(pool.clone())));
    Some(Arc::new(Service { repository: Repository { pool, table: "demarcate_service".into() } }))
}

async fn reset(service: &Service) {
    sqlx::query("truncate demarcate_service").execute(service.repository.pool.inner()).await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn declarative_transactions_on_postgres() {
    let Some(service) = service().await else { return };
    let service = &service;

    service.register("a").await.unwrap();
    assert_eq!(service.repository.rows().await, ["a"]);

    reset(service).await;
    assert!(matches!(service.register_then_fail("a").await, Err(ServiceError::Rejected)));
    assert!(service.repository.rows().await.is_empty());

    reset(service).await;
    assert!(matches!(service.register_two("a", "b").await, Err(ServiceError::Rejected)));
    assert!(service.repository.rows().await.is_empty(), "participating failure rolls back the whole transaction");

    reset(service).await;
    assert!(matches!(service.register_with_audit_then_fail("a").await, Err(ServiceError::Rejected)));
    assert_eq!(service.repository.rows().await, ["audit"], "REQUIRES_NEW commits independently");

    assert_eq!(service.read_only_setting().await.unwrap(), "on");

    reset(service).await;
    match service.requires_caller_transaction("a").await {
        Err(ServiceError::Transaction(TransactionError::InvalidTransactionState(message))) => {
            assert!(message.contains("mandatory"), "{message}");
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(service.repository.rows().await.is_empty());

    reset(service).await;
    service.mark_rollback_only("a").await.unwrap();
    assert!(service.repository.rows().await.is_empty(), "rollback-only commits as a rollback");

    reset(service).await;
    match service.repository.insert_strict("a").await {
        Err(ServiceError::Sql(error)) => {
            assert!(error.to_string().contains("No transaction is active"), "{error}");
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(service.repository.rows().await.is_empty());

    reset(service).await;
    let read = service.register_many_and_read(&["x", "y", "z"]).await.unwrap();
    assert_eq!(read, ["x", "y", "z"], "a stream reads the uncommitted rows of its own transaction");
    assert_eq!(service.repository.rows().await, ["x", "y", "z"]);

    reset(service).await;
    service.repository.insert("auto-commit").await.unwrap();
    assert_eq!(
        service.repository.rows().await,
        ["auto-commit"],
        "acquire() without a transaction commits each statement"
    );

    reset(service).await;
    let spawned = service.clone();
    let joined = synchronization::spawn(async move {
        let strict_outside = spawned.repository.insert_strict("outside").await.is_err();
        spawned.register("spawned").await.map(|()| strict_outside)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(joined, "a spawned task has no inherited transaction");
    assert_eq!(service.repository.rows().await, ["spawned"]);
}
