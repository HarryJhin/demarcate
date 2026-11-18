use std::sync::{Arc, Mutex};

use demarcate::postgres::{PgTransactionManager, TransactionalPool, manager};
use demarcate::{
    PlatformTransactionManager, TransactionError, TransactionPhase, on_transaction, registry, synchronization,
    transactional,
};
use sqlx::{AssertSqlSafe, Row};

const TABLE: &str = "demarcate_completion";

#[derive(Debug)]
enum ServiceError {
    Transaction(#[allow(dead_code)] TransactionError),
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

struct Service {
    pool: TransactionalPool,
    manager: Arc<PgTransactionManager>,
    after_commit: Mutex<Vec<(&'static str, bool)>>,
}

impl Service {
    async fn insert(&self, value: &str) -> Result<(), ServiceError> {
        sqlx::query(AssertSqlSafe(format!("insert into {TABLE} values($1)")))
            .bind(value.to_owned())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn rows(&self) -> Vec<String> {
        sqlx::query(AssertSqlSafe(format!("select v from {TABLE} order by v")))
            .fetch_all(self.pool.inner())
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>(0))
            .collect()
    }

    #[transactional]
    async fn insert_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn insert_in_transaction(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(propagation = RequiresNew)]
    async fn insert_in_new_transaction(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional]
    async fn commit_with_data_access_after_commit(self: &Arc<Self>) -> Result<(), ServiceError> {
        self.insert("outer").await?;
        let service = self.clone();
        on_transaction(TransactionPhase::AfterCommit, false, move || {
            let service = service.clone();
            async move {
                let plain = service.insert("plain").await.is_ok();
                let failing = service.insert_then_fail("failing_required").await.is_ok();
                let required = service.insert_in_transaction("required").await.is_ok();
                let independent = service.insert_in_new_transaction("requires_new").await.is_ok();
                *service.after_commit.lock().unwrap() =
                    vec![("plain", plain), ("failing", failing), ("required", required), ("requires_new", independent)];
                Ok(())
            }
        })
        .await
        .map_err(|_| ServiceError::Rejected)?;
        Ok(())
    }
}

async fn service() -> Arc<Service> {
    let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
    let connections = sqlx::postgres::PgPoolOptions::new().max_connections(6).connect(&url).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("drop table if exists {TABLE}"))).execute(&connections).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("create table {TABLE}(v text)"))).execute(&connections).await.unwrap();
    let pool = TransactionalPool::new(connections);
    let manager = Arc::new(manager(pool.clone()));
    registry().set_default(manager.clone());
    Arc::new(Service { pool, manager, after_commit: Mutex::new(Vec::new()) })
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn data_access_after_commit_is_kept_only_in_a_new_transaction() {
    let service = service().await;

    service.commit_with_data_access_after_commit().await.unwrap();

    assert_eq!(
        *service.after_commit.lock().unwrap(),
        [("plain", true), ("failing", false), ("required", true), ("requires_new", true)]
    );
    assert_eq!(service.rows().await, ["outer", "requires_new"]);

    sqlx::query(AssertSqlSafe(format!("truncate {TABLE}"))).execute(service.pool.inner()).await.unwrap();
    let rolled_back = synchronization::scope(async {
        let status = service.manager.transaction(None).await.unwrap();
        service.insert("before").await.unwrap();
        let (step, beside) = tokio::join!(service.insert_in_transaction("step"), service.insert("beside"));
        service.insert("after").await.unwrap();
        let outcome = (step.is_ok(), beside.is_ok());
        service.manager.rollback(status.as_ref()).await.unwrap();
        outcome
    })
    .await;
    assert!(rolled_back.0, "the transactional call joins the transaction opened through the manager");
    assert!(service.rows().await.is_empty(), "{:?} {:?}", rolled_back, service.rows().await);
}
