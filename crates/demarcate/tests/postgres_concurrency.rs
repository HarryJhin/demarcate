use std::sync::Arc;
use std::time::Duration;

use demarcate::postgres::{PgTransactionManager, TransactionalPool, manager};
use demarcate::{
    PlatformTransactionManager, TransactionError, TransactionPhase, current_transaction_status, on_transaction,
    registry, synchronization, transactional,
};
use sqlx::{AssertSqlSafe, Row};

const TABLE: &str = "demarcate_concurrency";

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
}

struct SiblingReport {
    insert: Result<(), ServiceError>,
    listener_registered: Option<bool>,
    saw_status: bool,
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

    async fn sibling(&self, value: &str) -> SiblingReport {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let listener_registered = on_transaction(TransactionPhase::AfterCommit, false, || async { Ok(()) }).await.ok();
        let saw_status = current_transaction_status().is_ok();
        let insert = self.insert(value).await;
        SiblingReport { insert, listener_registered, saw_status }
    }

    #[transactional(propagation = RequiresNew)]
    async fn slow_audit(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        tokio::time::sleep(Duration::from_millis(80)).await;
        Ok(())
    }

    #[transactional]
    async fn slow_step(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        tokio::time::sleep(Duration::from_millis(80)).await;
        Ok(())
    }

    #[transactional]
    async fn fails_after_audit_beside_query(&self) -> Result<SiblingReport, ServiceError> {
        self.insert("before").await?;
        let (audit, sibling) = tokio::join!(self.slow_audit("audit"), self.sibling("sibling"));
        audit?;
        self.insert("after").await?;
        REPORT.with(|slot| *slot.borrow_mut() = Some(sibling));
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn fails_after_step_beside_query(&self) -> Result<SiblingReport, ServiceError> {
        self.insert("before").await?;
        let (step, sibling) = tokio::join!(self.slow_step("step"), self.sibling("sibling"));
        step?;
        self.insert("after").await?;
        REPORT.with(|slot| *slot.borrow_mut() = Some(sibling));
        Err(ServiceError::Rejected)
    }
}

thread_local! {
    static REPORT: std::cell::RefCell<Option<SiblingReport>> = const { std::cell::RefCell::new(None) };
}

fn take_report() -> SiblingReport {
    REPORT.with(|slot| slot.borrow_mut().take()).expect("the sibling ran")
}

fn assert_sibling_was_told(report: &SiblingReport, rows: &[String]) {
    assert!(report.insert.is_err() || !rows.contains(&"sibling".to_owned()), "sibling write survived: {rows:?}");
    assert_ne!(report.listener_registered, Some(false), "the listener was skipped without an error");
    assert!(!report.saw_status, "the sibling saw the status of another call");
}

async fn service() -> Service {
    let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
    let connections = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("drop table if exists {TABLE}"))).execute(&connections).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("create table {TABLE}(v text)"))).execute(&connections).await.unwrap();
    let pool = TransactionalPool::new(connections);
    let manager = Arc::new(manager(pool.clone()));
    registry().set_default(manager.clone());
    Service { pool, manager }
}

async fn reset(service: &Service) {
    sqlx::query(AssertSqlSafe(format!("truncate {TABLE}"))).execute(service.pool.inner()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn futures_beside_a_transactional_call_do_not_escape_the_transaction() {
    let service = service().await;

    let result = service.fails_after_audit_beside_query().await;
    assert!(matches!(result, Err(ServiceError::Rejected)), "{:?}", result.err());
    let rows = service.rows().await;
    assert!(!rows.contains(&"before".to_owned()) && !rows.contains(&"after".to_owned()), "{rows:?}");
    assert!(rows.contains(&"audit".to_owned()), "{rows:?}");
    assert_sibling_was_told(&take_report(), &rows);

    reset(&service).await;
    let result = service.fails_after_step_beside_query().await;
    assert!(matches!(result, Err(ServiceError::Rejected)), "{:?}", result.err());
    let rows = service.rows().await;
    assert_sibling_was_told(&take_report(), &rows);
    assert!(rows.is_empty() || rows == ["sibling"], "{rows:?}");
    assert!(rows.is_empty(), "{rows:?}");

    reset(&service).await;
    let (rolled_back, report) = synchronization::scope(async {
        let status = service.manager.transaction(None).await.unwrap();
        service.insert("before").await.unwrap();
        let (step, sibling) = tokio::join!(service.slow_step("step"), service.sibling("sibling"));
        step.unwrap();
        service.insert("after").await.unwrap();
        (service.manager.rollback(status.as_ref()).await, sibling)
    })
    .await;
    rolled_back.unwrap();
    let rows = service.rows().await;
    assert!(report.insert.is_err() || !rows.contains(&"sibling".to_owned()), "sibling write survived: {rows:?}");
    assert!(!rows.contains(&"before".to_owned()) && !rows.contains(&"after".to_owned()), "{rows:?}");
}
