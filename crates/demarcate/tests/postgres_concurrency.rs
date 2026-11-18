use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use demarcate::postgres::{PgTransactionManager, TransactionalPool, manager};
use demarcate::{
    Propagation, TransactionDefinition, TransactionError, TransactionPhase, TransactionTemplate, on_transaction,
    registry, synchronization, transactional,
};
use sqlx::{AssertSqlSafe, Row};
use tokio::sync::Notify;

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

#[derive(Clone, Copy, Debug, PartialEq)]
enum Nested {
    Required,
    RequiresNew,
    Savepoint,
    NotSupported,
    Template,
}

impl Nested {
    fn commits_on_its_own(self) -> bool {
        matches!(self, Self::RequiresNew | Self::NotSupported | Self::Template)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Sibling {
    Plain,
    Strict,
    Transactional,
    Listener,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Starting,
    Running,
    Completing,
}

#[derive(Debug)]
enum Outcome {
    Done,
    Skipped,
    Failed,
}

#[derive(Debug)]
struct Report {
    nested: bool,
    sibling: Outcome,
}

struct Service {
    pool: TransactionalPool,
    manager: Arc<PgTransactionManager>,
}

impl Service {
    async fn insert(&self, value: &str) -> Result<(), ServiceError> {
        sqlx::query(AssertSqlSafe(format!("insert into {TABLE} values($1)")))
            .bind(value.to_owned())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn insert_strict(&self, value: &str) -> Result<(), ServiceError> {
        sqlx::query(AssertSqlSafe(format!("insert into {TABLE} values($1)")))
            .bind(value.to_owned())
            .execute(&self.pool.transaction_required())
            .await?;
        Ok(())
    }

    async fn session(&self) -> Result<(i32, String), ServiceError> {
        let row = sqlx::query("select pg_backend_pid(), txid_current()::text").fetch_one(&self.pool).await?;
        Ok((row.get(0), row.get(1)))
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

    async fn nested_body(&self, completing: &Notify) -> Result<(), ServiceError> {
        self.insert("nested").await?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        completing.notify_one();
        Ok(())
    }

    #[transactional]
    async fn nested_required(&self, completing: &Notify) -> Result<(), ServiceError> {
        self.nested_body(completing).await
    }

    #[transactional(propagation = RequiresNew)]
    async fn nested_requires_new(&self, completing: &Notify) -> Result<(), ServiceError> {
        self.nested_body(completing).await
    }

    #[transactional(propagation = Nested)]
    async fn nested_savepoint(&self, completing: &Notify) -> Result<(), ServiceError> {
        self.nested_body(completing).await
    }

    #[transactional(propagation = NotSupported)]
    async fn nested_not_supported(&self, completing: &Notify) -> Result<(), ServiceError> {
        self.nested_body(completing).await
    }

    async fn run_nested(&self, nested: Nested, completing: &Notify) -> Result<(), ServiceError> {
        let result = match nested {
            Nested::Required => self.nested_required(completing).await,
            Nested::RequiresNew => self.nested_requires_new(completing).await,
            Nested::Savepoint => self.nested_savepoint(completing).await,
            Nested::NotSupported => self.nested_not_supported(completing).await,
            Nested::Template => {
                let mut definition = TransactionDefinition::new();
                definition.propagation = Propagation::RequiresNew;
                TransactionTemplate::with_definition(self.manager.clone(), definition)
                    .execute(|_| self.nested_body(completing))
                    .await
                    .map_err(|_| ServiceError::Rejected)
            }
        };
        completing.notify_one();
        result
    }

    #[transactional]
    async fn insert_in_transaction(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    async fn run_sibling(
        &self,
        sibling: Sibling,
        phase: Phase,
        completing: &Notify,
        listener_ran: &Arc<AtomicBool>,
    ) -> Outcome {
        match phase {
            Phase::Starting => {}
            Phase::Running => tokio::time::sleep(Duration::from_millis(15)).await,
            Phase::Completing => completing.notified().await,
        }
        let done = match sibling {
            Sibling::Plain => self.insert("sibling").await.is_ok(),
            Sibling::Strict => self.insert_strict("sibling").await.is_ok(),
            Sibling::Transactional => self.insert_in_transaction("sibling").await.is_ok(),
            Sibling::Listener => {
                let ran = listener_ran.clone();
                let registered = on_transaction(TransactionPhase::AfterCommit, false, move || {
                    let ran = ran.clone();
                    async move {
                        ran.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await;
                match registered {
                    Ok(true) => true,
                    Ok(false) => return Outcome::Skipped,
                    Err(_) => false,
                }
            }
        };
        if done { Outcome::Done } else { Outcome::Failed }
    }

    #[transactional]
    async fn outer_that_rolls_back(
        &self,
        nested: Nested,
        sibling: Sibling,
        phase: Phase,
        listener_ran: &Arc<AtomicBool>,
        report: &Mutex<Option<Report>>,
    ) -> Result<(), ServiceError> {
        self.insert("outer_before").await?;
        let completing = Notify::new();
        let (nested, sibling) = tokio::join!(
            self.run_nested(nested, &completing),
            self.run_sibling(sibling, phase, &completing, listener_ran)
        );
        let _ = self.insert("outer_after").await;
        *report.lock().unwrap() = Some(Report { nested: nested.is_ok(), sibling });
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn outer_with_sibling_session(&self) -> Result<[(i32, String); 3], ServiceError> {
        let before = self.session().await?;
        let completing = Notify::new();
        let (nested, beside) = tokio::join!(self.nested_requires_new(&completing), async {
            tokio::time::sleep(Duration::from_millis(15)).await;
            self.insert("sibling").await?;
            self.session().await
        });
        nested?;
        Ok([before, beside?, self.session().await?])
    }

    #[transactional]
    async fn session_in_transaction(&self) -> Result<(i32, String), ServiceError> {
        let session = self.session().await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(session)
    }

    #[transactional]
    async fn outer_that_cancels_nested(&self, phase: Phase) -> Result<[(i32, String); 2], ServiceError> {
        let before = self.session().await?;
        self.insert("outer_before").await?;
        let completing = Notify::new();
        {
            let nested = self.nested_requires_new(&completing);
            tokio::pin!(nested);
            let cancel = async {
                match phase {
                    Phase::Starting => tokio::task::yield_now().await,
                    Phase::Running => tokio::time::sleep(Duration::from_millis(15)).await,
                    Phase::Completing => completing.notified().await,
                }
            };
            tokio::select! {
                biased;
                _ = &mut nested => panic!("the nested call must be cancelled before it completes"),
                _ = cancel => {}
            }
        }
        self.insert("outer_after").await?;
        Ok([before, self.session().await?])
    }

    #[transactional(propagation = Supports)]
    async fn supports_with_requires_new_beside_query(&self) -> Result<(), ServiceError> {
        self.insert("supports_before").await?;
        let completing = Notify::new();
        let (nested, beside) = tokio::join!(self.nested_requires_new(&completing), async {
            tokio::time::sleep(Duration::from_millis(15)).await;
            self.insert("sibling").await
        });
        nested?;
        beside?;
        self.insert("supports_after").await
    }
}

async fn service() -> Service {
    let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
    let connections = sqlx::postgres::PgPoolOptions::new().max_connections(6).connect(&url).await.unwrap();
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

async fn idle_in_transaction(service: &Service) -> i64 {
    sqlx::query(
        "select count(*) from pg_stat_activity where state like 'idle in transaction%' and datname = current_database()",
    )
    .fetch_one(service.pool.inner())
    .await
    .unwrap()
    .get(0)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transactions_hold_when_futures_run_beside_transactional_calls() {
    let service = service().await;

    let mut violations = Vec::new();
    for nested in [Nested::Required, Nested::RequiresNew, Nested::Savepoint, Nested::NotSupported, Nested::Template] {
        for sibling in [Sibling::Plain, Sibling::Strict, Sibling::Transactional, Sibling::Listener] {
            for phase in [Phase::Starting, Phase::Running, Phase::Completing] {
                reset(&service).await;
                let listener_ran = Arc::new(AtomicBool::new(false));
                let report = Mutex::new(None);
                let result = tokio::time::timeout(
                    Duration::from_secs(10),
                    service.outer_that_rolls_back(nested, sibling, phase, &listener_ran, &report),
                )
                .await;
                let case = format!("{nested:?} beside {sibling:?} {phase:?}");
                let Ok(result) = result else {
                    violations.push(format!("{case}: did not finish"));
                    continue;
                };
                let report = report.lock().unwrap().take();
                let rows = service.rows().await;
                if result.is_ok() {
                    violations.push(format!("{case}: the outer call returned Ok"));
                }
                let nested_committed = nested.commits_on_its_own() && report.as_ref().is_some_and(|r| r.nested);
                let survivors: Vec<_> = rows.iter().filter(|row| !(nested_committed && *row == "nested")).collect();
                if !survivors.is_empty() {
                    violations.push(format!("{case}: rows survived the rollback: {survivors:?}, report {report:?}"));
                }
                if matches!(report, Some(Report { sibling: Outcome::Skipped, .. })) {
                    violations.push(format!("{case}: the listener was skipped without an error"));
                }
                if listener_ran.load(Ordering::SeqCst) {
                    violations.push(format!("{case}: an AfterCommit listener of the rolled back call ran"));
                }
                if idle_in_transaction(&service).await != 0 {
                    violations.push(format!("{case}: a transaction was left open"));
                }
            }
        }
    }
    assert!(violations.is_empty(), "{} violations:\n{}", violations.len(), violations.join("\n"));

    reset(&service).await;
    let [before, beside, after] = service.outer_with_sibling_session().await.unwrap();
    assert_eq!(beside, before, "the sibling runs on the connection and transaction of its caller");
    assert_eq!(after, before);
    assert_eq!(service.rows().await, ["nested", "sibling"]);

    reset(&service).await;
    let (first, second) = tokio::join!(service.session_in_transaction(), service.session_in_transaction());
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_ne!(first.1, second.1, "two top-level calls are two transactions");
    assert_ne!(first.0, second.0);

    let (first, second) = synchronization::scope(async {
        tokio::join!(service.session_in_transaction(), service.session_in_transaction())
    })
    .await;
    assert_ne!(first.unwrap().1, second.unwrap().1, "also inside an explicit scope");

    for phase in [Phase::Starting, Phase::Running, Phase::Completing] {
        reset(&service).await;
        let [before, after] = service.outer_that_cancels_nested(phase).await.unwrap_or_else(|error| {
            panic!("{phase:?}: {error:?}");
        });
        assert_eq!(after, before, "{phase:?}: the caller keeps its connection and transaction");
        let rows = service.rows().await;
        assert!(rows.contains(&"outer_before".to_owned()) && rows.contains(&"outer_after".to_owned()), "{rows:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(idle_in_transaction(&service).await, 0, "{phase:?}");
    }

    reset(&service).await;
    service.supports_with_requires_new_beside_query().await.unwrap();
    assert_eq!(service.rows().await, ["nested", "sibling", "supports_after", "supports_before"]);
}
