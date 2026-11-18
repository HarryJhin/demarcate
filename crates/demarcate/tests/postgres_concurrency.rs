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

type Call<'a> = std::pin::Pin<Box<dyn Future<Output = Result<(), ServiceError>> + 'a>>;

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
    nested_fails: AtomicBool,
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
        if self.nested_fails.load(Ordering::SeqCst) { Err(ServiceError::Rejected) } else { Ok(()) }
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
    async fn outer(
        &self,
        nested: Nested,
        sibling: Sibling,
        phase: Phase,
        outer_fails: bool,
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
        if outer_fails { Err(ServiceError::Rejected) } else { Ok(()) }
    }

    #[transactional]
    async fn holds_its_status_in_a_listener(&self, started: &Notify) -> Result<(), ServiceError> {
        self.insert("held").await?;
        let status = demarcate::current_transaction_status()?;
        on_transaction(TransactionPhase::AfterCommit, false, move || {
            let status = status.clone();
            async move {
                let _ = status.is_completed();
                Ok(())
            }
        })
        .await
        .map_err(|_| ServiceError::Rejected)?;
        started.notify_one();
        std::future::pending().await
    }

    #[transactional(propagation = Nested)]
    async fn nested_step(&self, value: &str, work: Duration, fails: bool) -> Result<(), ServiceError> {
        self.insert(value).await?;
        tokio::time::sleep(work).await;
        if fails { Err(ServiceError::Rejected) } else { Ok(()) }
    }

    #[transactional]
    async fn outer_with_two_nested(
        &self,
        first: (Duration, bool),
        second_starts_after: Duration,
        second: (Duration, bool),
    ) -> Result<(bool, bool), ServiceError> {
        self.insert("outer").await?;
        let (first, second) = tokio::join!(self.nested_step("first", first.0, first.1), async {
            tokio::time::sleep(second_starts_after).await;
            self.nested_step("second", second.0, second.1).await
        });
        Ok((first.is_ok(), second.is_ok()))
    }

    #[transactional(propagation = Nested)]
    async fn nested_that_holds_its_status(&self) -> Result<(), ServiceError> {
        self.insert("held_nested").await?;
        let status = demarcate::current_transaction_status()?;
        on_transaction(TransactionPhase::AfterCompletion, false, move || {
            let status = status.clone();
            async move {
                let _ = status.is_completed();
                Ok(())
            }
        })
        .await
        .map_err(|_| ServiceError::Rejected)?;
        std::future::pending().await
    }

    #[transactional]
    async fn outer_that_cancels_a_nested_call_holding_its_status(&self) -> Result<bool, ServiceError> {
        self.insert("outer").await?;
        let cancelled =
            tokio::time::timeout(Duration::from_millis(30), self.nested_that_holds_its_status()).await.is_err();
        assert!(cancelled);
        Ok(self.insert("outer_after").await.is_ok())
    }

    #[transactional(propagation = Nested)]
    async fn nested_with_failing_call_beside_inner_nested(&self) -> Result<(), ServiceError> {
        let (inner, beside) = tokio::join!(self.nested_step("inner", Duration::from_millis(40), false), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.insert_then_fail_in_transaction().await
        });
        inner?;
        beside
    }

    #[transactional]
    async fn insert_then_fail_in_transaction(&self) -> Result<(), ServiceError> {
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn outer_that_survives_a_failed_nested_call(&self) -> Result<(), ServiceError> {
        self.insert("outer").await?;
        assert!(self.nested_with_failing_call_beside_inner_nested().await.is_err());
        self.insert("outer_after").await
    }

    #[transactional]
    async fn ignores_a_failed_statement(&self) -> Result<(), ServiceError> {
        self.insert("outer").await?;
        let failed = sqlx::query("select missing_column from demarcate_no_such_table").execute(&self.pool).await;
        assert!(failed.is_err());
        Ok(())
    }

    #[transactional]
    async fn recovers_with_a_savepoint_written_in_sql(&self) -> Result<(), ServiceError> {
        self.insert("outer").await?;
        sqlx::query("savepoint written_in_sql").execute(&self.pool).await?;
        assert!(sqlx::query("select missing_column from demarcate_no_such_table").execute(&self.pool).await.is_err());
        sqlx::query("rollback to savepoint written_in_sql").execute(&self.pool).await?;
        self.insert("outer_after").await
    }

    #[transactional]
    async fn ignores_a_failed_statement_then_runs_something_harmless(&self) -> Result<(), ServiceError> {
        use sqlx::Executor as _;
        self.insert("outer").await?;
        assert!(sqlx::query("select missing_column from demarcate_no_such_table").execute(&self.pool).await.is_err());
        let _ = sqlx::raw_sql("").execute(&self.pool).await;
        let _ = sqlx::raw_sql("-- nothing").execute(&self.pool).await;
        let _ = (&self.pool).prepare(sqlx::SqlStr::from_static("select 1")).await;
        Ok(())
    }

    #[transactional]
    async fn leaves_a_savepoint_open(&self) -> Result<(), ServiceError> {
        demarcate::current_transaction_status()?.create_savepoint().await?;
        self.insert("inner").await
    }

    #[transactional]
    async fn outer_after_a_call_that_left_a_savepoint_open(&self) -> Result<(), ServiceError> {
        self.insert("outer").await?;
        self.leaves_a_savepoint_open().await?;
        self.insert("outer_after").await?;
        self.nested_step("nested", Duration::ZERO, false).await
    }

    #[transactional(propagation = Nested)]
    async fn nested_that_drops_a_call_of_its_caller(&self, call: Call<'_>) -> Result<(), ServiceError> {
        drop(call);
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn outer_whose_call_is_dropped_inside_a_nested_call(&self) -> Result<(), ServiceError> {
        self.insert("outer").await?;
        let mut call: Call<'_> = Box::pin(self.insert_then_wait("dropped"));
        tokio::select! {
            biased;
            _ = &mut call => panic!("the call must still be running"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        assert!(self.nested_that_drops_a_call_of_its_caller(call).await.is_err());
        Ok(())
    }

    #[transactional]
    async fn insert_then_wait(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        std::future::pending().await
    }

    #[transactional(propagation = Supports)]
    async fn supports_with_query_then_two_queries(&self) -> Result<(), ServiceError> {
        self.insert("zero").await?;
        let (first, second) = tokio::join!(self.insert("first"), self.insert("second"));
        first?;
        second
    }

    #[transactional(propagation = Supports)]
    async fn supports_with_two_queries(&self) -> Result<(), ServiceError> {
        let (first, second) = tokio::join!(self.insert("first"), self.insert("second"));
        first?;
        second
    }

    #[transactional(propagation = Supports)]
    async fn supports_with_cancelled_required(&self) -> Result<(), ServiceError> {
        self.insert("supports").await?;
        {
            let cancelled = self.insert_in_transaction("cancelled");
            tokio::pin!(cancelled);
            tokio::select! {
                biased;
                _ = &mut cancelled => panic!("the call must be cancelled while it begins"),
                _ = tokio::task::yield_now() => {}
            }
        }
        self.insert_in_transaction("next").await
    }

    #[transactional]
    async fn outer_that_lets_a_call_escape<'a>(&'a self, escaped: &mut Option<Call<'a>>) -> Result<(), ServiceError> {
        self.insert("outer_before").await?;
        let mut call: Call<'a> = Box::pin(self.slow_then_insert("escaped"));
        tokio::select! {
            biased;
            _ = &mut call => panic!("the call must still be running"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        *escaped = Some(call);
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn slow_then_insert(&self, value: &str) -> Result<(), ServiceError> {
        tokio::time::sleep(Duration::from_millis(60)).await;
        self.insert(value).await
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
    Service { pool, manager, nested_fails: AtomicBool::new(false) }
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
    for (outer_fails, nested_fails) in [(true, false), (false, true), (false, false), (true, true)] {
        service.nested_fails.store(nested_fails, Ordering::SeqCst);
        for nested in [Nested::Required, Nested::RequiresNew, Nested::Savepoint, Nested::NotSupported, Nested::Template]
        {
            for sibling in [Sibling::Plain, Sibling::Strict, Sibling::Transactional, Sibling::Listener] {
                for phase in [Phase::Starting, Phase::Running, Phase::Completing] {
                    reset(&service).await;
                    let listener_ran = Arc::new(AtomicBool::new(false));
                    let report = Mutex::new(None);
                    let result = tokio::time::timeout(
                        Duration::from_secs(10),
                        service.outer(nested, sibling, phase, outer_fails, &listener_ran, &report),
                    )
                    .await;
                    let case = format!(
                        "{nested:?} beside {sibling:?} {phase:?} outer_fails={outer_fails} nested_fails={nested_fails}"
                    );
                    let Ok(result) = result else {
                        violations.push(format!("{case}: did not finish"));
                        continue;
                    };
                    let Some(report) = report.lock().unwrap().take() else {
                        violations.push(format!("{case}: the outer call did not reach its end"));
                        continue;
                    };
                    let rows = service.rows().await;
                    let committed = result.is_ok();
                    let has = |row: &str| rows.iter().any(|existing| existing == row);
                    if outer_fails && committed {
                        violations.push(format!("{case}: the outer call returned Ok"));
                    }
                    for row in ["outer_before", "outer_after"] {
                        if has(row) && !committed {
                            violations.push(format!("{case}: {row} survived the rollback, rows {rows:?}"));
                        }
                    }
                    if sibling != Sibling::Listener {
                        let sibling_done = matches!(report.sibling, Outcome::Done);
                        if has("sibling") && !committed {
                            violations.push(format!("{case}: the sibling row survived the rollback, {report:?}"));
                        }
                        if committed && sibling_done && !has("sibling") {
                            violations
                                .push(format!("{case}: the sibling write returned Ok and was lost, rows {rows:?}"));
                        }
                    }
                    let nested_expected = if nested.commits_on_its_own() {
                        match nested {
                            Nested::NotSupported => has("nested"),
                            _ => report.nested,
                        }
                    } else {
                        committed && report.nested
                    };
                    if has("nested") != nested_expected {
                        violations.push(format!(
                            "{case}: nested row present={}, {report:?}, outer Ok={committed}",
                            has("nested")
                        ));
                    }
                    if matches!(report.sibling, Outcome::Skipped) {
                        violations.push(format!("{case}: the listener was skipped without an error"));
                    }
                    if sibling == Sibling::Listener && matches!(report.sibling, Outcome::Done) {
                        if listener_ran.load(Ordering::SeqCst) != committed {
                            violations.push(format!("{case}: listener ran={} but outer Ok={committed}", !committed));
                        }
                    } else if listener_ran.load(Ordering::SeqCst) {
                        violations.push(format!("{case}: a listener ran that was not registered"));
                    }
                    if idle_in_transaction(&service).await != 0 {
                        violations.push(format!("{case}: a transaction was left open"));
                    }
                }
            }
        }
    }
    service.nested_fails.store(false, Ordering::SeqCst);
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

    reset(&service).await;
    let started = Notify::new();
    tokio::select! {
        biased;
        _ = service.holds_its_status_in_a_listener(&started) => panic!("the call never completes"),
        _ = started.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(idle_in_transaction(&service).await, 0, "a cancelled call that holds its status is rolled back");
    assert!(service.rows().await.is_empty());

    reset(&service).await;
    service.supports_with_two_queries().await.unwrap();
    assert_eq!(service.rows().await, ["first", "second"]);

    reset(&service).await;
    service.supports_with_cancelled_required().await.unwrap();
    assert_eq!(service.rows().await, ["next", "supports"]);

    reset(&service).await;
    let mut escaped = None;
    assert!(service.outer_that_lets_a_call_escape(&mut escaped).await.is_err());
    let escaped = escaped.expect("the call escaped").await;
    let rows = service.rows().await;
    assert!(
        escaped.is_err() && rows.is_empty(),
        "a call that outlives the transaction it joined must fail, got {escaped:?} and rows {rows:?}"
    );

    let ms = Duration::from_millis;
    for (first, second_starts_after, second) in [
        ((ms(80), true), ms(10), (ms(0), false)),
        ((ms(40), false), ms(10), (ms(80), false)),
        ((ms(40), true), ms(10), (ms(80), false)),
        ((ms(40), false), ms(10), (ms(80), true)),
        ((ms(80), false), ms(10), (ms(0), true)),
    ] {
        reset(&service).await;
        let result = service.outer_with_two_nested(first, second_starts_after, second).await;
        let rows = service.rows().await;
        let case = format!("{first:?} then after {second_starts_after:?} {second:?}: {result:?}, rows {rows:?}");
        match result {
            Ok((first_ok, second_ok)) => {
                assert!(
                    rows.contains(&"outer".to_owned()),
                    "the outer call returned Ok and nothing was committed: {case}"
                );
                assert_eq!(rows.contains(&"first".to_owned()), first_ok, "{case}");
                assert_eq!(rows.contains(&"second".to_owned()), second_ok, "{case}");
            }
            Err(_) => assert!(rows.is_empty(), "{case}"),
        }
        assert_eq!(idle_in_transaction(&service).await, 0, "{case}");
    }

    reset(&service).await;
    let result = service.outer_that_cancels_a_nested_call_holding_its_status().await;
    let rows = service.rows().await;
    assert!(!rows.contains(&"held_nested".to_owned()), "the cancelled nested call was committed: {result:?} {rows:?}");
    assert!(result.is_err() || rows == ["outer", "outer_after"], "{result:?} {rows:?}");
    assert_eq!(idle_in_transaction(&service).await, 0);

    reset(&service).await;
    service.outer_that_survives_a_failed_nested_call().await.unwrap();
    assert_eq!(service.rows().await, ["outer", "outer_after"]);

    reset(&service).await;
    let result = service.ignores_a_failed_statement().await;
    assert!(result.is_err(), "a transaction that a failed statement aborted cannot report a commit");
    assert!(service.rows().await.is_empty());

    reset(&service).await;
    service.supports_with_query_then_two_queries().await.unwrap();
    assert_eq!(service.rows().await, ["first", "second", "zero"]);

    reset(&service).await;
    service.recovers_with_a_savepoint_written_in_sql().await.unwrap();
    assert_eq!(service.rows().await, ["outer", "outer_after"]);

    reset(&service).await;
    service.outer_after_a_call_that_left_a_savepoint_open().await.unwrap();
    assert_eq!(service.rows().await, ["inner", "nested", "outer", "outer_after"]);

    reset(&service).await;
    let result = service.outer_whose_call_is_dropped_inside_a_nested_call().await;
    let rows = service.rows().await;
    assert!(!rows.contains(&"dropped".to_owned()), "the dropped call was committed: {result:?} {rows:?}");
    assert_eq!(idle_in_transaction(&service).await, 0);

    reset(&service).await;
    sqlx::query("select 1").execute(&service.pool).await.unwrap();
    let result = service.ignores_a_failed_statement_then_runs_something_harmless().await;
    assert!(result.is_err(), "an empty statement or a prepare does not show that the transaction recovered");
    assert!(service.rows().await.is_empty());
}
