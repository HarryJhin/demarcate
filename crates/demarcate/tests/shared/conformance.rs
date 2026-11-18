// The behaviour that every database driver must show, written once. A file per database
// defines `mod db` and the `db_test!` macro and includes this file.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use demarcate::{
    Isolation, Propagation, TransactionDefinition, TransactionError, TransactionPhase, TransactionTemplate,
    current_transaction_status, on_transaction, registry, transactional,
};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, Notify, OnceCell};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Postgres,
    MySql,
    Sqlite,
}

const TABLE: &str = "demarcate_conformance";

#[derive(Debug)]
enum ServiceError {
    Transaction(TransactionError),
    Sql(db::Error),
    Rejected,
}

impl From<TransactionError> for ServiceError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<db::Error> for ServiceError {
    fn from(error: db::Error) -> Self {
        Self::Sql(error)
    }
}

impl ServiceError {
    fn infrastructure(&self) -> Option<&TransactionError> {
        match self {
            Self::Transaction(error) => Some(error),
            Self::Sql(error) => db::transaction_error(error),
            _ => None,
        }
    }
}

struct Service {
    raw: db::Raw,
    pool: db::TransactionalPool,
    manager: Arc<dyn demarcate::PlatformTransactionManager>,
}

impl Service {
    async fn insert(&self, value: &str) -> Result<(), ServiceError> {
        Ok(db::insert(&self.pool, TABLE, value).await?)
    }

    async fn visible(&self) -> Result<Vec<String>, ServiceError> {
        Ok(db::values(&self.pool, TABLE).await?)
    }

    async fn committed(&self) -> Vec<String> {
        db::raw_values(&self.raw, &format!("select v from {TABLE} order by v")).await
    }

    #[transactional]
    async fn register(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional]
    async fn register_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn register_two_and_fail(&self, first: &str, second: &str) -> Result<(), ServiceError> {
        self.register(first).await?;
        self.register_then_fail(second).await
    }

    #[transactional]
    async fn swallow_joined_failure(&self, first: &str, second: &str) -> Result<(), ServiceError> {
        self.insert(first).await?;
        let _ = self.register_then_fail(second).await;
        Ok(())
    }

    #[transactional(propagation = RequiresNew)]
    async fn audit(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional]
    async fn audit_then_write_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.audit("audit").await?;
        self.insert(value).await?;
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn write_then_audit_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        self.audit("audit").await?;
        Err(ServiceError::Rejected)
    }

    #[transactional(propagation = Nested)]
    async fn nested(&self, value: &str, fail: bool) -> Result<(), ServiceError> {
        self.insert(value).await?;
        if fail { Err(ServiceError::Rejected) } else { Ok(()) }
    }

    #[transactional]
    async fn around_nested(&self, fail_nested: bool) -> Result<(), ServiceError> {
        self.insert("before").await?;
        let nested = self.nested("nested", fail_nested).await;
        assert_eq!(nested.is_err(), fail_nested, "{nested:?}");
        self.insert("z-after").await
    }

    #[transactional(propagation = Mandatory)]
    async fn mandatory(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(propagation = Never)]
    async fn never(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(propagation = NotSupported)]
    async fn outside(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(propagation = Supports)]
    async fn supports(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional]
    async fn outside_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        self.outside("outside").await?;
        self.insert(value).await?;
        Err(ServiceError::Rejected)
    }

    #[transactional]
    async fn never_inside(&self) -> Result<(), ServiceError> {
        self.never("never").await
    }

    #[transactional]
    async fn mark_rollback_only(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        current_transaction_status()?.set_rollback_only()?;
        Ok(())
    }

    #[transactional(read_only)]
    async fn write_in_read_only(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(isolation = Serializable)]
    async fn serializable(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(isolation = ReadCommitted)]
    async fn read_committed(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await
    }

    #[transactional(timeout = 1)]
    async fn outlive_the_timeout(&self) -> Result<(), ServiceError> {
        self.insert("before-deadline").await?;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        self.insert("after-deadline").await
    }

    #[transactional]
    async fn write_and_read(&self, values: &[&str]) -> Result<(Vec<String>, Vec<String>, Vec<String>), ServiceError> {
        for value in values {
            self.insert(value).await?;
        }
        let own = self.visible().await?;
        let committed = self.committed().await;
        let streamed = db::stream_values(&self.pool, TABLE).await?;
        Ok((own, committed, streamed))
    }

    #[transactional]
    async fn register_and_notify(&self, value: &str, fail: bool, notified: Arc<AtomicBool>) -> Result<(), ServiceError> {
        self.insert(value).await?;
        on_transaction(TransactionPhase::AfterCommit, false, move || {
            let notified = notified.clone();
            async move {
                notified.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .map_err(TransactionError::Other)?;
        if fail { Err(ServiceError::Rejected) } else { Ok(()) }
    }

    #[transactional]
    async fn register_and_wait(&self, value: &str, gate: Arc<Notify>) -> Result<(), ServiceError> {
        self.insert(value).await?;
        gate.notified().await;
        Ok(())
    }

    #[transactional]
    async fn ignore_a_failed_statement(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        assert!(self.insert(value).await.is_err(), "the second insert violates the primary key");
        Ok(())
    }

    #[transactional]
    async fn recover_in_nested(&self, value: &str) -> Result<(), ServiceError> {
        self.insert(value).await?;
        assert!(self.nested(value, false).await.is_err(), "the nested insert violates the primary key");
        self.insert("z-after").await
    }
}

struct Env {
    service: Arc<Service>,
    serial: Mutex<()>,
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("runtime"));
static ENV: OnceCell<Env> = OnceCell::const_new();

async fn env() -> &'static Env {
    ENV.get_or_init(|| async {
        let raw = db::connect(4).await;
        db::raw_execute(&raw, &format!("drop table if exists {TABLE}")).await;
        db::raw_execute(&raw, &format!("create table {TABLE} (v varchar(100) primary key)")).await;
        let pool = db::transactional(&raw);
        let manager: Arc<dyn demarcate::PlatformTransactionManager> = Arc::new(db::manager(pool.clone()));
        registry().set_default(manager.clone());
        Env { service: Arc::new(Service { raw, pool, manager }), serial: Mutex::new(()) }
    })
    .await
}

fn run_test(body: impl FnOnce(&'static Service) -> Pin<Box<dyn Future<Output = ()>>>) {
    RUNTIME.block_on(async {
        let env = env().await;
        let _serial = env.serial.lock().await;
        db::raw_execute(&env.service.raw, &format!("delete from {TABLE}")).await;
        body(&env.service).await;
    });
}

db_test!(a_transaction_commits_on_ok_and_rolls_back_on_err, |service| {
    service.register("a").await.unwrap();
    assert!(matches!(service.register_then_fail("b").await, Err(ServiceError::Rejected)));
    assert_eq!(service.committed().await, ["a"]);
});

db_test!(a_failure_of_a_joined_call_rolls_back_the_whole_transaction, |service| {
    assert!(matches!(service.register_two_and_fail("a", "b").await, Err(ServiceError::Rejected)));
    assert!(service.committed().await.is_empty());
});

db_test!(a_caller_that_swallows_the_failure_of_a_joined_call_cannot_commit, |service| {
    match service.swallow_joined_failure("a", "b").await {
        Err(ServiceError::Transaction(TransactionError::UnexpectedRollback(_))) => {}
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(service.committed().await.is_empty());
});

db_test!(requires_new_commits_although_the_caller_rolls_back, |service| {
    assert!(matches!(service.audit_then_write_then_fail("a").await, Err(ServiceError::Rejected)));
    assert_eq!(service.committed().await, ["audit"]);
});

db_test!(requires_new_after_a_write_of_the_caller, |service| {
    let result = service.write_then_audit_then_fail("a").await;
    match db::KIND {
        Kind::Sqlite => {
            match &result {
                Err(ServiceError::Sql(error)) => assert!(error.to_string().contains("locked"), "{error}"),
                other => panic!("SQLite allows one writer, so the inner write must fail: {other:?}"),
            }
            assert!(service.committed().await.is_empty());
        }
        Kind::Postgres | Kind::MySql => {
            assert!(matches!(result, Err(ServiceError::Rejected)), "{result:?}");
            assert_eq!(service.committed().await, ["audit"]);
        }
    }
});

db_test!(a_failed_nested_transaction_undoes_only_its_own_work, |service| {
    service.around_nested(true).await.unwrap();
    assert_eq!(service.committed().await, ["before", "z-after"]);
});

db_test!(a_nested_transaction_commits_with_its_caller, |service| {
    service.around_nested(false).await.unwrap();
    assert_eq!(service.committed().await, ["before", "nested", "z-after"]);
});

db_test!(mandatory_needs_a_transaction_of_the_caller, |service| {
    match service.mandatory("a").await {
        Err(ServiceError::Transaction(TransactionError::InvalidTransactionState(message))) => {
            assert!(message.contains("mandatory"), "{message}");
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(service.committed().await.is_empty());
});

db_test!(never_refuses_a_transaction_of_the_caller, |service| {
    service.never("alone").await.unwrap();
    match service.never_inside().await {
        Err(ServiceError::Transaction(TransactionError::InvalidTransactionState(_))) => {}
        other => panic!("unexpected result: {other:?}"),
    }
    assert_eq!(service.committed().await, ["alone"]);
});

db_test!(not_supported_runs_outside_the_transaction_of_the_caller, |service| {
    assert!(matches!(service.outside_then_fail("a").await, Err(ServiceError::Rejected)));
    assert_eq!(service.committed().await, ["outside"]);
});

db_test!(supports_runs_without_a_transaction_when_there_is_none, |service| {
    service.supports("a").await.unwrap();
    assert_eq!(service.committed().await, ["a"]);
});

db_test!(a_transaction_marked_rollback_only_is_rolled_back, |service| {
    service.mark_rollback_only("a").await.unwrap();
    assert!(service.committed().await.is_empty());
});

db_test!(a_write_in_a_read_only_transaction, |service| {
    let result = service.write_in_read_only("a").await;
    match db::KIND {
        Kind::Sqlite => {
            result.unwrap();
            assert_eq!(service.committed().await, ["a"], "SQLite does not enforce the read-only flag");
        }
        Kind::Postgres | Kind::MySql => {
            assert!(matches!(&result, Err(ServiceError::Sql(error)) if db::is_database_error(error)), "{result:?}");
            assert!(service.committed().await.is_empty());
        }
    }
    service.register("after").await.unwrap();
    assert!(service.committed().await.contains(&"after".to_owned()), "the next transaction can write");
});

db_test!(an_isolation_level_applies_to_the_transaction, |service| {
    service.serializable("a").await.unwrap();
    let result = service.read_committed("b").await;
    match db::KIND {
        Kind::Sqlite => {
            assert!(
                matches!(result, Err(ServiceError::Transaction(TransactionError::InvalidIsolationLevel(_)))),
                "{result:?}"
            );
            assert_eq!(service.committed().await, ["a"]);
        }
        Kind::Postgres | Kind::MySql => {
            result.unwrap();
            assert_eq!(service.committed().await, ["a", "b"]);
        }
    }
});

db_test!(a_query_after_the_deadline_fails_and_the_transaction_rolls_back, |service| {
    let result = service.outlive_the_timeout().await;
    let error = result.expect_err("the query after the deadline must fail");
    assert!(matches!(error.infrastructure(), Some(TransactionError::TimedOut(_))), "{error:?}");
    assert!(service.committed().await.is_empty());
});

db_test!(a_pool_that_requires_a_transaction_refuses_to_run_without_one, |service| {
    let strict = service.pool.transaction_required();
    let error = db::insert(&strict, TABLE, "a").await.expect_err("the pool must refuse to run");
    assert!(matches!(db::transaction_error(&error), Some(TransactionError::NoTransaction(_))), "{error:?}");
    assert!(service.committed().await.is_empty());
});

db_test!(a_transaction_reads_its_own_writes_and_others_do_not, |service| {
    let (own, committed, streamed) = service.write_and_read(&["x", "y"]).await.unwrap();
    assert_eq!(own, ["x", "y"]);
    assert_eq!(streamed, ["x", "y"]);
    assert!(committed.is_empty(), "another connection must not see the rows before the commit: {committed:?}");
    assert_eq!(service.committed().await, ["x", "y"]);
});

db_test!(an_after_commit_listener_runs_only_when_the_transaction_commits, |service| {
    let committed = Arc::new(AtomicBool::new(false));
    service.register_and_notify("a", false, committed.clone()).await.unwrap();
    assert!(committed.load(Ordering::SeqCst));

    let rolled_back = Arc::new(AtomicBool::new(false));
    assert!(service.register_and_notify("b", true, rolled_back.clone()).await.is_err());
    assert!(!rolled_back.load(Ordering::SeqCst));
    assert_eq!(service.committed().await, ["a"]);
});

db_test!(a_cancelled_call_is_rolled_back_and_releases_its_connection, |service| {
    for round in 0..8 {
        let gate = Arc::new(Notify::new());
        let cancelled =
            tokio::time::timeout(Duration::from_millis(100), service.register_and_wait(&format!("c{round}"), gate)).await;
        assert!(cancelled.is_err(), "the call must still be waiting when it is dropped");
    }
    tokio::time::timeout(Duration::from_secs(20), service.register("after")).await.expect("a connection is free").unwrap();
    assert_eq!(service.committed().await, ["after"]);
});

db_test!(transactions_of_calls_beside_each_other_are_independent, |service| {
    let (first, second) = tokio::join!(service.register("a"), service.register_then_fail("b"));
    first.unwrap();
    assert!(matches!(second, Err(ServiceError::Rejected)));
    assert_eq!(service.committed().await, ["a"]);
});

db_test!(a_statement_that_failed_and_was_ignored, |service| {
    let result = service.ignore_a_failed_statement("a").await;
    match db::KIND {
        Kind::Postgres => {
            assert!(
                matches!(result, Err(ServiceError::Transaction(TransactionError::UnexpectedRollback(_)))),
                "PostgreSQL aborts the transaction: {result:?}"
            );
            assert!(service.committed().await.is_empty());
        }
        Kind::MySql | Kind::Sqlite => {
            result.unwrap();
            assert_eq!(service.committed().await, ["a"], "the failed statement alone is undone");
        }
    }
});

db_test!(a_statement_that_failed_in_a_nested_transaction_leaves_the_caller_usable, |service| {
    service.recover_in_nested("a").await.unwrap();
    assert_eq!(service.committed().await, ["a", "z-after"]);
});

db_test!(a_template_runs_the_same_transactions_in_code, |service| {
    let template = TransactionTemplate::with_definition(
        service.manager.clone(),
        TransactionDefinition::new().propagation(Propagation::Required).isolation(Isolation::Default),
    );
    template.execute(|_status| async move { service.insert("a").await }).await.unwrap();
    let failed = template
        .execute(|_status| async move {
            service.insert("b").await?;
            Err::<(), ServiceError>(ServiceError::Rejected)
        })
        .await;
    assert!(failed.is_err());
    assert_eq!(service.committed().await, ["a"]);
});

db_test!(the_settings_of_a_transaction_do_not_stay_on_its_connection, |service| {
    let raw = db::connect(1).await;
    db::raw_execute(&raw, &format!("create table if not exists {TABLE}_single (v varchar(100) primary key)")).await;
    db::raw_execute(&raw, &format!("delete from {TABLE}_single")).await;
    let pool = db::transactional(&raw);
    let manager: Arc<dyn demarcate::PlatformTransactionManager> = Arc::new(db::manager(pool.clone()));
    let insert = |value: &'static str| {
        let pool = pool.clone();
        async move { db::insert(&pool, &format!("{TABLE}_single"), value).await.map_err(ServiceError::Sql) }
    };

    let restricted = TransactionTemplate::with_definition(
        manager.clone(),
        TransactionDefinition::new().isolation(Isolation::Serializable).read_only(true).timeout(Duration::from_secs(30)),
    );
    let read_only = restricted.execute(|_status| insert("in-read-only")).await;
    assert_eq!(read_only.is_ok(), db::KIND == Kind::Sqlite, "{read_only:?}");

    let plain = TransactionTemplate::new(manager);
    plain.execute(|_status| insert("in-the-next-transaction")).await.unwrap();
    insert("without-a-transaction").await.unwrap();
    let _ = service;
});
