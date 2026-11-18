use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use demarcate_core::{
    BoxError, BoxFuture, CompletionStatus, Isolation, ManagerSettings, PlatformTransactionManager, Propagation,
    TemplateError, TransactionDefinition, TransactionError, TransactionSynchronization, TransactionTemplate,
    synchronization as tsm,
};
use demarcate_sqlx::postgres::{TransactionalPool, manager_with_settings};
use sqlx::{AssertSqlSafe, Row};

type AppResult<T> = Result<T, TransactionError>;

struct Fixture {
    pool: TransactionalPool,
    table: String,
}

impl Fixture {
    async fn new(name: &str) -> Option<Self> {
        let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
        let connections = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
        let table = format!("demarcate_{name}");
        sqlx::query(AssertSqlSafe(format!("drop table if exists {table}"))).execute(&connections).await.unwrap();
        sqlx::query(AssertSqlSafe(format!("create table {table}(v text)"))).execute(&connections).await.unwrap();
        Some(Self { pool: TransactionalPool::new(connections), table })
    }

    fn manager(&self, configure: impl FnOnce(&mut ManagerSettings)) -> Arc<dyn PlatformTransactionManager> {
        let mut settings = ManagerSettings { nested_transaction_allowed: true, ..ManagerSettings::default() };
        configure(&mut settings);
        Arc::new(manager_with_settings(self.pool.clone(), settings))
    }

    fn template(&self, propagation: Propagation) -> TransactionTemplate {
        self.template_on(self.manager(|_| {}), propagation)
    }

    fn template_on(&self, manager: Arc<dyn PlatformTransactionManager>, propagation: Propagation) -> TransactionTemplate {
        TransactionTemplate::with_definition(manager, TransactionDefinition::with_propagation(propagation))
    }

    async fn insert(&self, value: &str) -> AppResult<()> {
        let mut connection = self.pool.acquire().await?;
        sqlx::query(AssertSqlSafe(format!("insert into {} values($1)", self.table)))
            .bind(value.to_owned())
            .execute(&mut *connection)
            .await
            .map_err(|error| TransactionError::Other(error.into()))?;
        Ok(())
    }

    async fn show(&self, setting: &'static str) -> AppResult<String> {
        let mut connection = self.pool.acquire().await?;
        let row = sqlx::query(AssertSqlSafe(format!("show {setting}")))
            .fetch_one(&mut *connection)
            .await
            .map_err(|error| TransactionError::Other(error.into()))?;
        Ok(row.get(0))
    }

    async fn backend_pid(&self) -> AppResult<i32> {
        let mut connection = self.pool.acquire().await?;
        let row = sqlx::query("select pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .map_err(|error| TransactionError::Other(error.into()))?;
        Ok(row.get(0))
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

    fn bound(&self) -> bool {
        tsm::has_resource(self.pool.resource_key())
    }
}

async fn scoped<F: Future>(future: F) -> F::Output {
    tsm::scope(async {
        let output = future.await;
        assert!(tsm::resource_keys().is_empty(), "resources must be unbound after the test");
        assert!(!tsm::is_synchronization_active(), "synchronization must be inactive after the test");
        assert!(!tsm::is_current_transaction_read_only());
        assert!(!tsm::is_actual_transaction_active());
        output
    })
    .await
}

#[derive(Default)]
struct RecordingSynchronization {
    calls: Mutex<Vec<String>>,
}

impl RecordingSynchronization {
    fn record(&self, call: impl Into<String>) {
        self.calls.lock().unwrap().push(call.into());
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl TransactionSynchronization for RecordingSynchronization {
    fn before_commit(&self, read_only: bool) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            self.record(format!("before_commit({read_only})"));
            Ok(())
        })
    }
    fn before_completion(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            self.record("before_completion");
            Ok(())
        })
    }
    fn after_commit(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            self.record("after_commit");
            Ok(())
        })
    }
    fn after_completion(&self, status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            self.record(format!("after_completion({status:?})"));
            Ok(())
        })
    }
    fn savepoint<'a>(&'a self, _savepoint: &'a demarcate_core::Savepoint) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.record("savepoint");
            Ok(())
        })
    }
    fn savepoint_rollback<'a>(&'a self, _savepoint: &'a demarcate_core::Savepoint) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.record("savepoint_rollback");
            Ok(())
        })
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transaction_commit() {
    let Some(f) = Fixture::new("commit").await else { return };
    let f = &f;
    scoped(async {
        let synchronization = Arc::new(RecordingSynchronization::default());
        let synchronization = &synchronization;
        assert!(!f.bound());
        f.template(Propagation::Required)
            .execute(|status| async move {
                assert!(f.bound());
                assert!(tsm::is_synchronization_active());
                assert!(status.is_new_transaction());
                assert!(tsm::is_actual_transaction_active());
                tsm::register_synchronization(synchronization.clone())?;
                f.insert("a").await
            })
            .await
            .unwrap();
        assert!(!f.bound());
        assert_eq!(f.rows().await, ["a"]);
        assert_eq!(
            synchronization.calls(),
            ["before_commit(false)", "before_completion", "after_commit", "after_completion(Committed)"]
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transaction_rollback_on_error() {
    let Some(f) = Fixture::new("rollback").await else { return };
    let f = &f;
    scoped(async {
        let synchronization = Arc::new(RecordingSynchronization::default());
        let synchronization = &synchronization;
        let result: Result<(), _> = f
            .template(Propagation::Required)
            .execute(|_status| async move {
                tsm::register_synchronization(synchronization.clone())?;
                f.insert("a").await?;
                Err(TransactionError::Other("Application exception".into()))
            })
            .await;
        assert!(matches!(result, Err(TemplateError::Application(TransactionError::Other(_)))));
        assert!(f.rows().await.is_empty());
        assert_eq!(synchronization.calls(), ["before_completion", "after_completion(RolledBack)"]);
    })
    .await;
}

async fn assert_participating_transaction_with_rollback_only(name: &str, fail_early: bool) {
    let Some(f) = Fixture::new(name).await else { return };
    let f = &f;
    scoped(async {
        let manager = f.manager(|settings| settings.fail_early_on_global_rollback_only = fail_early);
        let manager = &manager;
        assert!(!f.bound());
        assert!(!tsm::is_synchronization_active());

        let outer = manager.transaction(None).await.unwrap();
        let synchronization = Arc::new(RecordingSynchronization::default());
        tsm::register_synchronization(synchronization.clone()).unwrap();
        assert!(outer.is_new_transaction());
        f.insert("a").await.unwrap();

        let template = f.template_on(manager.clone(), Propagation::Required);
        let template = &template;
        let participation = template
            .execute(|status1| async move {
                assert!(!status1.is_new_transaction());
                assert!(!status1.is_rollback_only());
                template
                    .execute(|status2| async move {
                        assert!(f.bound());
                        assert!(tsm::is_synchronization_active());
                        assert!(!status2.is_new_transaction());
                        status2.set_rollback_only()
                    })
                    .await
                    .map_err(|error| TransactionError::Other(error.to_string().into()))?;
                assert!(!status1.is_new_transaction());
                assert!(status1.is_rollback_only());
                Ok::<_, TransactionError>(())
            })
            .await;

        if fail_early {
            assert!(matches!(
                participation,
                Err(TemplateError::Transaction(TransactionError::UnexpectedRollback(_)))
            ));
            manager.rollback(outer.as_ref()).await.unwrap();
        } else {
            participation.unwrap();
            let error = manager.commit(outer.as_ref()).await.unwrap_err();
            assert!(matches!(error, TransactionError::UnexpectedRollback(_)));
        }

        assert!(!f.bound());
        assert_eq!(synchronization.calls(), ["before_completion", "after_completion(RolledBack)"]);
        assert!(f.rows().await.is_empty());
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn participating_transaction_with_rollback_only() {
    assert_participating_transaction_with_rollback_only("participating_rollback_only", false).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn participating_transaction_with_rollback_only_and_fail_early() {
    assert_participating_transaction_with_rollback_only("participating_fail_early", true).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn participating_transaction_with_incompatible_isolation_level() {
    let Some(f) = Fixture::new("incompatible_isolation").await else { return };
    let f = &f;
    scoped(async {
        let manager = f.manager(|settings| settings.validate_existing_transaction = true);
        let manager = &manager;
        let result = f
            .template_on(manager.clone(), Propagation::Required)
            .execute(|_outer| async move {
                f.insert("a").await?;
                let mut inner = f.template_on(manager.clone(), Propagation::Required);
                inner.definition_mut().isolation = Isolation::Serializable;
                match inner.execute(|_| async move { Ok::<_, TransactionError>(()) }).await {
                    Err(TemplateError::Transaction(error)) => Err::<(), _>(error),
                    other => panic!("unexpected result: {other:?}"),
                }
            })
            .await;
        assert!(matches!(result, Err(TemplateError::Application(TransactionError::InvalidTransactionState(_)))));
        assert!(f.rows().await.is_empty());
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn participating_transaction_with_incompatible_read_only() {
    let Some(f) = Fixture::new("incompatible_read_only").await else { return };
    let f = &f;
    scoped(async {
        let manager = f.manager(|settings| settings.validate_existing_transaction = true);
        let manager = &manager;
        let mut outer = f.template_on(manager.clone(), Propagation::Required);
        outer.definition_mut().read_only = true;
        let result = outer
            .execute(|_outer| async move {
                match f.template_on(manager.clone(), Propagation::Required).execute(|_| async move { Ok::<_, TransactionError>(()) }).await {
                    Err(TemplateError::Transaction(error)) => Err::<(), _>(error),
                    other => panic!("unexpected result: {other:?}"),
                }
            })
            .await;
        assert!(matches!(result, Err(TemplateError::Application(TransactionError::InvalidTransactionState(_)))));
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn propagation_requires_new_with_existing_transaction() {
    let Some(f) = Fixture::new("requires_new").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Required)
            .execute(|outer| async move {
                assert!(outer.is_new_transaction());
                f.insert("outer").await?;
                let outer_pid = f.backend_pid().await?;
                f.template(Propagation::RequiresNew)
                    .execute(|inner| async move {
                        assert!(inner.is_new_transaction());
                        assert!(tsm::is_actual_transaction_active());
                        f.insert("inner").await?;
                        assert_ne!(f.backend_pid().await?, outer_pid);
                        inner.set_rollback_only()
                    })
                    .await
                    .map_err(|error| TransactionError::Other(error.to_string().into()))?;
                assert!(!outer.is_rollback_only());
                assert_eq!(f.backend_pid().await?, outer_pid);
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
        assert_eq!(f.rows().await, ["outer"]);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn propagation_not_supported_with_existing_transaction() {
    let Some(f) = Fixture::new("not_supported").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Required)
            .execute(|outer| async move {
                f.insert("outer").await?;
                f.template(Propagation::NotSupported)
                    .execute(|inner| async move {
                        assert!(!f.bound());
                        assert!(!inner.has_transaction());
                        assert!(!inner.is_new_transaction());
                        assert!(!tsm::is_actual_transaction_active());
                        assert!(tsm::is_synchronization_active());
                        Ok::<_, TransactionError>(())
                    })
                    .await
                    .map_err(|error| TransactionError::Other(error.to_string().into()))?;
                assert!(f.bound());
                assert!(outer.is_new_transaction());
                assert!(tsm::is_actual_transaction_active());
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
        assert_eq!(f.rows().await, ["outer"]);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn propagation_never_with_existing_transaction() {
    let Some(f) = Fixture::new("never").await else { return };
    let f = &f;
    scoped(async {
        let result = f
            .template(Propagation::Required)
            .execute(|_outer| async move {
                f.insert("outer").await?;
                match f.template(Propagation::Never).execute(|_| async move { Ok::<_, TransactionError>(()) }).await {
                    Err(TemplateError::Transaction(error)) => Err::<(), _>(error),
                    other => panic!("unexpected result: {other:?}"),
                }
            })
            .await;
        assert!(matches!(result, Err(TemplateError::Application(TransactionError::InvalidTransactionState(_)))));
        assert!(f.rows().await.is_empty());
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn propagation_supports_and_requires_new_with_early_access() {
    let Some(f) = Fixture::new("supports_requires_new").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Supports)
            .execute(|outer| async move {
                assert!(tsm::is_synchronization_active());
                assert!(!outer.has_transaction());
                let first = f.backend_pid().await?;
                assert_eq!(f.backend_pid().await?, first);
                f.template(Propagation::RequiresNew)
                    .execute(|inner| async move {
                        assert!(inner.is_new_transaction());
                        assert_ne!(f.backend_pid().await?, first);
                        f.insert("inner").await
                    })
                    .await
                    .map_err(|error| TransactionError::Other(error.to_string().into()))?;
                assert_eq!(f.backend_pid().await?, first);
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
        assert_eq!(f.rows().await, ["inner"]);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transaction_with_isolation_and_read_only() {
    let Some(f) = Fixture::new("isolation_read_only").await else { return };
    let f = &f;
    scoped(async {
        let mut template = f.template(Propagation::Required);
        template.definition_mut().isolation = Isolation::Serializable;
        template.definition_mut().read_only = true;
        template
            .execute(|_status| async move {
                assert!(tsm::is_current_transaction_read_only());
                assert_eq!(tsm::current_transaction_isolation_level(), Some(Isolation::Serializable));
                assert_eq!(f.show("transaction_isolation").await?, "serializable");
                assert_eq!(f.show("transaction_read_only").await?, "on");
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
        assert_eq!(f.show("transaction_isolation").await.unwrap(), "read committed");
        assert_eq!(f.show("transaction_read_only").await.unwrap(), "off");
        assert!(f.rows().await.is_empty());
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transaction_with_propagation_supports() {
    let Some(f) = Fixture::new("supports").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Supports)
            .execute(|status| async move {
                assert!(!f.bound());
                assert!(!status.is_new_transaction());
                assert!(!tsm::is_actual_transaction_active());
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn existing_transaction_with_propagation_nested_and_rollback() {
    let Some(f) = Fixture::new("nested_rollback").await else { return };
    let f = &f;
    scoped(async {
        let synchronization = Arc::new(RecordingSynchronization::default());
        let synchronization = &synchronization;
        f.template(Propagation::Required)
            .execute(|outer| async move {
                tsm::register_synchronization(synchronization.clone())?;
                f.insert("outer").await?;
                f.template(Propagation::Nested)
                    .execute(|inner| async move {
                        assert!(!inner.is_new_transaction());
                        assert!(inner.is_nested());
                        assert!(inner.has_savepoint());
                        f.insert("nested").await?;
                        inner.set_rollback_only()
                    })
                    .await
                    .map_err(|error| TransactionError::Other(error.to_string().into()))?;
                assert!(outer.is_new_transaction());
                assert!(!outer.is_rollback_only());
                f.insert("after").await
            })
            .await
            .unwrap();
        assert_eq!(f.rows().await, ["after", "outer"]);
        assert_eq!(
            synchronization.calls(),
            [
                "savepoint",
                "savepoint_rollback",
                "before_commit(false)",
                "before_completion",
                "after_commit",
                "after_completion(Committed)"
            ]
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn existing_transaction_with_propagation_nested_and_required_rollback_only() {
    let Some(f) = Fixture::new("nested_required_rollback_only").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Required)
            .execute(|_outer| async move {
                f.insert("outer").await?;
                let nested = f
                    .template(Propagation::Nested)
                    .execute(|_nested| async move {
                        f.insert("nested").await?;
                        f.template(Propagation::Required)
                            .execute(|inner| async move {
                                assert!(!inner.is_new_transaction());
                                inner.set_rollback_only()
                            })
                            .await
                            .map_err(|error| TransactionError::Other(error.to_string().into()))
                    })
                    .await;
                assert!(matches!(nested, Err(TemplateError::Transaction(TransactionError::UnexpectedRollback(_)))));
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
        assert_eq!(f.rows().await, ["outer"]);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn existing_transaction_with_manual_savepoint_and_rollback() {
    let Some(f) = Fixture::new("manual_savepoint").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Required)
            .execute(|status| async move {
                f.insert("kept").await?;
                let savepoint = status.create_savepoint().await?;
                f.insert("discarded").await?;
                status.rollback_to_savepoint(&savepoint).await
            })
            .await
            .unwrap();
        assert_eq!(f.rows().await, ["kept"]);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transaction_with_propagation_nested_without_existing_transaction() {
    let Some(f) = Fixture::new("nested_new").await else { return };
    let f = &f;
    scoped(async {
        f.template(Propagation::Nested)
            .execute(|status| async move {
                assert!(status.is_new_transaction());
                assert!(!status.is_nested());
                assert!(!status.has_savepoint());
                assert_eq!(status.transaction_name(), "");
                Ok::<_, TransactionError>(())
            })
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn transaction_with_timeout() {
    let Some(f) = Fixture::new("timeout").await else { return };
    let f = &f;
    scoped(async {
        let mut template = f.template(Propagation::Required);
        template.definition_mut().timeout = Some(Duration::from_secs(1));
        let result = template
            .execute(|_status| async move {
                f.insert("a").await?;
                tokio::time::sleep(Duration::from_millis(1500)).await;
                f.insert("b").await
            })
            .await;
        assert!(matches!(result, Err(TemplateError::Application(TransactionError::TimedOut(_)))));
        assert!(f.rows().await.is_empty());
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn dropped_future_rolls_back_and_releases_task_state() {
    let Some(f) = Fixture::new("dropped").await else { return };
    let f = &f;
    scoped(async {
        let template = f.template(Propagation::Required);
        let cancelled = tokio::time::timeout(
            Duration::from_millis(100),
            template.execute(|_status| async move {
                f.insert("a").await?;
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok::<_, TransactionError>(())
            }),
        )
        .await;
        assert!(cancelled.is_err());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(f.rows().await.is_empty());
    })
    .await;
}
