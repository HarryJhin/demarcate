mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use demarcate_core::{
    BoundResource, BoxError, BoxFuture, CompletionStatus, Isolation, Propagation, Savepoint, SynchronizationMode,
    TransactionDefinition, TransactionError, TransactionSynchronization, synchronization as tsm,
};
use support::*;

type Hook = Box<dyn Fn() -> BoxFuture<'static, Result<(), TransactionError>> + Send + Sync>;

struct TestTransactionSynchronization {
    data_source: Arc<dyn DataSource>,
    status: CompletionStatus,
    before_commit_called: AtomicBool,
    before_completion_called: AtomicBool,
    after_commit_called: AtomicBool,
    after_completion_called: AtomicBool,
    after_completion_exception: Mutex<Option<TransactionError>>,
    hook: Option<Hook>,
}

impl TestTransactionSynchronization {
    fn new(data_source: &Arc<dyn DataSource>, status: CompletionStatus) -> Arc<Self> {
        Self::with_hook(data_source, status, None)
    }
    fn with_hook(data_source: &Arc<dyn DataSource>, status: CompletionStatus, hook: Option<Hook>) -> Arc<Self> {
        Arc::new(Self {
            data_source: data_source.clone(),
            status,
            before_commit_called: AtomicBool::new(false),
            before_completion_called: AtomicBool::new(false),
            after_commit_called: AtomicBool::new(false),
            after_completion_called: AtomicBool::new(false),
            after_completion_exception: Mutex::new(None),
            hook,
        })
    }
    fn flags(&self) -> [bool; 4] {
        [
            self.before_commit_called.load(Ordering::SeqCst),
            self.before_completion_called.load(Ordering::SeqCst),
            self.after_commit_called.load(Ordering::SeqCst),
            self.after_completion_called.load(Ordering::SeqCst),
        ]
    }
    async fn do_after_completion(&self, status: CompletionStatus) -> Result<(), TransactionError> {
        assert!(!self.after_completion_called.swap(true, Ordering::SeqCst));
        if status != self.status {
            return Err(TransactionError::Other(format!("expected {:?} but was {status:?}", self.status).into()));
        }
        if !has_resource(&self.data_source) {
            return Err(TransactionError::Other("resource must be bound in afterCompletion".into()));
        }
        if let Some(hook) = &self.hook {
            hook().await?;
        }
        Ok(())
    }
}

impl TransactionSynchronization for TestTransactionSynchronization {
    fn before_commit(&self, _read_only: bool) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            assert_eq!(self.status, CompletionStatus::Committed, "Should never be called");
            assert!(!self.before_commit_called.swap(true, Ordering::SeqCst));
            Ok(())
        })
    }
    fn before_completion(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            assert!(!self.before_completion_called.swap(true, Ordering::SeqCst));
            Ok(())
        })
    }
    fn after_commit(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            assert_eq!(self.status, CompletionStatus::Committed, "Should never be called");
            assert!(!self.after_commit_called.swap(true, Ordering::SeqCst));
            Ok(())
        })
    }
    fn after_completion(&self, status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            if let Err(error) = self.do_after_completion(status).await {
                *self.after_completion_exception.lock().unwrap() = Some(error);
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct TestSavepointSynchronization {
    savepoint_called: AtomicBool,
    savepoint_rollback_called: AtomicBool,
}

impl TransactionSynchronization for TestSavepointSynchronization {
    fn savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            assert!(!self.savepoint_called.swap(true, Ordering::SeqCst));
            Ok(())
        })
    }
    fn savepoint_rollback<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            assert!(!self.savepoint_rollback_called.swap(true, Ordering::SeqCst));
            Ok(())
        })
    }
}

fn def(propagation: Propagation) -> TransactionDefinition {
    TransactionDefinition::with_propagation(propagation)
}

fn pre(ds: &Arc<dyn DataSource>) {
    assert!(!has_resource(ds));
    assert!(!tsm::is_synchronization_active());
}

async fn assert_transaction_commit_restoring_auto_commit(auto_commit: bool) {
    scoped(async {
        let con = Con::new();
        con.given(|settings| settings.auto_commit = auto_commit);
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = template(&tm, TransactionDefinition::new());
        pre(ds);
        execute(&tt, |status| async move {
            assert!(has_resource(ds));
            assert!(tsm::is_synchronization_active());
            assert!(status.is_new_transaction());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            let t_con = utils::connection(ds).await?;
            let mut guard = t_con.lock().await;
            guard.warnings().await.map_err(|error| TransactionError::Other(error.into()))?;
            guard.clear_warnings().await.map_err(|error| TransactionError::Other(error.into()))?;
            Ok(())
        })
        .await
        .unwrap();
        pre(ds);
        if auto_commit {
            con.log.verify_in_order(&["setAutoCommit(false)", "commit", "setAutoCommit(true)"]);
        }
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_commit_with_auto_commit_true() {
    assert_transaction_commit_restoring_auto_commit(true).await;
}

#[tokio::test]
async fn transaction_commit_with_auto_commit_false() {
    assert_transaction_commit_restoring_auto_commit(false).await;
}

async fn assert_transaction_rollback_restoring_auto_commit(auto_commit: bool) {
    scoped(async {
        let con = Con::new();
        con.given(|settings| settings.auto_commit = auto_commit);
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = template(&tm, TransactionDefinition::new());
        pre(ds);
        let error = execute(&tt, |status| async move {
            assert!(has_resource(ds));
            assert!(tsm::is_synchronization_active());
            assert!(status.is_new_transaction());
            utils::connection(ds).await?;
            Err::<(), _>(application_exception())
        })
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "Application exception");
        pre(ds);
        if auto_commit {
            con.log.verify_in_order(&["setAutoCommit(false)", "rollback", "setAutoCommit(true)"]);
        }
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_rollback_with_auto_commit_true() {
    assert_transaction_rollback_restoring_auto_commit(true).await;
}

#[tokio::test]
async fn transaction_rollback_with_auto_commit_false() {
    assert_transaction_rollback_restoring_auto_commit(false).await;
}

#[tokio::test]
async fn transaction_rollback_only() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager_with(ds, |settings| settings.transaction_synchronization = SynchronizationMode::Never);
        let tt = template(&tm, TransactionDefinition::new());
        pre(ds);
        let holder = Arc::new(ConnectionHolder::with_transaction_active(con.shared.clone(), true));
        tsm::bind_resource(data_source_key(ds), BoundResource::holder(holder)).unwrap();
        let error = execute(&tt, |status| async move {
            assert!(has_resource(ds));
            assert!(!tsm::is_synchronization_active());
            assert!(!status.is_new_transaction());
            Err::<(), _>(application_exception())
        })
        .await
        .unwrap_err();
        assert!(!tsm::is_synchronization_active());
        assert_eq!(error.to_string(), "Application exception");
        tsm::unbind_resource(&data_source_key(ds)).unwrap();
        assert!(!has_resource(ds));
    })
    .await;
}

async fn assert_participating_transaction_with_rollback_only(fail_early: bool) {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager_with(ds, |settings| settings.fail_early_on_global_rollback_only = fail_early);
        pre(ds);

        let ts = tm.transaction(Some(&TransactionDefinition::new())).await.unwrap();
        let synch = TestTransactionSynchronization::new(ds, CompletionStatus::RolledBack);
        tsm::register_synchronization(synch.clone()).unwrap();

        let outer_transaction_boundary_reached = AtomicBool::new(false);
        let attempt: Result<(), TransactionError> = async {
            assert!(ts.is_new_transaction());
            let tt = &template(tm, TransactionDefinition::new());
            execute(tt, |status1| async move {
                assert!(!status1.is_new_transaction());
                assert!(!status1.is_rollback_only());
                execute(tt, |status2| async move {
                    assert!(has_resource(ds));
                    assert!(tsm::is_synchronization_active());
                    assert!(!status2.is_new_transaction());
                    status2.set_rollback_only()
                })
                .await?;
                assert!(!status1.is_new_transaction());
                assert!(status1.is_rollback_only());
                Ok(())
            })
            .await?;
            outer_transaction_boundary_reached.store(true, Ordering::SeqCst);
            tm.commit(ts.as_ref()).await?;
            panic!("Should have thrown UnexpectedRollbackException");
        }
        .await;

        assert!(matches!(attempt, Err(TransactionError::UnexpectedRollback(_))), "{attempt:?}");
        if !outer_transaction_boundary_reached.load(Ordering::SeqCst) {
            tm.rollback(ts.as_ref()).await.unwrap();
        }
        assert_eq!(outer_transaction_boundary_reached.load(Ordering::SeqCst), !fail_early);

        assert!(!has_resource(ds));
        assert_eq!(synch.flags(), [false, true, false, true]);
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn participating_transaction_with_rollback_only() {
    assert_participating_transaction_with_rollback_only(false).await;
}

#[tokio::test]
async fn participating_transaction_with_rollback_only_and_fail_early() {
    assert_participating_transaction_with_rollback_only(true).await;
}

#[tokio::test]
async fn participating_transaction_with_incompatible_isolation_level() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager_with(ds, |settings| settings.validate_existing_transaction = true);
        pre(ds);
        let tt = template(&tm, TransactionDefinition::new());
        let tt2 = &template(&tm, TransactionDefinition::new().isolation(Isolation::Serializable));
        let error = execute(&tt, |status| async move {
            assert!(!status.is_rollback_only());
            execute(tt2, |status2| async move { status2.set_rollback_only() }).await?;
            assert!(status.is_rollback_only());
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransactionError::InvalidTransactionState(_)), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn participating_transaction_with_incompatible_read_only() {
    scoped(async {
        let con = Con::new();
        con.will_throw("setReadOnly", "read-only not supported");
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager_with(ds, |settings| settings.validate_existing_transaction = true);
        pre(ds);
        let tt = template(&tm, TransactionDefinition::new().read_only(true));
        let tt2 = &template(&tm, TransactionDefinition::new().read_only(false));
        let error = execute(&tt, |status| async move {
            assert!(!status.is_rollback_only());
            execute(tt2, |status2| async move { status2.set_rollback_only() }).await?;
            assert!(status.is_rollback_only());
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransactionError::InvalidTransactionState(_)), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn participating_transaction_with_transaction_started_from_synch() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        pre(ds);
        let tt = &template(&tm, def(Propagation::RequiresNew));
        let hook_manager = tm.clone();
        let hook: Hook = Box::new(move || {
            let tt = template(&hook_manager, def(Propagation::RequiresNew));
            Box::pin(async move {
                execute(&tt, |_status2| async move { Ok(()) }).await?;
                tsm::register_synchronization(Arc::new(TestSavepointSynchronization::default()))
            })
        });
        let synch = &TestTransactionSynchronization::with_hook(ds, CompletionStatus::Committed, Some(hook));
        execute(tt, |_status| async move { tsm::register_synchronization(synch.clone()) }).await.unwrap();
        assert!(!has_resource(ds));
        assert_eq!(synch.flags(), [true, true, true, true]);
        assert!(matches!(*synch.after_completion_exception.lock().unwrap(), Some(TransactionError::InvalidState(_))));
        con.log.verify_times("commit", 2);
        con.log.verify_times("close", 2);
    })
    .await;
}

#[tokio::test]
async fn participating_transaction_with_different_connection_obtained_from_synch() {
    scoped(async {
        let con = Con::new();
        let con2 = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let ds2 = MockDataSource::returning(&[&con2]);
        let tm = manager(ds);
        pre(ds);
        let tt = &template(&tm, TransactionDefinition::new());
        let hook: Hook = Box::new(move || {
            let ds2 = ds2.clone();
            Box::pin(async move {
                let con = utils::connection(&ds2).await?;
                utils::release_connection(Some(&con), Some(&ds2)).await;
                Ok(())
            })
        });
        let synch = &TestTransactionSynchronization::with_hook(ds, CompletionStatus::Committed, Some(hook));
        execute(tt, |_status| async move { tsm::register_synchronization(synch.clone()) }).await.unwrap();
        assert!(!has_resource(ds));
        assert_eq!(synch.flags(), [true, true, true, true]);
        assert!(synch.after_completion_exception.lock().unwrap().is_none());
        con.log.verify("commit");
        con.log.verify("close");
        con2.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn participating_transaction_with_rollback_only_and_inner_synch() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager_with(ds, |settings| settings.transaction_synchronization = SynchronizationMode::Never);
        let tm2 = &manager(ds);
        pre(ds);

        let ts = tm.transaction(Some(&TransactionDefinition::new())).await.unwrap();
        let synch = &TestTransactionSynchronization::new(ds, CompletionStatus::Unknown);

        let attempt: Result<(), TransactionError> = async {
            assert!(ts.is_new_transaction());
            let tt = &template(tm2, TransactionDefinition::new());
            execute(tt, |status| async move {
                assert!(!status.is_new_transaction());
                assert!(!status.is_rollback_only());
                execute(tt, |status2| async move {
                    assert!(has_resource(ds));
                    assert!(tsm::is_synchronization_active());
                    assert!(!status2.is_new_transaction());
                    status2.set_rollback_only()
                })
                .await?;
                assert!(!status.is_new_transaction());
                assert!(status.is_rollback_only());
                tsm::register_synchronization(synch.clone())
            })
            .await?;
            tm.commit(ts.as_ref()).await
        }
        .await;
        assert!(matches!(attempt, Err(TransactionError::UnexpectedRollback(_))), "{attempt:?}");

        assert!(!has_resource(ds));
        assert_eq!(synch.flags(), [false, true, false, true]);
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn propagation_requires_new_with_existing_transaction() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = &template(&tm, def(Propagation::RequiresNew));
        pre(ds);
        execute(tt, |status| async move {
            assert!(status.is_new_transaction());
            assert!(tsm::is_synchronization_active());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            execute(tt, |status2| async move {
                assert!(has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(status2.is_new_transaction());
                assert!(!tsm::is_current_transaction_read_only());
                assert!(tsm::is_actual_transaction_active());
                status2.set_rollback_only()
            })
            .await?;
            assert!(status.is_new_transaction());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con.log.verify("rollback");
        con.log.verify("commit");
        con.log.verify_times("close", 2);
    })
    .await;
}

#[tokio::test]
async fn propagation_requires_new_with_existing_transaction_and_unrelated_data_source() {
    scoped(async {
        let con = Con::new();
        let con2 = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let ds2 = &MockDataSource::returning(&[&con2]);
        let tm = manager(ds);
        let tm2 = manager(ds2);
        let tt = template(&tm, def(Propagation::RequiresNew));
        let tt2 = &template(&tm2, def(Propagation::RequiresNew));
        assert!(!has_resource(ds));
        assert!(!has_resource(ds2));
        assert!(!tsm::is_synchronization_active());
        execute(&tt, |status| async move {
            assert!(status.is_new_transaction());
            assert!(tsm::is_synchronization_active());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            execute(tt2, |status2| async move {
                assert!(has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(status2.is_new_transaction());
                assert!(!tsm::is_current_transaction_read_only());
                assert!(tsm::is_actual_transaction_active());
                status2.set_rollback_only()
            })
            .await?;
            assert!(status.is_new_transaction());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        assert!(!has_resource(ds2));
        con.log.verify("commit");
        con.log.verify("close");
        con2.log.verify("rollback");
        con2.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn propagation_requires_new_with_existing_transaction_and_unrelated_failing_data_source() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let ds2 = &MockDataSource::failing("failure");
        let tm = manager(ds);
        let tm2 = manager_with(ds2, |settings| settings.transaction_synchronization = SynchronizationMode::Never);
        let tt = template(&tm, def(Propagation::RequiresNew));
        let tt2 = &template(&tm2, def(Propagation::RequiresNew));
        assert!(!has_resource(ds));
        assert!(!has_resource(ds2));
        assert!(!tsm::is_synchronization_active());
        let error = execute(&tt, |status| async move {
            assert!(tsm::is_synchronization_active());
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.is_read_only());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            execute(tt2, |status2| async move { status2.set_rollback_only() }).await
        })
        .await
        .unwrap_err();
        match &error {
            TransactionError::CannotCreateTransaction { source: Some(source), .. } => {
                assert_eq!(source.to_string(), "failure");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(!has_resource(ds));
        assert!(!has_resource(ds2));
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn propagation_not_supported_with_existing_transaction() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = template(tm, def(Propagation::RequiresNew));
        pre(ds);
        execute(&tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            let tt = template(tm, def(Propagation::NotSupported));
            execute(&tt, |status2| async move {
                assert!(!has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(!status2.has_transaction());
                assert!(!status2.is_new_transaction());
                assert!(!status2.is_nested());
                assert!(!status2.is_read_only());
                assert!(!tsm::is_current_transaction_read_only());
                assert!(!tsm::is_actual_transaction_active());
                status2.set_rollback_only()
            })
            .await?;
            assert!(status.is_new_transaction());
            assert!(!tsm::is_current_transaction_read_only());
            assert!(tsm::is_actual_transaction_active());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con.log.verify("commit");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn propagation_never_with_existing_transaction() {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = template(tm, def(Propagation::RequiresNew));
        pre(ds);
        let error = execute(&tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            let tt = template(tm, def(Propagation::Never));
            execute(&tt, |_status2| async move {
                panic!("Should have thrown IllegalTransactionStateException");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await?;
            panic!("Should have thrown IllegalTransactionStateException");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransactionError::InvalidTransactionState(_)), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn propagation_supports_and_requires_new() {
    scoped(async {
        let con = &Con::new();
        let ds = &MockDataSource::returning(&[con]);
        let tm = &manager(ds);
        let tt = template(tm, def(Propagation::Supports));
        pre(ds);
        execute(&tt, |status| async move {
            assert!(tsm::is_synchronization_active());
            assert!(!status.has_transaction());
            assert!(!status.is_new_transaction());
            assert!(!status.is_nested());
            let tt2 = template(tm, def(Propagation::RequiresNew));
            execute(&tt2, |status2| async move {
                assert!(has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(status2.has_transaction());
                assert!(status2.is_new_transaction());
                assert!(!status2.is_nested());
                assert!(con.is(&utils::connection(ds).await?));
                assert!(con.is(&utils::connection(ds).await?));
                Ok(())
            })
            .await
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con.log.verify("commit");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn propagation_supports_and_requires_new_with_early_access() {
    scoped(async {
        let con1 = &Con::new();
        let con2 = &Con::new();
        let ds = &MockDataSource::returning(&[con1, con2]);
        let tm = &manager(ds);
        let tt = template(tm, def(Propagation::Supports));
        pre(ds);
        execute(&tt, |status| async move {
            assert!(tsm::is_synchronization_active());
            assert!(con1.is(&utils::connection(ds).await?));
            assert!(con1.is(&utils::connection(ds).await?));
            assert!(!status.has_transaction());
            assert!(!status.is_new_transaction());
            assert!(!status.is_nested());
            let tt2 = template(tm, def(Propagation::RequiresNew));
            execute(&tt2, |status2| async move {
                assert!(has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(con2.is(&utils::connection(ds).await?));
                assert!(con2.is(&utils::connection(ds).await?));
                assert!(status2.has_transaction());
                assert!(status2.is_new_transaction());
                assert!(!status2.is_nested());
                Ok(())
            })
            .await?;
            assert!(con1.is(&utils::connection(ds).await?));
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con1.log.verify("close");
        con2.log.verify("commit");
        con2.log.verify("close");
    })
    .await;
}

async fn assert_transaction_read_only(tm: &Tm, ds: &Arc<dyn DataSource>, isolation: Isolation) {
    let tt = template(tm, def(Propagation::RequiresNew).isolation(isolation).read_only(true).name("my-transaction"));
    assert!(!has_resource(ds));
    execute(&tt, |status| async move {
        assert_eq!(status.transaction_name(), "my-transaction");
        assert!(status.has_transaction());
        assert!(status.is_new_transaction());
        assert!(!status.is_nested());
        assert!(!status.has_savepoint());
        assert!(status.is_read_only());
        assert!(tsm::is_current_transaction_read_only());
        assert!(tsm::is_actual_transaction_active());
        assert!(!status.is_rollback_only());
        assert!(!status.is_completed());
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_resource(ds));
}

#[tokio::test]
async fn transaction_with_isolation_and_read_only() {
    scoped(async {
        let con = Con::new();
        con.given(|settings| {
            settings.isolation = Isolation::ReadUncommitted;
            settings.auto_commit = true;
            settings.read_only = false;
        });
        let ds = &MockDataSource::returning(&[&con]);
        assert_transaction_read_only(&manager(ds), ds, Isolation::RepeatableRead).await;
        con.log.verify_exactly(&[
            "isReadOnly",
            "setReadOnly(true)",
            "getTransactionIsolation",
            "setTransactionIsolation(RepeatableRead)",
            "getAutoCommit",
            "setAutoCommit(false)",
            "commit",
            "setAutoCommit(true)",
            "setTransactionIsolation(ReadUncommitted)",
            "setReadOnly(false)",
            "close",
        ]);
    })
    .await;
}

#[tokio::test]
async fn transaction_with_default_read_only() {
    scoped(async {
        let con = Con::new();
        con.given(|settings| {
            settings.auto_commit = true;
            settings.read_only = true;
        });
        let ds = &MockDataSource::returning(&[&con]);
        assert_transaction_read_only(&manager(ds), ds, Isolation::Default).await;
        con.log.verify_exactly(&[
            "isReadOnly",
            "getAutoCommit",
            "setAutoCommit(false)",
            "commit",
            "setAutoCommit(true)",
            "close",
        ]);
    })
    .await;
}

#[tokio::test]
async fn transaction_with_enforce_read_only() {
    scoped(async {
        let con = Con::new();
        con.given(|settings| {
            settings.auto_commit = true;
            settings.read_only = false;
        });
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager_from(DataSourceTransactionDriver::new(ds.clone()).enforce_read_only(true), |_| {});
        assert_transaction_read_only(&tm, ds, Isolation::Default).await;
        con.log.verify_exactly(&[
            "isReadOnly",
            "setReadOnly(true)",
            "getAutoCommit",
            "setAutoCommit(false)",
            "createStatement",
            "stmt.executeUpdate(SET TRANSACTION READ ONLY)",
            "stmt.close",
            "commit",
            "setAutoCommit(true)",
            "setReadOnly(false)",
            "close",
        ]);
    })
    .await;
}

async fn transaction_with_timeout(timeout: u64) {
    scoped(async {
        let con = Con::new();
        con.given(|settings| settings.auto_commit = true);
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = template(&tm, TransactionDefinition::new().timeout(Duration::from_secs(timeout)));
        assert!(!has_resource(ds));
        let result = execute(&tt, |_status| async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let con = utils::connection(ds).await?;
            let mut ps2 = con
                .lock()
                .await
                .prepare_statement("some SQL statement")
                .await
                .map_err(|error| TransactionError::Other(error.into()))?;
            utils::apply_transaction_timeout(ps2.as_mut(), Some(ds)).await
        })
        .await;
        if timeout <= 1 {
            assert!(matches!(result, Err(TransactionError::TimedOut(_))), "{result:?}");
        } else {
            result.unwrap();
        }
        assert!(!has_resource(ds));
        if timeout > 1 {
            con.log.verify(&format!("ps.setQueryTimeout({})", timeout - 1));
            con.log.verify("commit");
        } else {
            con.log.verify("rollback");
        }
        con.log.verify_in_order(&["setAutoCommit(false)", "setAutoCommit(true)"]);
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_with_1_second_timeout() {
    transaction_with_timeout(1).await;
}

#[tokio::test]
async fn transaction_with_10_second_timeout() {
    transaction_with_timeout(10).await;
}

#[tokio::test]
async fn transaction_with_exception_on_begin() {
    scoped(async {
        let con = Con::new();
        con.will_throw("getAutoCommit", "Cannot begin");
        let ds = &MockDataSource::returning(&[&con]);
        let tt = template(&manager(ds), TransactionDefinition::new());
        let error = execute(&tt, |_status| async move { Ok(()) }).await.unwrap_err();
        assert!(matches!(error, TransactionError::CannotCreateTransaction { .. }), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_with_exception_on_commit() {
    scoped(async {
        let con = Con::new();
        con.will_throw("commit", "Cannot commit");
        let ds = &MockDataSource::returning(&[&con]);
        let tt = template(&manager(ds), TransactionDefinition::new());
        let error = execute(&tt, |_status| async move { Ok(()) }).await.unwrap_err();
        assert!(matches!(error, TransactionError::System { .. }), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_with_exception_on_commit_and_rollback_on_commit_failure() {
    scoped(async {
        let con = Con::new();
        con.will_throw("commit", "Cannot commit");
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager_with(ds, |settings| settings.rollback_on_commit_failure = true);
        let tt = template(&tm, TransactionDefinition::new());
        let error = execute(&tt, |_status| async move { Ok(()) }).await.unwrap_err();
        assert!(matches!(error, TransactionError::System { .. }), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify("rollback");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_with_exception_on_rollback() {
    scoped(async {
        let con = Con::new();
        con.given(|settings| settings.auto_commit = true);
        con.will_throw("rollback", "Cannot rollback");
        let ds = &MockDataSource::returning(&[&con]);
        let tt = template(&manager(ds), TransactionDefinition::new());
        let error = execute(&tt, |status| async move {
            assert_eq!(status.transaction_name(), "");
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            assert!(!status.is_read_only());
            assert!(!status.is_rollback_only());
            status.set_rollback_only()?;
            assert!(status.is_rollback_only());
            assert!(!status.is_completed());
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransactionError::System { .. }), "{error:?}");
        assert!(!has_resource(ds));
        con.log.verify_in_order(&["setAutoCommit(false)", "rollback", "setAutoCommit(true)"]);
        con.log.verify("close");
    })
    .await;
}

async fn transaction_without_resource(propagation: Propagation, check_state: bool) {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tt = template(&manager(ds), def(propagation));
        assert!(!has_resource(ds));
        execute(&tt, |status| async move {
            assert!(!has_resource(ds));
            assert!(!status.is_new_transaction());
            if check_state {
                assert!(!tsm::is_current_transaction_read_only());
                assert!(!tsm::is_actual_transaction_active());
            }
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
    })
    .await;
}

#[tokio::test]
async fn transaction_with_propagation_supports() {
    transaction_without_resource(Propagation::Supports, true).await;
}

#[tokio::test]
async fn transaction_with_propagation_not_supported() {
    transaction_without_resource(Propagation::NotSupported, false).await;
}

#[tokio::test]
async fn transaction_with_propagation_never() {
    transaction_without_resource(Propagation::Never, false).await;
}

fn savepoint_con() -> Con {
    let con = Con::new();
    con.given(|settings| settings.supports_savepoints = true);
    con
}

async fn assert_existing_transaction_with_propagation_nested(count: usize) {
    scoped(async {
        let con = savepoint_con();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = &template(&tm, def(Propagation::Nested));
        pre(ds);
        execute(tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            let synch = &Arc::new(TestSavepointSynchronization::default());
            tsm::register_synchronization(synch.clone())?;
            for _ in 0..count {
                execute(tt, |status2| async move {
                    assert!(has_resource(ds));
                    assert!(tsm::is_synchronization_active());
                    assert!(status2.has_transaction());
                    assert!(!status2.is_new_transaction());
                    assert!(status2.is_nested());
                    assert!(status2.has_savepoint());
                    assert!(synch.savepoint_called.load(Ordering::SeqCst));
                    Ok(())
                })
                .await?;
                assert!(!synch.savepoint_rollback_called.load(Ordering::SeqCst));
                synch.savepoint_called.store(false, Ordering::SeqCst);
            }
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        for index in 1..=count {
            con.log.verify(&format!("setSavepoint({SAVEPOINT_NAME_PREFIX}{index})"));
        }
        assert_eq!(con.log.times_starting_with("releaseSavepoint("), count);
        con.log.verify("commit");
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn existing_transaction_with_propagation_nested() {
    assert_existing_transaction_with_propagation_nested(1).await;
}

#[tokio::test]
async fn existing_transaction_with_propagation_nested_twice() {
    assert_existing_transaction_with_propagation_nested(2).await;
}

fn verify_savepoint_rollback(con: &Con) {
    con.log.verify("rollback(SAVEPOINT_1)");
    con.log.verify("releaseSavepoint(SAVEPOINT_1)");
    con.log.verify("commit");
    con.log.verify("close");
}

#[tokio::test]
async fn existing_transaction_with_propagation_nested_and_rollback() {
    scoped(async {
        let con = savepoint_con();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = manager(ds);
        let tt = &template(&tm, def(Propagation::Nested));
        pre(ds);
        execute(tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            let synch = &Arc::new(TestSavepointSynchronization::default());
            tsm::register_synchronization(synch.clone())?;
            execute(tt, |status2| async move {
                assert!(has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(status2.has_transaction());
                assert!(!status2.is_new_transaction());
                assert!(status2.is_nested());
                assert!(status2.has_savepoint());
                assert!(synch.savepoint_called.load(Ordering::SeqCst));
                assert!(!synch.savepoint_rollback_called.load(Ordering::SeqCst));
                status2.set_rollback_only()
            })
            .await?;
            assert!(synch.savepoint_rollback_called.load(Ordering::SeqCst));
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        verify_savepoint_rollback(&con);
    })
    .await;
}

async fn nested_and_required(rollback_only: bool) {
    scoped(async {
        let con = savepoint_con();
        let ds = &MockDataSource::returning(&[&con]);
        let tm = &manager(ds);
        let tt = &template(tm, def(Propagation::Nested));
        pre(ds);
        execute(tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            let synch = &Arc::new(TestSavepointSynchronization::default());
            tsm::register_synchronization(synch.clone())?;
            let nested = execute(tt, |status2| async move {
                assert!(has_resource(ds));
                assert!(tsm::is_synchronization_active());
                assert!(status2.has_transaction());
                assert!(!status2.is_new_transaction());
                assert!(status2.is_nested());
                assert!(status2.has_savepoint());
                assert!(synch.savepoint_called.load(Ordering::SeqCst));
                assert!(!synch.savepoint_rollback_called.load(Ordering::SeqCst));
                let ntt = template(tm, TransactionDefinition::new());
                execute(&ntt, |status3| async move {
                    assert!(has_resource(ds));
                    assert!(tsm::is_synchronization_active());
                    assert!(status3.has_transaction());
                    assert!(!status3.is_new_transaction());
                    assert!(!status3.is_nested());
                    assert!(!status3.has_savepoint());
                    if rollback_only {
                        status3.set_rollback_only()
                    } else {
                        Err(TransactionError::InvalidState("IllegalStateException".into()))
                    }
                })
                .await
            })
            .await;
            if rollback_only {
                assert!(matches!(nested, Err(TransactionError::UnexpectedRollback(_))), "{nested:?}");
            } else {
                assert!(matches!(nested, Err(TransactionError::InvalidState(_))), "{nested:?}");
            }
            assert!(synch.savepoint_rollback_called.load(Ordering::SeqCst));
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        verify_savepoint_rollback(&con);
    })
    .await;
}

#[tokio::test]
async fn existing_transaction_with_propagation_nested_and_required_rollback() {
    nested_and_required(false).await;
}

#[tokio::test]
async fn existing_transaction_with_propagation_nested_and_required_rollback_only() {
    nested_and_required(true).await;
}

#[tokio::test]
async fn existing_transaction_with_manual_savepoint() {
    scoped(async {
        let con = savepoint_con();
        let (ds, ds_log) = MockDataSource::returning_with_log(&[&con]);
        let ds = &ds;
        let tt = template(&manager(ds), def(Propagation::Nested));
        pre(ds);
        execute(&tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            let synch = Arc::new(TestSavepointSynchronization::default());
            tsm::register_synchronization(synch.clone())?;
            let savepoint = status.create_savepoint().await?;
            assert!(synch.savepoint_called.load(Ordering::SeqCst));
            status.release_savepoint(&savepoint).await?;
            assert!(!synch.savepoint_rollback_called.load(Ordering::SeqCst));
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con.log.verify("releaseSavepoint(SAVEPOINT_1)");
        con.log.verify("commit");
        con.log.verify("close");
        ds_log.verify("getConnection");
    })
    .await;
}

#[tokio::test]
async fn existing_transaction_with_manual_savepoint_and_rollback() {
    scoped(async {
        let con = savepoint_con();
        let ds = &MockDataSource::returning(&[&con]);
        let tt = template(&manager(ds), def(Propagation::Nested));
        pre(ds);
        execute(&tt, |status| async move {
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            let synch = Arc::new(TestSavepointSynchronization::default());
            tsm::register_synchronization(synch.clone())?;
            let savepoint = status.create_savepoint().await?;
            assert!(synch.savepoint_called.load(Ordering::SeqCst));
            assert!(!synch.savepoint_rollback_called.load(Ordering::SeqCst));
            status.rollback_to_savepoint(&savepoint).await?;
            assert!(synch.savepoint_rollback_called.load(Ordering::SeqCst));
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con.log.verify("rollback(SAVEPOINT_1)");
        con.log.verify("commit");
        con.log.verify("close");
    })
    .await;
}

async fn transaction_with_propagation_nested_new(rollback: bool) {
    scoped(async {
        let con = Con::new();
        let ds = &MockDataSource::returning(&[&con]);
        let tt = template(&manager(ds), def(Propagation::Nested));
        pre(ds);
        execute(&tt, |status| async move {
            assert_eq!(status.transaction_name(), "");
            assert!(status.has_transaction());
            assert!(status.is_new_transaction());
            assert!(!status.is_nested());
            assert!(!status.has_savepoint());
            assert!(!status.is_read_only());
            assert!(!status.is_rollback_only());
            if rollback {
                status.set_rollback_only()?;
                assert!(status.is_rollback_only());
            }
            assert!(!status.is_completed());
            Ok(())
        })
        .await
        .unwrap();
        assert!(!has_resource(ds));
        con.log.verify(if rollback { "rollback" } else { "commit" });
        con.log.verify("close");
    })
    .await;
}

#[tokio::test]
async fn transaction_with_propagation_nested() {
    transaction_with_propagation_nested_new(false).await;
}

#[tokio::test]
async fn transaction_with_propagation_nested_and_rollback() {
    transaction_with_propagation_nested_new(true).await;
}
