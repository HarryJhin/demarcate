use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use demarcate_core::{
    ManagerSettings, PlatformTransactionManager, Propagation, SynchronizationMode, TemplateError,
    TransactionDefinition, TransactionDriver, TransactionError, TransactionExecution, TransactionExecutionListener,
    TransactionManager, TransactionTemplate, synchronization,
};

#[derive(Default)]
struct Flags {
    begin: AtomicBool,
    commit: AtomicBool,
    rollback: AtomicBool,
    rollback_only: AtomicBool,
}

struct TestDriver {
    existing_transaction: bool,
    can_create_transaction: bool,
    fail_rollback: bool,
    flags: Arc<Flags>,
}

impl TransactionDriver for TestDriver {
    type Transaction = &'static str;
    type Suspended = ();

    fn transaction(&self, _settings: &ManagerSettings) -> Result<Self::Transaction, TransactionError> {
        Ok("transaction")
    }
    fn is_existing_transaction(&self, _transaction: &Self::Transaction) -> bool {
        self.existing_transaction
    }
    async fn begin(
        &self,
        transaction: &Self::Transaction,
        _definition: &TransactionDefinition,
        _settings: &ManagerSettings,
    ) -> Result<(), TransactionError> {
        assert_eq!(*transaction, "transaction");
        if !self.can_create_transaction {
            return Err(TransactionError::CannotCreateTransaction {
                message: "Cannot create transaction".into(),
                source: None,
            });
        }
        self.flags.begin.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn commit(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        assert_eq!(*transaction, "transaction");
        self.flags.commit.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn rollback(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        assert_eq!(*transaction, "transaction");
        self.flags.rollback.store(true, Ordering::SeqCst);
        if self.fail_rollback {
            return Err(TransactionError::System {
                message: "system exception".into(),
                source: None,
                application: None,
            });
        }
        Ok(())
    }
    fn set_rollback_only(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        assert_eq!(*transaction, "transaction");
        self.flags.rollback_only.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct TestListener {
    before_begin: AtomicBool,
    after_begin: AtomicBool,
    before_commit: AtomicBool,
    after_commit: AtomicBool,
    before_rollback: AtomicBool,
    after_rollback: AtomicBool,
}

impl TransactionExecutionListener for TestListener {
    fn before_begin(&self, _transaction: &dyn TransactionExecution) {
        self.before_begin.store(true, Ordering::SeqCst);
    }
    fn after_begin(&self, _transaction: &dyn TransactionExecution, _failure: Option<&TransactionError>) {
        self.after_begin.store(true, Ordering::SeqCst);
    }
    fn before_commit(&self, _transaction: &dyn TransactionExecution) {
        self.before_commit.store(true, Ordering::SeqCst);
    }
    fn after_commit(&self, _transaction: &dyn TransactionExecution, _failure: Option<&TransactionError>) {
        self.after_commit.store(true, Ordering::SeqCst);
    }
    fn before_rollback(&self, _transaction: &dyn TransactionExecution) {
        self.before_rollback.store(true, Ordering::SeqCst);
    }
    fn after_rollback(&self, _transaction: &dyn TransactionExecution, _failure: Option<&TransactionError>) {
        self.after_rollback.store(true, Ordering::SeqCst);
    }
}

struct Fixture {
    tm: TransactionManager<TestDriver>,
    flags: Arc<Flags>,
    listener: Arc<TestListener>,
}

fn fixture(existing_transaction: bool, fail_rollback: bool) -> Fixture {
    let flags = Arc::new(Flags::default());
    let listener = Arc::new(TestListener::default());
    let mut tm = TransactionManager::new(TestDriver {
        existing_transaction,
        can_create_transaction: true,
        fail_rollback,
        flags: flags.clone(),
    });
    tm.settings_mut().transaction_synchronization = SynchronizationMode::Never;
    tm.add_listener(listener.clone());
    Fixture { tm, flags, listener }
}

fn flags(f: &Flags) -> [bool; 4] {
    [
        f.begin.load(Ordering::SeqCst),
        f.commit.load(Ordering::SeqCst),
        f.rollback.load(Ordering::SeqCst),
        f.rollback_only.load(Ordering::SeqCst),
    ]
}

fn listener(l: &TestListener) -> [bool; 6] {
    [
        l.before_begin.load(Ordering::SeqCst),
        l.after_begin.load(Ordering::SeqCst),
        l.before_commit.load(Ordering::SeqCst),
        l.after_commit.load(Ordering::SeqCst),
        l.before_rollback.load(Ordering::SeqCst),
        l.after_rollback.load(Ordering::SeqCst),
    ]
}

async fn scoped<F: Future>(future: F) -> F::Output {
    synchronization::scope(async {
        let output = future.await;
        assert!(synchronization::resource_keys().is_empty());
        assert!(!synchronization::is_synchronization_active());
        output
    })
    .await
}

fn definition(propagation: Propagation) -> TransactionDefinition {
    TransactionDefinition::with_propagation(propagation)
}

#[tokio::test]
async fn no_existing_transaction() {
    scoped(async {
        let f = fixture(false, false);
        let status1 = f.tm.status(Some(&definition(Propagation::Supports))).await.unwrap();
        assert!(!status1.has_transaction());
        let status2 = f.tm.status(Some(&definition(Propagation::Required))).await.unwrap();
        assert!(status2.has_transaction());
        assert!(status2.is_new_transaction());
        let error = f.tm.status(Some(&definition(Propagation::Mandatory))).await.err().unwrap();
        assert!(matches!(error, TransactionError::InvalidTransactionState(_)));
    })
    .await;
}

#[tokio::test]
async fn existing_transaction() {
    scoped(async {
        let f = fixture(true, false);
        for propagation in [Propagation::Supports, Propagation::Required, Propagation::Mandatory] {
            let status = f.tm.status(Some(&definition(propagation))).await.unwrap();
            assert!(status.transaction().is_some());
            assert!(!status.is_new_transaction());
        }
    })
    .await;
}

#[tokio::test]
async fn commit_without_existing_transaction() {
    scoped(async {
        let f = fixture(false, false);
        let status = f.tm.transaction(None).await.unwrap();
        f.tm.commit(status.as_ref()).await.unwrap();
        assert_eq!(flags(&f.flags), [true, true, false, false]);
        assert_eq!(listener(&f.listener), [true, true, true, true, false, false]);
    })
    .await;
}

#[tokio::test]
async fn rollback_without_existing_transaction() {
    scoped(async {
        let f = fixture(false, false);
        let status = f.tm.transaction(None).await.unwrap();
        f.tm.rollback(status.as_ref()).await.unwrap();
        assert_eq!(flags(&f.flags), [true, false, true, false]);
        assert_eq!(listener(&f.listener), [true, true, false, false, true, true]);
    })
    .await;
}

#[tokio::test]
async fn rollback_only_without_existing_transaction() {
    scoped(async {
        let f = fixture(false, false);
        let status = f.tm.transaction(None).await.unwrap();
        status.set_rollback_only().unwrap();
        f.tm.commit(status.as_ref()).await.unwrap();
        assert_eq!(flags(&f.flags), [true, false, true, false]);
        assert_eq!(listener(&f.listener), [true, true, false, false, true, true]);
    })
    .await;
}

#[tokio::test]
async fn commit_with_existing_transaction() {
    scoped(async {
        let f = fixture(true, false);
        let status = f.tm.transaction(None).await.unwrap();
        f.tm.commit(status.as_ref()).await.unwrap();
        assert_eq!(flags(&f.flags), [false, false, false, false]);
        assert_eq!(listener(&f.listener), [false; 6]);
    })
    .await;
}

#[tokio::test]
async fn rollback_with_existing_transaction() {
    scoped(async {
        let f = fixture(true, false);
        let status = f.tm.transaction(None).await.unwrap();
        f.tm.rollback(status.as_ref()).await.unwrap();
        assert_eq!(flags(&f.flags), [false, false, false, true]);
        assert_eq!(listener(&f.listener), [false; 6]);
    })
    .await;
}

#[tokio::test]
async fn rollback_only_with_existing_transaction() {
    scoped(async {
        let f = fixture(true, false);
        let status = f.tm.transaction(None).await.unwrap();
        status.set_rollback_only().unwrap();
        f.tm.commit(status.as_ref()).await.unwrap();
        assert_eq!(flags(&f.flags), [false, false, false, true]);
        assert_eq!(listener(&f.listener), [false; 6]);
    })
    .await;
}

#[tokio::test]
async fn transaction_template() {
    scoped(async {
        let f = fixture(false, false);
        let template = TransactionTemplate::new(Arc::new(f.tm));
        template.execute(|_status| async { Ok::<(), std::io::Error>(()) }).await.unwrap();
        assert_eq!(flags(&f.flags), [true, true, false, false]);
    })
    .await;
}

#[tokio::test]
async fn transaction_template_with_exception() {
    scoped(async {
        let f = fixture(false, false);
        let template = TransactionTemplate::new(Arc::new(f.tm));
        let result = template.execute(|_status| async { Err::<(), &str>("Some application exception") }).await;
        assert!(matches!(result, Err(TemplateError::Application("Some application exception"))));
        assert_eq!(flags(&f.flags), [true, false, true, false]);
    })
    .await;
}

#[tokio::test]
async fn transaction_template_with_rollback_exception() {
    scoped(async {
        let f = fixture(false, true);
        let template = TransactionTemplate::new(Arc::new(f.tm));
        let result = template.execute(|_status| async { Err::<(), &str>("Some application exception") }).await;
        match result {
            Err(TemplateError::Rollback { application, rollback }) => {
                assert_eq!(application, "Some application exception");
                assert!(matches!(rollback, TransactionError::System { .. }));
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(flags(&f.flags), [true, false, true, false]);
    })
    .await;
}

#[tokio::test]
async fn commit_twice_is_rejected() {
    scoped(async {
        let f = fixture(false, false);
        let status = f.tm.transaction(None).await.unwrap();
        f.tm.commit(status.as_ref()).await.unwrap();
        let error = f.tm.commit(status.as_ref()).await.err().unwrap();
        assert!(matches!(error, TransactionError::InvalidTransactionState(_)));
    })
    .await;
}
