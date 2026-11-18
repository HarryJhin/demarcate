mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use demarcate_core::{
    BoxError, ResourceHolderSupport, SimpleTransactionStatus, TransactionDefinition, TransactionError,
    TransactionExecution, TransactionPhase, TransactionTemplate, on_transaction,
};
use support::*;

type Calls = Arc<Mutex<Vec<&'static str>>>;

async fn listen(calls: &Calls, phase: TransactionPhase, name: &'static str, fallback: bool) -> Result<bool, BoxError> {
    let calls = calls.clone();
    on_transaction(phase, fallback, move || {
        let calls = calls.clone();
        async move {
            calls.lock().unwrap().push(name);
            Ok(())
        }
    })
    .await
}

async fn register_all(calls: &Calls) -> Result<(), TransactionError> {
    for (phase, name) in [
        (TransactionPhase::BeforeCommit, "before_commit"),
        (TransactionPhase::AfterCommit, "after_commit"),
        (TransactionPhase::AfterRollback, "after_rollback"),
        (TransactionPhase::AfterCompletion, "after_completion"),
    ] {
        assert!(listen(calls, phase, name, false).await.map_err(TransactionError::Other)?);
    }
    assert!(calls.lock().unwrap().is_empty(), "listeners must not run before the transaction completes");
    Ok(())
}

#[tokio::test]
async fn transactional_listeners_after_commit() {
    scoped(async {
        let con = Con::new();
        let ds = MockDataSource::returning(&[&con]);
        let tt = template(&manager(&ds), TransactionDefinition::new());
        let calls = &Calls::default();
        execute(&tt, |_status| async move { register_all(calls).await }).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), ["before_commit", "after_commit", "after_completion"]);
    })
    .await;
}

#[tokio::test]
async fn transactional_listeners_after_rollback() {
    scoped(async {
        let con = Con::new();
        let ds = MockDataSource::returning(&[&con]);
        let tt = template(&manager(&ds), TransactionDefinition::new());
        let calls = &Calls::default();
        let result = execute(&tt, |_status| async move {
            register_all(calls).await?;
            Err::<(), _>(application_exception())
        })
        .await;
        assert!(result.is_err());
        assert_eq!(*calls.lock().unwrap(), ["after_rollback", "after_completion"]);
    })
    .await;
}

#[tokio::test]
async fn transactional_listener_without_transaction() {
    scoped(async {
        let calls = &Calls::default();
        assert!(!listen(calls, TransactionPhase::AfterCommit, "skipped", false).await.unwrap());
        assert!(calls.lock().unwrap().is_empty());
        assert!(listen(calls, TransactionPhase::AfterCommit, "fallback", true).await.unwrap());
        assert_eq!(*calls.lock().unwrap(), ["fallback"]);
    })
    .await;
}

#[tokio::test]
async fn without_transaction_runs_the_action_with_a_non_transactional_status() {
    let value = TransactionTemplate::without_transaction(|status| async move {
        assert!(!status.is_new_transaction());
        assert!(!status.is_rollback_only());
        Ok::<_, TransactionError>(42)
    })
    .await
    .unwrap();
    assert_eq!(value, 42);
}

#[test]
fn resource_holder_support_tracks_deadline_and_references() {
    let holder = ResourceHolderSupport::new();
    assert!(!holder.has_timeout());
    assert!(matches!(holder.time_to_live_in_millis(), Err(TransactionError::InvalidState(_))));

    holder.set_timeout(Duration::from_secs(10));
    assert!(holder.has_timeout());
    let seconds = holder.time_to_live_in_seconds().unwrap();
    assert!((9..=10).contains(&seconds), "{seconds}");
    assert!(!holder.is_rollback_only());

    holder.set_timeout(Duration::ZERO);
    assert!(matches!(holder.time_to_live_in_seconds(), Err(TransactionError::TimedOut(_))));
    assert!(holder.is_rollback_only(), "an expired deadline marks the holder rollback-only");

    assert!(!holder.is_open());
    holder.requested();
    assert!(holder.is_open());
    holder.released();
    assert!(!holder.is_open());

    holder.set_synchronized_with_transaction(true);
    holder.clear();
    assert!(!holder.is_synchronized_with_transaction());
    assert!(!holder.is_rollback_only());
    assert!(!holder.has_timeout());

    assert!(!holder.is_void());
    holder.unbound();
    assert!(holder.is_void());
}

#[test]
fn simple_transaction_status() {
    let status = SimpleTransactionStatus::new();
    assert!(status.is_new_transaction());
    assert!(!status.is_rollback_only());
    status.set_rollback_only().unwrap();
    assert!(status.is_rollback_only());
    status.set_completed();
    assert!(status.is_completed());
    assert!(matches!(status.set_rollback_only(), Err(TransactionError::InvalidState(_))));
}
