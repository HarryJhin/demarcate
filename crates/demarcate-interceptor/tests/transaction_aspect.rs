mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use demarcate_core::{Propagation, TransactionDefinition, TransactionError};
use demarcate_interceptor::{
    RollbackRule, TransactionAttribute, TransactionManagerRegistry, current_transaction_info,
    current_transaction_status, invoke_with_manager,
};
use support::{AppError, CallCountingTransactionManager};

#[tokio::test]
async fn no_transaction() {
    assert!(matches!(current_transaction_status(), Err(TransactionError::NoTransaction(_))));
}

#[tokio::test]
async fn transaction_should_succeed() {
    let tm = CallCountingTransactionManager::new();
    assert!(current_transaction_status().is_err());
    let value = invoke_with_manager(tm.clone(), TransactionAttribute::new(), "test::get_name", async {
        assert!(current_transaction_status().is_ok());
        Ok::<_, AppError>("name")
    })
    .await
    .unwrap();
    assert_eq!(value, "name");
    assert!(current_transaction_status().is_err());
    assert_eq!(tm.counts(), [1, 1, 0]);
    assert_eq!(tm.definition().name.as_deref(), Some("test::get_name"));
}

#[tokio::test]
async fn two_transactions_should_succeed() {
    let tm = CallCountingTransactionManager::new();
    for name in ["test::get_name", "test::set_name"] {
        invoke_with_manager(tm.clone(), TransactionAttribute::new(), name, async { Ok::<_, AppError>(()) })
            .await
            .unwrap();
    }
    assert_eq!(tm.counts(), [2, 2, 0]);
}

#[tokio::test]
async fn enclosing_transaction_with_nested_transaction_on_advised_inside() {
    let tm = CallCountingTransactionManager::new();
    let outer = TransactionAttribute::with_definition(TransactionDefinition::with_propagation(Propagation::Required));
    let inner =
        TransactionAttribute::with_definition(TransactionDefinition::with_propagation(Propagation::RequiresNew));
    let inner_manager = tm.clone();
    invoke_with_manager(tm.clone(), outer, "test::exceptional", async move {
        let info = current_transaction_info().unwrap();
        assert!(info.has_transaction());
        assert_eq!(info.attribute.definition.propagation, Propagation::Required);
        invoke_with_manager(inner_manager, inner, "test::get_spouse", async {
            let info = current_transaction_info().unwrap();
            assert_eq!(info.attribute.definition.propagation, Propagation::RequiresNew);
            assert_eq!(info.joinpoint_identification, "test::get_spouse");
            Ok::<_, AppError>(())
        })
        .await?;
        let info = current_transaction_info().unwrap();
        assert_eq!(info.attribute.definition.propagation, Propagation::Required);
        assert_eq!(info.joinpoint_identification, "test::exceptional");
        Ok::<_, AppError>(())
    })
    .await
    .unwrap();
    assert_eq!(tm.counts(), [2, 2, 0]);
}

async fn do_test_rollback_on_exception(should_rollback: bool, rollback_exception: bool) {
    let tm = CallCountingTransactionManager::new();
    if rollback_exception {
        tm.fail_rollback.store(true, Ordering::SeqCst);
        tm.fail_commit_with_system_exception.store(true, Ordering::SeqCst);
    }
    let attribute = if should_rollback {
        TransactionAttribute::new()
    } else {
        TransactionAttribute::new().rule(RollbackRule::no_rollback_for::<AppError>())
    };
    let result = invoke_with_manager(tm.clone(), attribute, "test::exceptional", async {
        Err::<(), _>(AppError::Business("expected"))
    })
    .await;
    let expected_counts = if should_rollback { [1, 0, 1] } else { [1, 1, 0] };
    assert_eq!(tm.counts(), expected_counts);
    match (rollback_exception, should_rollback, result) {
        (false, _, Err(AppError::Business("expected"))) => {}
        (true, _, Err(AppError::Transaction(TransactionError::System { .. }))) => {}
        (_, _, other) => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn rollback_on_exception() {
    do_test_rollback_on_exception(true, false).await;
}

#[tokio::test]
async fn no_rollback_on_exception() {
    do_test_rollback_on_exception(false, false).await;
}

#[tokio::test]
async fn rollback_on_exception_with_rollback_exception() {
    do_test_rollback_on_exception(true, true).await;
}

#[tokio::test]
async fn no_rollback_on_exception_with_rollback_exception() {
    do_test_rollback_on_exception(false, true).await;
}

#[tokio::test]
async fn programmatic_rollback() {
    let tm = CallCountingTransactionManager::new();
    let value = invoke_with_manager(tm.clone(), TransactionAttribute::new(), "test::get_name", async {
        current_transaction_status()?.set_rollback_only()?;
        Ok::<_, AppError>("name")
    })
    .await
    .unwrap();
    assert_eq!(value, "name");
    assert_eq!(tm.counts(), [1, 1, 0]);
}

#[tokio::test]
async fn cannot_create_transaction() {
    let tm = CallCountingTransactionManager::new();
    tm.fail_begin.store(true, Ordering::SeqCst);
    let invoked = Arc::new(AtomicBool::new(false));
    let flag = invoked.clone();
    let result = invoke_with_manager(tm.clone(), TransactionAttribute::new(), "test::get_name", async move {
        flag.store(true, Ordering::SeqCst);
        Ok::<_, AppError>(())
    })
    .await;
    assert!(matches!(result, Err(AppError::Transaction(TransactionError::CannotCreateTransaction { .. }))));
    assert!(!invoked.load(Ordering::SeqCst));
    assert_eq!(tm.counts(), [0, 0, 0]);
}

#[tokio::test]
async fn cannot_commit_transaction() {
    let tm = CallCountingTransactionManager::new();
    tm.fail_commit.store(true, Ordering::SeqCst);
    let invoked = Arc::new(AtomicBool::new(false));
    let flag = invoked.clone();
    let result = invoke_with_manager(tm.clone(), TransactionAttribute::new(), "test::set_name", async move {
        flag.store(true, Ordering::SeqCst);
        Ok::<_, AppError>(())
    })
    .await;
    assert!(matches!(result, Err(AppError::Transaction(TransactionError::UnexpectedRollback(_)))));
    assert!(invoked.load(Ordering::SeqCst));
}

#[tokio::test]
async fn panic_rolls_back_and_propagates() {
    let tm = CallCountingTransactionManager::new();
    let manager = tm.clone();
    let joined = tokio::spawn(async move {
        invoke_with_manager(manager, TransactionAttribute::new(), "test::panics", async {
            if true {
                panic!("boom");
            }
            Ok::<_, AppError>(())
        })
        .await
    })
    .await;
    assert!(joined.unwrap_err().is_panic());
    assert_eq!(tm.counts(), [1, 0, 1]);
}

#[tokio::test]
async fn panic_publishes_method_rollback_event() {
    let seen: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::default();
    let sink = seen.clone();
    demarcate_interceptor::registry().add_rollback_listener(move |event| {
        if event.method == "test::panic_event" {
            sink.lock().unwrap().push(event.failure);
        }
    });
    let tm = CallCountingTransactionManager::new();
    let joined = tokio::spawn(async move {
        invoke_with_manager(tm, TransactionAttribute::new(), "test::panic_event", async {
            if true {
                panic!("boom");
            }
            Ok::<_, AppError>(())
        })
        .await
    })
    .await;
    assert!(joined.unwrap_err().is_panic());
    assert_eq!(*seen.lock().unwrap(), ["panic"]);
}

#[test]
fn determine_transaction_manager_with_qualifier_unknown() {
    let registry = TransactionManagerRegistry::new();
    let error = registry
        .determine_transaction_manager(&TransactionAttribute::new().qualifier("fooTransactionManager"))
        .err()
        .unwrap();
    assert!(error.to_string().contains("fooTransactionManager"));
}

#[test]
fn determine_transaction_manager_with_qualifier_and_default() {
    let registry = TransactionManagerRegistry::new();
    let default = CallCountingTransactionManager::new();
    let foo = CallCountingTransactionManager::new();
    registry.set_default(default.clone());
    registry.register("fooTransactionManager", foo.clone());
    let chosen = registry
        .determine_transaction_manager(&TransactionAttribute::new().qualifier("fooTransactionManager"))
        .unwrap();
    assert!(std::ptr::addr_eq(Arc::as_ptr(&chosen), Arc::as_ptr(&foo)));
    let chosen = registry.determine_transaction_manager(&TransactionAttribute::new()).unwrap();
    assert!(std::ptr::addr_eq(Arc::as_ptr(&chosen), Arc::as_ptr(&default)));
}

#[test]
fn determine_transaction_manager_with_empty_qualifier_and_default() {
    let registry = TransactionManagerRegistry::new();
    let default = CallCountingTransactionManager::new();
    registry.set_default(default.clone());
    let chosen = registry.determine_transaction_manager(&TransactionAttribute::new().qualifier("")).unwrap();
    assert!(std::ptr::addr_eq(Arc::as_ptr(&chosen), Arc::as_ptr(&default)));
}

#[test]
fn determine_transaction_manager_without_any_manager() {
    let registry = TransactionManagerRegistry::new();
    let error = registry.determine_transaction_manager(&TransactionAttribute::new()).err().unwrap();
    assert!(error.to_string().contains("set_default"));
}

#[tokio::test]
async fn rollback_publishes_method_rollback_event() {
    let seen: Arc<std::sync::Mutex<Vec<(String, &'static str)>>> = Arc::default();
    let sink = seen.clone();
    demarcate_interceptor::registry().add_rollback_listener(move |event| {
        if event.method == "test::publishes_rollback" {
            sink.lock().unwrap().push((event.method.clone(), event.failure));
        }
    });
    let tm = CallCountingTransactionManager::new();
    let result = invoke_with_manager(tm.clone(), TransactionAttribute::new(), "test::publishes_rollback", async {
        Err::<(), _>(AppError::Business("expected"))
    })
    .await;
    assert!(result.is_err());
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].1.ends_with("AppError"));
    }

    let committed = invoke_with_manager(
        tm.clone(),
        TransactionAttribute::new().rule(RollbackRule::no_rollback_for::<AppError>()),
        "test::publishes_rollback",
        async { Err::<(), _>(AppError::Business("expected")) },
    )
    .await;
    assert!(committed.is_err());
    assert_eq!(seen.lock().unwrap().len(), 1, "no event when the transaction commits");
}
