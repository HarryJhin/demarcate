#[path = "../../demarcate-interceptor/tests/support/mod.rs"]
mod support;

use std::time::Duration;

use demarcate::{Isolation, Propagation, TransactionError, current_transaction_status, registry, transactional};
use support::{AppError, CallCountingTransactionManager, OtherError};

#[transactional(transaction_manager = "macro-commit")]
async fn commits() -> Result<&'static str, AppError> {
    assert!(current_transaction_status().is_ok());
    Ok("done")
}

#[tokio::test]
async fn with_commit() {
    let tm = CallCountingTransactionManager::new();
    registry().register("macro-commit", tm.clone());
    assert_eq!(commits().await.unwrap(), "done");
    assert_eq!(tm.counts(), [1, 1, 0]);
    assert_eq!(tm.definition().name.as_deref(), Some("transactional_macro::commits"));
}

#[transactional(transaction_manager = "macro-rollback")]
async fn fails() -> Result<(), AppError> {
    Err(AppError::Business("failed"))
}

#[tokio::test]
async fn with_rollback_on_error() {
    let tm = CallCountingTransactionManager::new();
    registry().register("macro-rollback", tm.clone());
    assert!(matches!(fails().await, Err(AppError::Business("failed"))));
    assert_eq!(tm.counts(), [1, 0, 1]);
}

#[transactional(transaction_manager = "macro-no-rollback", no_rollback_for = [AppError])]
async fn fails_without_rollback() -> Result<(), AppError> {
    Err(AppError::Business("warning"))
}

#[tokio::test]
async fn with_commit_on_error_and_no_rollback_rule() {
    let tm = CallCountingTransactionManager::new();
    registry().register("macro-no-rollback", tm.clone());
    assert!(matches!(fails_without_rollback().await, Err(AppError::Business("warning"))));
    assert_eq!(tm.counts(), [1, 1, 0]);
}

#[transactional(transaction_manager = "macro-other-rule", no_rollback_for = [OtherError])]
async fn fails_with_unrelated_rule() -> Result<(), AppError> {
    Err(AppError::Business("failed"))
}

#[tokio::test]
async fn with_rollback_when_rule_names_another_error() {
    let tm = CallCountingTransactionManager::new();
    registry().register("macro-other-rule", tm.clone());
    assert!(fails_with_unrelated_rule().await.is_err());
    assert_eq!(tm.counts(), [1, 0, 1]);
}

#[transactional(
    transaction_manager = "macro-attributes",
    propagation = RequiresNew,
    isolation = Serializable,
    timeout = 30,
    read_only,
    label = ["reporting"]
)]
async fn with_attributes(value: &str) -> Result<usize, AppError> {
    let info = demarcate::current_transaction_info().expect("transaction info");
    assert_eq!(info.attribute.labels, ["reporting"]);
    Ok(value.len())
}

#[tokio::test]
async fn attributes_reach_the_transaction_manager() {
    let tm = CallCountingTransactionManager::new();
    registry().register("macro-attributes", tm.clone());
    assert_eq!(with_attributes("abc").await.unwrap(), 3);
    let definition = tm.definition();
    assert_eq!(definition.propagation, Propagation::RequiresNew);
    assert_eq!(definition.isolation, Isolation::Serializable);
    assert_eq!(definition.timeout, Some(Duration::from_secs(30)));
    assert!(definition.read_only);
}

struct Service {
    prefix: String,
}

impl Service {
    #[transactional(transaction_manager = "macro-method")]
    async fn greet(&self, name: &str) -> Result<String, AppError> {
        if name.is_empty() {
            return Err(AppError::Business("empty"));
        }
        Ok(format!("{} {name}", self.prefix))
    }
}

#[tokio::test]
async fn works_on_methods_with_borrowed_arguments_and_early_return() {
    let tm = CallCountingTransactionManager::new();
    registry().register("macro-method", tm.clone());
    let service = Service { prefix: "hello".into() };
    assert_eq!(service.greet("rust").await.unwrap(), "hello rust");
    assert!(service.greet("").await.is_err());
    assert_eq!(tm.counts(), [2, 1, 1]);
}

#[transactional(transaction_manager = "macro-unknown")]
async fn unknown_manager() -> Result<(), AppError> {
    panic!("must not be invoked");
}

#[tokio::test]
async fn unknown_transaction_manager_is_reported_before_the_body_runs() {
    match unknown_manager().await {
        Err(AppError::Transaction(TransactionError::InvalidState(message))) => {
            assert!(message.contains("macro-unknown"), "{message}");
        }
        other => panic!("unexpected result: {other:?}"),
    }
}
