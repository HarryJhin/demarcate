mod support;

use demarcate_interceptor::{RollbackRule, TransactionAttribute};
use support::{AppError, OtherError};

#[test]
fn default_rule() {
    let attribute = TransactionAttribute::new();
    assert!(attribute.rollback_on::<AppError>());
    assert!(attribute.rollback_on::<OtherError>());
    assert!(attribute.rollback_on::<std::io::Error>());
}

#[test]
fn rule_for_rollback_on_named_error() {
    let attribute = TransactionAttribute::new().rule(RollbackRule::rollback_for_name("io::error::Error"));
    assert!(attribute.rollback_on::<std::io::Error>());
    assert!(attribute.rollback_on::<AppError>());
}

#[test]
fn rule_for_commit_on_error() {
    let attribute = TransactionAttribute::new()
        .rule(RollbackRule::no_rollback_for_name("AppError"))
        .rule(RollbackRule::rollback_for_name("io::error::Error"));
    assert!(!attribute.rollback_on::<AppError>());
    assert!(attribute.rollback_on::<OtherError>());
    assert!(attribute.rollback_on::<std::io::Error>());
}

#[test]
fn rule_for_selective_commit_with_type() {
    let attribute = TransactionAttribute::new().rule(RollbackRule::no_rollback_for::<AppError>());
    assert!(!attribute.rollback_on::<AppError>());
    assert!(attribute.rollback_on::<OtherError>());
}

#[test]
fn conflicting_rules_first_rule_wins_at_equal_depth() {
    let rollback_first = TransactionAttribute::new()
        .rule(RollbackRule::rollback_for::<AppError>())
        .rule(RollbackRule::no_rollback_for::<AppError>());
    assert!(rollback_first.rollback_on::<AppError>());
    let commit_first = TransactionAttribute::new()
        .rule(RollbackRule::no_rollback_for::<AppError>())
        .rule(RollbackRule::rollback_for::<AppError>());
    assert!(!commit_first.rollback_on::<AppError>());
}

#[test]
fn not_found() {
    assert_eq!(RollbackRule::rollback_for_name("NoSuchError").depth_of::<AppError>(), -1);
    assert_eq!(RollbackRule::rollback_for::<OtherError>().depth_of::<AppError>(), -1);
}

#[test]
fn found_immediately_when_direct_match() {
    assert_eq!(RollbackRule::rollback_for::<AppError>().depth_of::<AppError>(), 0);
}

#[test]
fn found_immediately_when_name_of_error_contains_pattern() {
    assert_eq!(RollbackRule::rollback_for_name("support::App").depth_of::<AppError>(), 0);
}

#[test]
fn type_rule_does_not_match_error_with_similar_name() {
    assert_eq!(RollbackRule::rollback_for::<std::fmt::Error>().depth_of::<std::io::Error>(), -1);
}

#[test]
#[should_panic(expected = "must not be empty")]
fn constructor_preconditions() {
    RollbackRule::rollback_for_name("  ");
}

#[test]
fn type_rule_pattern_is_the_type_name() {
    assert!(RollbackRule::rollback_for::<AppError>().exception_pattern().ends_with("AppError"));
}

#[test]
fn predicate_rule_decides_on_the_error_value() {
    let attribute = TransactionAttribute::new()
        .rule(RollbackRule::no_rollback_if(|error: &AppError| matches!(error, AppError::Business("warning"))));
    assert!(!attribute.rollback_on_value(&AppError::Business("warning")));
    assert!(attribute.rollback_on_value(&AppError::Business("failed")));
    assert!(attribute.rollback_on_value(&OtherError::Failed));
    assert!(attribute.rollback_on::<AppError>(), "without a value a predicate rule does not match");
}

#[test]
fn predicate_rule_can_inspect_the_source_chain() {
    #[derive(Debug)]
    struct Wrapped(std::io::Error);
    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("wrapped")
        }
    }
    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }
    let attribute = TransactionAttribute::new().rule(RollbackRule::no_rollback_if(|error: &Wrapped| {
        demarcate_interceptor::error_chain_contains::<std::io::Error>(error)
    }));
    assert!(!attribute.rollback_on_value(&Wrapped(std::io::Error::other("disk"))));
    assert!(!demarcate_interceptor::error_chain_contains::<std::fmt::Error>(&Wrapped(std::io::Error::other("disk"))));
}
