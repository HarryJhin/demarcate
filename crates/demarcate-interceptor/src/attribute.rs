use std::any::{Any, TypeId, type_name};
use std::fmt;
use std::sync::Arc;

use demarcate_core::TransactionDefinition;

type Predicate = Arc<dyn Fn(&dyn Any) -> bool + Send + Sync>;

#[derive(Clone)]
enum Matcher {
    Type { id: TypeId, name: &'static str },
    Pattern(String),
    Predicate { name: &'static str, test: Predicate },
}

/// A rule that decides, for one kind of error, whether a transactional function that returns it
/// rolls back or commits.
///
/// A rule matches an error by its type ([`rollback_for`](RollbackRule::rollback_for)), by a part
/// of its type name ([`rollback_for_name`](RollbackRule::rollback_for_name)), or by a test of
/// its value ([`rollback_if`](RollbackRule::rollback_if)). The `no_rollback_*` constructors
/// create rules that commit.
///
/// Without rules, every error rolls back. When several rules match, a rule that tests the value
/// wins over a rule that names the type, and among equals the first one wins.
///
/// In `#[transactional]` the rules are written as `rollback_for = [E]`, `no_rollback_for = [E]`,
/// `rollback_for_name = "..."`, `rollback_if = test` and their counterparts.
#[derive(Clone)]
pub struct RollbackRule {
    matcher: Matcher,
    rollback: bool,
}

impl fmt::Debug for RollbackRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RollbackRule")
            .field("error_pattern", &self.error_pattern())
            .field("rollback", &self.rollback)
            .finish()
    }
}

impl RollbackRule {
    /// Creates a rule that rolls back when the error type of the function is `E`.
    pub fn rollback_for<E: 'static>() -> Self {
        Self { matcher: Matcher::Type { id: TypeId::of::<E>(), name: type_name::<E>() }, rollback: true }
    }

    /// Creates a rule that commits when the error type of the function is `E`.
    pub fn no_rollback_for<E: 'static>() -> Self {
        Self { matcher: Matcher::Type { id: TypeId::of::<E>(), name: type_name::<E>() }, rollback: false }
    }

    /// Creates a rule that rolls back when the name of the error type contains the given text.
    ///
    /// The name is the full path of the type, as [`std::any::type_name`] gives it. Note that a short
    /// pattern matches every type whose path contains it.
    ///
    /// # Panics
    ///
    /// Panics when the pattern is empty.
    pub fn rollback_for_name(pattern: impl Into<String>) -> Self {
        Self::pattern(pattern.into(), true)
    }

    /// Creates a rule that commits when the name of the error type contains the given text.
    ///
    /// # Panics
    ///
    /// Panics when the pattern is empty.
    pub fn no_rollback_for_name(pattern: impl Into<String>) -> Self {
        Self::pattern(pattern.into(), false)
    }

    /// Creates a rule that rolls back when the test returns `true` for the error value.
    ///
    /// Use this to decide by the variant of an error enum, or by the cause of an error; see
    /// [`error_chain_contains`].
    pub fn rollback_if<E: 'static>(test: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        Self::predicate(test, true)
    }

    /// Creates a rule that commits when the test returns `true` for the error value.
    pub fn no_rollback_if<E: 'static>(test: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        Self::predicate(test, false)
    }

    fn predicate<E: 'static>(test: impl Fn(&E) -> bool + Send + Sync + 'static, rollback: bool) -> Self {
        let test: Predicate = Arc::new(move |error| error.downcast_ref::<E>().is_some_and(&test));
        Self { matcher: Matcher::Predicate { name: type_name::<E>(), test }, rollback }
    }

    fn pattern(pattern: String, rollback: bool) -> Self {
        assert!(!pattern.trim().is_empty(), "the pattern of a rollback rule must not be empty");
        Self { matcher: Matcher::Pattern(pattern), rollback }
    }

    fn specificity(&self) -> u8 {
        match self.matcher {
            Matcher::Predicate { .. } => 0,
            Matcher::Type { .. } | Matcher::Pattern(_) => 1,
        }
    }

    /// Returns whether this rule rolls back, as opposed to commits, when it matches.
    pub fn is_rollback(&self) -> bool {
        self.rollback
    }

    /// Returns the text that names the errors this rule matches.
    ///
    /// For a rule made with [`rollback_for_name`](RollbackRule::rollback_for_name) or
    /// [`no_rollback_for_name`](RollbackRule::no_rollback_for_name) this is the pattern. For every
    /// other rule it is the full type name of the error, as [`std::any::type_name`] gives it.
    pub fn error_pattern(&self) -> &str {
        match &self.matcher {
            Matcher::Type { name, .. } | Matcher::Predicate { name, .. } => name,
            Matcher::Pattern(pattern) => pattern,
        }
    }

    /// Returns `0` when this rule matches the error type `E`, and `-1` when it does not.
    ///
    /// A rule that tests the value never matches here, because there is no value.
    pub fn depth_of<E: 'static>(&self) -> i32 {
        self.depth(None, TypeId::of::<E>(), type_name::<E>())
    }

    /// Returns `0` when this rule matches the given error, and `-1` when it does not.
    pub fn depth_of_value<E: 'static>(&self, error: &E) -> i32 {
        self.depth(Some(error), TypeId::of::<E>(), type_name::<E>())
    }

    /// Returns `0` when this rule matches an error of the given type, and `-1` when it does not.
    ///
    /// `error` is the value, when one is available for rules that test it.
    pub fn depth(&self, error: Option<&dyn Any>, error_type: TypeId, error_type_name: &str) -> i32 {
        let matched = match &self.matcher {
            Matcher::Type { id, .. } => *id == error_type,
            Matcher::Pattern(pattern) => error_type_name.contains(pattern.as_str()),
            Matcher::Predicate { test, .. } => error.is_some_and(|error| test(error)),
        };
        if matched { 0 } else { -1 }
    }
}

/// The transaction settings of one function: the definition, the rollback rules and the
/// transaction manager to use.
///
/// `#[transactional]` builds an attribute from its arguments. Build one by hand to call
/// [`invoke_within_transaction`](crate::invoke_within_transaction) directly.
#[derive(Debug, Clone, Default)]
pub struct TransactionAttribute {
    /// The propagation, isolation, timeout, read-only flag and name of the transaction.
    pub definition: TransactionDefinition,
    /// The rules that decide which errors roll back. Empty means that every error rolls back.
    pub rollback_rules: Vec<RollbackRule>,
    /// The name under which the transaction manager is registered, or `None` for the default manager.
    pub qualifier: Option<String>,
    /// Free-form labels for this function. The library does not interpret them; a custom transaction
    /// manager or listener can.
    pub labels: Vec<String>,
    /// A description of the function. When set, it replaces the path of the function as the name of
    /// the transaction and in [`FunctionRollbackEvent`](crate::FunctionRollbackEvent).
    pub descriptor: Option<String>,
}

impl TransactionAttribute {
    /// Creates an attribute with the default definition, no rules and the default manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an attribute with the given definition.
    pub fn with_definition(definition: TransactionDefinition) -> Self {
        Self { definition, ..Self::default() }
    }

    /// Adds a rollback rule.
    pub fn rule(mut self, rule: RollbackRule) -> Self {
        self.rollback_rules.push(rule);
        self
    }

    /// Sets the name of the transaction manager to use.
    pub fn qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    /// Adds a label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    /// Sets the description of the function.
    pub fn descriptor(mut self, descriptor: impl Into<String>) -> Self {
        self.descriptor = Some(descriptor.into());
        self
    }

    /// Returns whether an error of type `E` rolls back, judged by the rules that do not need the
    /// value.
    pub fn rollback_on<E: 'static>(&self) -> bool {
        self.rollback_on_type(None, TypeId::of::<E>(), type_name::<E>())
    }

    /// Returns whether the given error rolls back.
    pub fn rollback_on_value<E: 'static>(&self, error: &E) -> bool {
        self.rollback_on_type(Some(error), TypeId::of::<E>(), type_name::<E>())
    }

    /// Returns whether an error of the given type rolls back.
    ///
    /// The winning rule decides; see [`RollbackRule`] for the order. When no rule matches, the
    /// answer is `true`.
    pub fn rollback_on_type(&self, error: Option<&dyn Any>, error_type: TypeId, error_type_name: &str) -> bool {
        let mut winner: Option<&RollbackRule> = None;
        let mut deepest = (i32::MAX, u8::MAX);
        for rule in &self.rollback_rules {
            let depth = rule.depth(error, error_type, error_type_name);
            if depth >= 0 && (depth, rule.specificity()) < deepest {
                deepest = (depth, rule.specificity());
                winner = Some(rule);
            }
        }
        winner.is_none_or(RollbackRule::is_rollback)
    }
}

/// Returns whether the error, or one of its sources, is of type `T`.
///
/// Use this in a [`RollbackRule::rollback_if`] test to decide by the cause of an error.
pub fn error_chain_contains<T: std::error::Error + 'static>(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<T>() {
            return true;
        }
        current = error.source();
    }
    false
}
