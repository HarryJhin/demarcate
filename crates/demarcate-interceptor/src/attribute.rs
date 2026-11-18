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

#[derive(Clone)]
pub struct RollbackRule {
    matcher: Matcher,
    rollback: bool,
}

impl fmt::Debug for RollbackRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RollbackRule")
            .field("exception_pattern", &self.exception_pattern())
            .field("rollback", &self.rollback)
            .finish()
    }
}

impl RollbackRule {
    pub fn rollback_for<E: 'static>() -> Self {
        Self { matcher: Matcher::Type { id: TypeId::of::<E>(), name: type_name::<E>() }, rollback: true }
    }

    pub fn no_rollback_for<E: 'static>() -> Self {
        Self { matcher: Matcher::Type { id: TypeId::of::<E>(), name: type_name::<E>() }, rollback: false }
    }

    pub fn rollback_for_name(pattern: impl Into<String>) -> Self {
        Self::pattern(pattern.into(), true)
    }

    pub fn no_rollback_for_name(pattern: impl Into<String>) -> Self {
        Self::pattern(pattern.into(), false)
    }

    pub fn rollback_if<E: 'static>(test: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        Self::predicate(test, true)
    }

    pub fn no_rollback_if<E: 'static>(test: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        Self::predicate(test, false)
    }

    fn predicate<E: 'static>(test: impl Fn(&E) -> bool + Send + Sync + 'static, rollback: bool) -> Self {
        let test: Predicate = Arc::new(move |error| error.downcast_ref::<E>().is_some_and(&test));
        Self { matcher: Matcher::Predicate { name: type_name::<E>(), test }, rollback }
    }

    fn pattern(pattern: String, rollback: bool) -> Self {
        assert!(!pattern.trim().is_empty(), "'exceptionPattern' cannot be null or empty");
        Self { matcher: Matcher::Pattern(pattern), rollback }
    }

    fn specificity(&self) -> u8 {
        match self.matcher {
            Matcher::Predicate { .. } => 0,
            Matcher::Type { .. } | Matcher::Pattern(_) => 1,
        }
    }

    pub fn is_rollback(&self) -> bool {
        self.rollback
    }

    pub fn exception_pattern(&self) -> &str {
        match &self.matcher {
            Matcher::Type { name, .. } | Matcher::Predicate { name, .. } => name,
            Matcher::Pattern(pattern) => pattern,
        }
    }

    pub fn depth_of<E: 'static>(&self) -> i32 {
        self.depth(None, TypeId::of::<E>(), type_name::<E>())
    }

    pub fn depth_of_value<E: 'static>(&self, error: &E) -> i32 {
        self.depth(Some(error), TypeId::of::<E>(), type_name::<E>())
    }

    pub fn depth(&self, error: Option<&dyn Any>, error_type: TypeId, error_type_name: &str) -> i32 {
        let matched = match &self.matcher {
            Matcher::Type { id, .. } => *id == error_type,
            Matcher::Pattern(pattern) => error_type_name.contains(pattern.as_str()),
            Matcher::Predicate { test, .. } => error.is_some_and(|error| test(error)),
        };
        if matched { 0 } else { -1 }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransactionAttribute {
    pub definition: TransactionDefinition,
    pub rollback_rules: Vec<RollbackRule>,
    pub qualifier: Option<String>,
    pub labels: Vec<String>,
    pub descriptor: Option<String>,
}

impl TransactionAttribute {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_definition(definition: TransactionDefinition) -> Self {
        Self { definition, ..Self::default() }
    }

    pub fn rule(mut self, rule: RollbackRule) -> Self {
        self.rollback_rules.push(rule);
        self
    }

    pub fn qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    pub fn descriptor(mut self, descriptor: impl Into<String>) -> Self {
        self.descriptor = Some(descriptor.into());
        self
    }

    pub fn rollback_on<E: 'static>(&self) -> bool {
        self.rollback_on_type(None, TypeId::of::<E>(), type_name::<E>())
    }

    pub fn rollback_on_value<E: 'static>(&self, error: &E) -> bool {
        self.rollback_on_type(Some(error), TypeId::of::<E>(), type_name::<E>())
    }

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
