use std::any::{TypeId, type_name};

use demarcate_core::TransactionDefinition;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Matcher {
    Type { id: TypeId, name: &'static str },
    Pattern(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackRule {
    matcher: Matcher,
    rollback: bool,
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

    fn pattern(pattern: String, rollback: bool) -> Self {
        assert!(!pattern.trim().is_empty(), "'exceptionPattern' cannot be null or empty");
        Self { matcher: Matcher::Pattern(pattern), rollback }
    }

    pub fn is_rollback(&self) -> bool {
        self.rollback
    }

    pub fn exception_pattern(&self) -> &str {
        match &self.matcher {
            Matcher::Type { name, .. } => name,
            Matcher::Pattern(pattern) => pattern,
        }
    }

    pub fn depth_of<E: 'static>(&self) -> i32 {
        self.depth(TypeId::of::<E>(), type_name::<E>())
    }

    pub fn depth(&self, error_type: TypeId, error_type_name: &str) -> i32 {
        let matched = match &self.matcher {
            Matcher::Type { id, .. } => *id == error_type,
            Matcher::Pattern(pattern) => error_type_name.contains(pattern.as_str()),
        };
        if matched { 0 } else { -1 }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
        self.rollback_on_type(TypeId::of::<E>(), type_name::<E>())
    }

    pub fn rollback_on_type(&self, error_type: TypeId, error_type_name: &str) -> bool {
        let mut winner: Option<&RollbackRule> = None;
        let mut deepest = i32::MAX;
        for rule in &self.rollback_rules {
            let depth = rule.depth(error_type, error_type_name);
            if depth >= 0 && depth < deepest {
                deepest = depth;
                winner = Some(rule);
            }
        }
        winner.is_none_or(RollbackRule::is_rollback)
    }
}
