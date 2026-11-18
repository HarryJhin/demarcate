use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Propagation {
    #[default]
    Required,
    Supports,
    Mandatory,
    RequiresNew,
    NotSupported,
    Never,
    Nested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Isolation {
    #[default]
    Default,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransactionDefinition {
    pub propagation: Propagation,
    pub isolation: Isolation,
    pub timeout: Option<Duration>,
    pub read_only: bool,
    pub name: Option<String>,
}

impl TransactionDefinition {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_propagation(propagation: Propagation) -> Self {
        Self { propagation, ..Self::default() }
    }
    pub fn propagation(mut self, propagation: Propagation) -> Self {
        self.propagation = propagation;
        self
    }
    pub fn isolation(mut self, isolation: Isolation) -> Self {
        self.isolation = isolation;
        self
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}
