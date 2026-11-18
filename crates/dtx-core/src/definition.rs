pub const TIMEOUT_DEFAULT: i32 = -1;

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

pub trait TransactionDefinition: Send + Sync {
    fn propagation_behavior(&self) -> Propagation {
        Propagation::Required
    }
    fn isolation_level(&self) -> Isolation {
        Isolation::Default
    }
    fn timeout(&self) -> i32 {
        TIMEOUT_DEFAULT
    }
    fn is_read_only(&self) -> bool {
        false
    }
    fn name(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StaticTransactionDefinition;

impl TransactionDefinition for StaticTransactionDefinition {}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DefaultTransactionDefinition {
    pub propagation_behavior: Propagation,
    pub isolation_level: Isolation,
    pub timeout: i32,
    pub read_only: bool,
    pub name: Option<String>,
}

impl DefaultTransactionDefinition {
    pub fn new() -> Self {
        Self { timeout: TIMEOUT_DEFAULT, ..Self::default_fields() }
    }
    pub fn with_propagation(propagation_behavior: Propagation) -> Self {
        Self { propagation_behavior, ..Self::new() }
    }
    pub fn from_definition(other: &dyn TransactionDefinition) -> Self {
        Self {
            propagation_behavior: other.propagation_behavior(),
            isolation_level: other.isolation_level(),
            timeout: other.timeout(),
            read_only: other.is_read_only(),
            name: other.name().map(str::to_owned),
        }
    }
    fn default_fields() -> Self {
        Self {
            propagation_behavior: Propagation::Required,
            isolation_level: Isolation::Default,
            timeout: TIMEOUT_DEFAULT,
            read_only: false,
            name: None,
        }
    }
}

impl TransactionDefinition for DefaultTransactionDefinition {
    fn propagation_behavior(&self) -> Propagation {
        self.propagation_behavior
    }
    fn isolation_level(&self) -> Isolation {
        self.isolation_level
    }
    fn timeout(&self) -> i32 {
        self.timeout
    }
    fn is_read_only(&self) -> bool {
        self.read_only
    }
    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}
