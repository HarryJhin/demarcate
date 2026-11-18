use std::fmt;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
pub enum TransactionError {
    CannotCreateTransaction { message: String, source: Option<BoxError> },
    IllegalTransactionState(String),
    InvalidIsolationLevel(String),
    InvalidTimeout { message: String, timeout: i32 },
    NestedTransactionNotSupported(String),
    NoTransaction(String),
    TransactionSuspensionNotSupported(String),
    TransactionSystem { message: String, source: Option<BoxError>, application: Option<BoxError> },
    TransactionTimedOut(String),
    TransactionUsage(String),
    UnexpectedRollback(String),
    HeuristicCompletion(String),
    IllegalState(String),
    Unexpected(BoxError),
}

impl TransactionError {
    pub fn is_transaction_exception(&self) -> bool {
        !matches!(self, Self::IllegalState(_) | Self::Unexpected(_))
    }
    pub fn system(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::TransactionSystem { message: message.into(), source: Some(source.into()), application: None }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CannotCreateTransaction { message, .. }
            | Self::IllegalTransactionState(message)
            | Self::InvalidIsolationLevel(message)
            | Self::InvalidTimeout { message, .. }
            | Self::NestedTransactionNotSupported(message)
            | Self::NoTransaction(message)
            | Self::TransactionSuspensionNotSupported(message)
            | Self::TransactionSystem { message, .. }
            | Self::TransactionTimedOut(message)
            | Self::TransactionUsage(message)
            | Self::UnexpectedRollback(message)
            | Self::HeuristicCompletion(message)
            | Self::IllegalState(message) => f.write_str(message),
            Self::Unexpected(source) => source.fmt(f),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CannotCreateTransaction { source, .. } | Self::TransactionSystem { source, .. } => {
                source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
            }
            Self::Unexpected(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}
