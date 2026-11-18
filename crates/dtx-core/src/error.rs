use std::fmt;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
pub enum TransactionError {
    CannotCreateTransaction { message: String, source: Option<BoxError> },
    CannotGetConnection { message: String, source: Option<BoxError> },
    InvalidTransactionState(String),
    InvalidIsolationLevel(String),
    NestedNotSupported(String),
    NoTransaction(String),
    SuspensionNotSupported(String),
    System { message: String, source: Option<BoxError>, application: Option<BoxError> },
    TimedOut(String),
    Usage(String),
    UnexpectedRollback(String),
    HeuristicCompletion(String),
    InvalidState(String),
    Other(BoxError),
}

impl TransactionError {
    pub fn is_transaction_error(&self) -> bool {
        !matches!(self, Self::InvalidState(_) | Self::CannotGetConnection { .. } | Self::Other(_))
    }
    pub fn system(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::System { message: message.into(), source: Some(source.into()), application: None }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CannotCreateTransaction { message, .. }
            | Self::CannotGetConnection { message, .. }
            | Self::InvalidTransactionState(message)
            | Self::InvalidIsolationLevel(message)
            | Self::NestedNotSupported(message)
            | Self::NoTransaction(message)
            | Self::SuspensionNotSupported(message)
            | Self::System { message, .. }
            | Self::TimedOut(message)
            | Self::Usage(message)
            | Self::UnexpectedRollback(message)
            | Self::HeuristicCompletion(message)
            | Self::InvalidState(message) => f.write_str(message),
            Self::Other(source) => source.fmt(f),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CannotCreateTransaction { source, .. }
            | Self::CannotGetConnection { source, .. }
            | Self::System { source, .. } => {
                source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
            }
            Self::Other(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}
