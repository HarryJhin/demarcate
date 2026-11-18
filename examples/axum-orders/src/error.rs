use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use demarcate::{BoxError, TransactionError};

#[derive(Debug)]
pub enum Error {
    InvalidOrder(&'static str),
    UnknownProduct(String),
    OutOfStock(String),
    CouponRejected,
    OrderNotFound,
    AlreadyCancelled,
    Transaction(TransactionError),
    Database(sqlx::Error),
    Internal(BoxError),
}

// A transactional function returns the failure to begin, commit or roll back through this.
impl From<TransactionError> for Error {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::InvalidOrder(reason) => (StatusCode::UNPROCESSABLE_ENTITY, (*reason).to_owned()),
            Self::UnknownProduct(sku) => (StatusCode::UNPROCESSABLE_ENTITY, format!("unknown product {sku}")),
            Self::OutOfStock(sku) => (StatusCode::CONFLICT, format!("not enough stock of {sku}")),
            Self::CouponRejected => (StatusCode::CONFLICT, "the coupon cannot be used".to_owned()),
            Self::OrderNotFound => (StatusCode::NOT_FOUND, "order not found".to_owned()),
            Self::AlreadyCancelled => (StatusCode::CONFLICT, "the order is already cancelled".to_owned()),
            Self::Transaction(_) | Self::Database(_) | Self::Internal(_) => {
                tracing::error!(error = ?self, "request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_owned())
            }
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

#[derive(serde::Serialize)]
struct ErrorBody {
    error: String,
}
