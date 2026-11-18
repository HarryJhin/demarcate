//! Handlers hold no transaction code. They call one service function, and the transaction is
//! complete when that function returns.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::error::Error;
use crate::model::{Order, PlaceOrder};
use crate::service::OrderService;

pub fn router(orders: Arc<OrderService>) -> Router {
    Router::new()
        .route("/orders", post(place))
        .route("/orders/{id}", get(find))
        .route("/orders/{id}/cancel", post(cancel))
        .with_state(orders)
}

async fn place(
    State(orders): State<Arc<OrderService>>,
    Json(request): Json<PlaceOrder>,
) -> Result<(StatusCode, Json<Order>), Error> {
    Ok((StatusCode::CREATED, Json(orders.place(request).await?)))
}

async fn find(State(orders): State<Arc<OrderService>>, Path(id): Path<i64>) -> Result<Json<Order>, Error> {
    Ok(Json(orders.find(id).await?))
}

async fn cancel(State(orders): State<Arc<OrderService>>, Path(id): Path<i64>) -> Result<Json<Order>, Error> {
    Ok(Json(orders.cancel(id).await?))
}
