pub mod error;
pub mod http;
pub mod model;
pub mod notification;
pub mod repository;
pub mod service;

use std::sync::Arc;

use demarcate::postgres::{TransactionalPool, manager};
use sqlx::PgPool;

use crate::notification::Notifier;
use crate::repository::{AuditEntries, Coupons, Orders, Stock};
use crate::service::{AuditLog, CouponService, OrderService};

pub async fn migrate(pg_pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!().run(pg_pool).await
}

/// Creates the pool that the repositories use and registers its transaction manager. Call it
/// once: every `#[transactional]` function of the process uses this manager.
pub fn transactional_pool(pg_pool: PgPool) -> TransactionalPool {
    let pool = TransactionalPool::new(pg_pool);
    demarcate::registry().set_default(Arc::new(manager(pool.clone())));
    pool
}

pub fn order_service(pool: &TransactionalPool, notifier: Notifier) -> Arc<OrderService> {
    let coupons = Coupons::new(pool);
    Arc::new(OrderService::new(
        Orders::new(pool),
        Stock::new(pool),
        coupons.clone(),
        CouponService::new(coupons),
        AuditLog::new(AuditEntries::new(pool)),
        notifier,
    ))
}
