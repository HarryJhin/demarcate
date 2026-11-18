//! The use cases and their transaction boundaries. Every `#[transactional]` of the application
//! is in this file.

use demarcate::{TransactionPhase, on_transaction, transactional};

use crate::error::Error;
use crate::model::{Order, OrderLine, OrderStatus, PlaceOrder};
use crate::notification::{Notifier, OrderPlaced};
use crate::repository::{AuditEntries, Coupons, Orders, Stock};

pub struct AuditLog {
    entries: AuditEntries,
}

impl AuditLog {
    pub fn new(entries: AuditEntries) -> Self {
        Self { entries }
    }

    // Runs in a transaction of its own, which commits before this function returns. The entry
    // stays when the transaction of the caller rolls back afterwards.
    #[transactional(propagation = RequiresNew)]
    pub async fn record(&self, event: &str, detail: &str) -> Result<(), Error> {
        self.entries.insert(event, detail).await
    }
}

pub struct CouponService {
    coupons: Coupons,
}

impl CouponService {
    pub fn new(coupons: Coupons) -> Self {
        Self { coupons }
    }

    // PostgreSQL aborts the whole transaction when a statement fails. `Nested` runs this
    // function on a savepoint, so a rejected coupon undoes only this function and the caller
    // can go on.
    #[transactional(propagation = Nested)]
    pub async fn redeem(&self, code: &str, order_id: i64) -> Result<i64, Error> {
        self.coupons.redeem(code, order_id).await
    }
}

pub struct OrderService {
    orders: Orders,
    stock: Stock,
    coupons: Coupons,
    coupon_service: CouponService,
    audit: AuditLog,
    notifier: Notifier,
}

impl OrderService {
    pub fn new(
        orders: Orders,
        stock: Stock,
        coupons: Coupons,
        coupon_service: CouponService,
        audit: AuditLog,
        notifier: Notifier,
    ) -> Self {
        Self { orders, stock, coupons, coupon_service, audit, notifier }
    }

    pub async fn place(&self, request: PlaceOrder) -> Result<Order, Error> {
        request.validate()?;
        self.place_valid(request).await
    }

    // One transaction for the order, its lines and the stock. Any `Err` rolls all of it back.
    #[transactional(timeout = 10)]
    async fn place_valid(&self, request: PlaceOrder) -> Result<Order, Error> {
        self.audit.record("order.requested", &request.customer).await?;

        // Two orders that take the same products in a different order would wait for each
        // other's row locks, and PostgreSQL would end one of them with a deadlock error. Taking
        // the stock in the order of the SKU prevents that.
        let mut by_sku: Vec<usize> = (0..request.lines.len()).collect();
        by_sku.sort_by(|&a, &b| request.lines[a].sku.cmp(&request.lines[b].sku));
        let mut unit_prices = vec![0_i64; request.lines.len()];
        for index in by_sku {
            let line = &request.lines[index];
            unit_prices[index] = self.stock.reserve(&line.sku, line.quantity).await?;
        }

        let order_id = self.orders.insert(&request.customer).await?;
        let mut subtotal_cents = 0_i64;
        for ((line_no, line), unit_price_cents) in (1_i32..).zip(&request.lines).zip(unit_prices) {
            let line = OrderLine { sku: line.sku.clone(), quantity: line.quantity, unit_price_cents };
            self.orders.insert_line(order_id, line_no, &line).await?;
            subtotal_cents = unit_price_cents
                .checked_mul(i64::from(line.quantity))
                .and_then(|amount| subtotal_cents.checked_add(amount))
                .ok_or(Error::InvalidOrder("the amount of the order is too large"))?;
        }

        let discount_cents = match &request.coupon {
            Some(code) => match self.coupon_service.redeem(code, order_id).await {
                Ok(discount_cents) => discount_cents.min(subtotal_cents).max(0),
                Err(Error::CouponRejected) => 0,
                Err(error) => return Err(error),
            },
            None => 0,
        };
        self.orders.set_amounts(order_id, subtotal_cents, discount_cents).await?;

        // The confirmation must not go out for an order that is rolled back, so it is sent
        // after the commit. The listener does not touch the database: the transaction is over
        // when it runs.
        let notifier = self.notifier.clone();
        let event = OrderPlaced { order_id, customer: request.customer.clone() };
        on_transaction(TransactionPhase::AfterCommit, false, move || {
            let notifier = notifier.clone();
            let event = event.clone();
            async move {
                notifier.order_placed(event);
                Ok(())
            }
        })
        .await
        .map_err(Error::Internal)?;

        self.orders.find(order_id).await?.ok_or(Error::OrderNotFound)
    }

    // The transaction begins as READ ONLY, so PostgreSQL rejects a write in it.
    #[transactional(read_only)]
    pub async fn find(&self, order_id: i64) -> Result<Order, Error> {
        self.orders.find(order_id).await?.ok_or(Error::OrderNotFound)
    }

    #[transactional]
    pub async fn cancel(&self, order_id: i64) -> Result<Order, Error> {
        match self.orders.lock_status(order_id).await? {
            None => return Err(Error::OrderNotFound),
            Some(OrderStatus::Cancelled) => return Err(Error::AlreadyCancelled),
            Some(OrderStatus::Placed) => {}
        }
        let order = self.orders.find(order_id).await?.ok_or(Error::OrderNotFound)?;
        let mut lines: Vec<&OrderLine> = order.lines.iter().collect();
        lines.sort_by(|a, b| a.sku.cmp(&b.sku));
        for line in lines {
            self.stock.release(&line.sku, line.quantity).await?;
        }
        self.coupons.release(order_id).await?;
        self.orders.set_status(order_id, OrderStatus::Cancelled).await?;
        self.orders.find(order_id).await?.ok_or(Error::OrderNotFound)
    }
}
