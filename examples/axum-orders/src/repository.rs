//! Plain SQL. No function here begins, commits or passes a transaction: every query goes to the
//! `TransactionalPool`, which runs it on the connection of the transaction that the calling
//! service function opened.

use demarcate::postgres::TransactionalPool;

use crate::error::Error;
use crate::model::{Order, OrderLine, OrderStatus};

#[derive(Clone)]
pub struct Orders {
    pool: TransactionalPool,
}

impl Orders {
    // `transaction_required` turns a call from outside a transaction into an error instead of
    // a query that commits on its own.
    pub fn new(pool: &TransactionalPool) -> Self {
        Self { pool: pool.transaction_required() }
    }

    pub async fn insert(&self, customer: &str) -> Result<i64, Error> {
        let (id,): (i64,) = sqlx::query_as("insert into orders (customer, status) values ($1, $2) returning id")
            .bind(customer.to_owned())
            .bind(OrderStatus::Placed.as_str())
            .fetch_one(&self.pool)
            .await?;
        Ok(id)
    }

    pub async fn insert_line(&self, order_id: i64, line_no: i32, line: &OrderLine) -> Result<(), Error> {
        sqlx::query(
            "insert into order_lines (order_id, line_no, sku, quantity, unit_price_cents) values ($1, $2, $3, $4, $5)",
        )
        .bind(order_id)
        .bind(line_no)
        .bind(line.sku.clone())
        .bind(line.quantity)
        .bind(line.unit_price_cents)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_amounts(&self, order_id: i64, subtotal_cents: i64, discount_cents: i64) -> Result<(), Error> {
        sqlx::query("update orders set subtotal_cents = $2, discount_cents = $3 where id = $1")
            .bind(order_id)
            .bind(subtotal_cents)
            .bind(discount_cents)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn lock_status(&self, order_id: i64) -> Result<Option<OrderStatus>, Error> {
        let row: Option<(String,)> = sqlx::query_as("select status from orders where id = $1 for update")
            .bind(order_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|(status,)| OrderStatus::parse(&status)).transpose()
    }

    pub async fn set_status(&self, order_id: i64, status: OrderStatus) -> Result<(), Error> {
        sqlx::query("update orders set status = $2 where id = $1")
            .bind(order_id)
            .bind(status.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn find(&self, order_id: i64) -> Result<Option<Order>, Error> {
        let row: Option<(String, String, i64, i64)> =
            sqlx::query_as("select customer, status, subtotal_cents, discount_cents from orders where id = $1")
                .bind(order_id)
                .fetch_optional(&self.pool)
                .await?;
        let Some((customer, status, subtotal_cents, discount_cents)) = row else {
            return Ok(None);
        };
        let lines: Vec<(String, i32, i64)> = sqlx::query_as(
            "select sku, quantity, unit_price_cents from order_lines where order_id = $1 order by line_no",
        )
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(Some(Order {
            id: order_id,
            customer,
            status: OrderStatus::parse(&status)?,
            subtotal_cents,
            discount_cents,
            total_cents: subtotal_cents - discount_cents,
            lines: lines
                .into_iter()
                .map(|(sku, quantity, unit_price_cents)| OrderLine { sku, quantity, unit_price_cents })
                .collect(),
        }))
    }
}

#[derive(Clone)]
pub struct Stock {
    pool: TransactionalPool,
}

impl Stock {
    pub fn new(pool: &TransactionalPool) -> Self {
        Self { pool: pool.transaction_required() }
    }

    /// Takes the quantity from the stock and returns the unit price.
    pub async fn reserve(&self, sku: &str, quantity: i32) -> Result<i64, Error> {
        let reserved: Option<(i64,)> = sqlx::query_as(
            "update products set stock = stock - $2 where sku = $1 and stock >= $2 returning price_cents",
        )
        .bind(sku.to_owned())
        .bind(quantity)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((price_cents,)) = reserved {
            return Ok(price_cents);
        }
        let known: Option<(i32,)> = sqlx::query_as("select 1 from products where sku = $1")
            .bind(sku.to_owned())
            .fetch_optional(&self.pool)
            .await?;
        match known {
            Some(_) => Err(Error::OutOfStock(sku.to_owned())),
            None => Err(Error::UnknownProduct(sku.to_owned())),
        }
    }

    pub async fn release(&self, sku: &str, quantity: i32) -> Result<(), Error> {
        sqlx::query("update products set stock = stock + $2 where sku = $1")
            .bind(sku.to_owned())
            .bind(quantity)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Coupons {
    pool: TransactionalPool,
}

impl Coupons {
    pub fn new(pool: &TransactionalPool) -> Self {
        Self { pool: pool.transaction_required() }
    }

    /// Records the redemption and returns the discount. A coupon that does not exist or that
    /// another order has redeemed violates a constraint, which is reported as `CouponRejected`.
    pub async fn redeem(&self, code: &str, order_id: i64) -> Result<i64, Error> {
        let inserted = sqlx::query("insert into coupon_redemptions (code, order_id) values ($1, $2)")
            .bind(code.to_owned())
            .bind(order_id)
            .execute(&self.pool)
            .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() || error.is_foreign_key_violation() => {
                return Err(Error::CouponRejected);
            }
            Err(error) => return Err(error.into()),
        }
        let (discount_cents,): (i64,) = sqlx::query_as("select discount_cents from coupons where code = $1")
            .bind(code.to_owned())
            .fetch_one(&self.pool)
            .await?;
        Ok(discount_cents)
    }

    pub async fn release(&self, order_id: i64) -> Result<(), Error> {
        sqlx::query("delete from coupon_redemptions where order_id = $1").bind(order_id).execute(&self.pool).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AuditEntries {
    pool: TransactionalPool,
}

impl AuditEntries {
    pub fn new(pool: &TransactionalPool) -> Self {
        Self { pool: pool.transaction_required() }
    }

    pub async fn insert(&self, event: &str, detail: &str) -> Result<(), Error> {
        sqlx::query("insert into audit_log (event, detail) values ($1, $2)")
            .bind(event.to_owned())
            .bind(detail.to_owned())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
