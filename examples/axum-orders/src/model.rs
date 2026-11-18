use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Deserialize)]
pub struct PlaceOrder {
    pub customer: String,
    pub lines: Vec<PlaceOrderLine>,
    pub coupon: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PlaceOrderLine {
    pub sku: String,
    pub quantity: i32,
}

impl PlaceOrder {
    pub fn validate(&self) -> Result<(), Error> {
        if self.customer.trim().is_empty() {
            return Err(Error::InvalidOrder("customer must not be empty"));
        }
        if self.lines.is_empty() {
            return Err(Error::InvalidOrder("an order needs at least one line"));
        }
        if self.lines.iter().any(|line| line.quantity <= 0) {
            return Err(Error::InvalidOrder("quantity must be positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    Placed,
    Cancelled,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Placed => "placed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "placed" => Ok(Self::Placed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::Internal(format!("unknown order status {other}").into())),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Order {
    pub id: i64,
    pub customer: String,
    pub status: OrderStatus,
    pub subtotal_cents: i64,
    pub discount_cents: i64,
    pub total_cents: i64,
    pub lines: Vec<OrderLine>,
}

#[derive(Debug, Serialize)]
pub struct OrderLine {
    pub sku: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
}
