use tokio::sync::broadcast;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPlaced {
    pub order_id: i64,
    pub customer: String,
}

/// Hands placed orders to whatever sends the confirmation: a mail sender, a message broker.
#[derive(Clone)]
pub struct Notifier {
    sender: broadcast::Sender<OrderPlaced>,
}

impl Notifier {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(256);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OrderPlaced> {
        self.sender.subscribe()
    }

    pub fn order_placed(&self, event: OrderPlaced) {
        if self.sender.send(event).is_err() {
            tracing::debug!("no subscriber for the order confirmation");
        }
    }
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}
