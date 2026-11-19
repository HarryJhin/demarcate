use demarcate_example_axum_orders::notification::Notifier;
use demarcate_example_axum_orders::{http, migrate, order_service};
use tokio::sync::broadcast::error::RecvError;
use tower::limit::GlobalConcurrencyLimitLayer;

// Placing an order holds two connections at one point: its own, and the one of the
// `RequiresNew` audit entry. The server therefore handles at most half as many requests at a
// time as the pool has connections; further requests wait for a free slot. The limit is one
// for all routes together.
fn max_concurrent_requests(max_connections: u32) -> usize {
    (max_connections / 2).max(1) as usize
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let address = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());

    let pool = demarcate::postgres::install_from_env().await?;
    migrate(pool.inner()).await?;
    let concurrent_requests = max_concurrent_requests(pool.inner().options().get_max_connections());

    let notifier = Notifier::new();
    let mut placed = notifier.subscribe();
    tokio::spawn(async move {
        loop {
            match placed.recv().await {
                Ok(event) => {
                    tracing::info!(order_id = event.order_id, customer = %event.customer, "sending the order confirmation");
                }
                Err(RecvError::Lagged(missed)) => tracing::warn!(missed, "order confirmations were dropped"),
                Err(RecvError::Closed) => break,
            }
        }
    });

    let router =
        http::router(order_service(&pool, notifier)).layer(GlobalConcurrencyLimitLayer::new(concurrent_requests));

    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address, "listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
