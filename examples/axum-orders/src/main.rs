use demarcate_example_axum_orders::notification::Notifier;
use demarcate_example_axum_orders::{http, migrate, order_service, transactional_pool};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::broadcast::error::RecvError;
use tower::limit::GlobalConcurrencyLimitLayer;

const MAX_CONNECTIONS: u32 = 20;

// Placing an order holds two connections at one point: its own, and the one of the
// `RequiresNew` audit entry. The server therefore handles at most half as many requests at a
// time as the pool has connections; further requests wait for a free slot. The limit is one
// for all routes together.
const MAX_CONCURRENT_REQUESTS: usize = (MAX_CONNECTIONS / 2) as usize;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set")?;
    let address = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());

    let pg_pool = PgPoolOptions::new().max_connections(MAX_CONNECTIONS).connect(&database_url).await?;
    migrate(&pg_pool).await?;

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

    let pool = transactional_pool(pg_pool);
    let router =
        http::router(order_service(&pool, notifier)).layer(GlobalConcurrencyLimitLayer::new(MAX_CONCURRENT_REQUESTS));

    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address, "listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
