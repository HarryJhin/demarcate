use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use demarcate::postgres::TransactionalPool;
use demarcate::{TransactionError, transactional};
use demarcate_example_axum_orders::error::Error;
use demarcate_example_axum_orders::model::{PlaceOrder, PlaceOrderLine};
use demarcate_example_axum_orders::notification::{Notifier, OrderPlaced};
use demarcate_example_axum_orders::repository::Stock;
use demarcate_example_axum_orders::service::OrderService;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::runtime::Runtime;
use tokio::sync::OnceCell;
use tokio::sync::broadcast::Receiver;
use tower::ServiceExt;

struct TestApp {
    router: Router,
    orders: Arc<OrderService>,
    pool: TransactionalPool,
    pg_pool: PgPool,
    notifier: Notifier,
}

// The transaction manager is installed once per process, so the tests share one application
// and one runtime, and each test works on products, coupons and customers of its own.
static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("runtime"));
static APP: OnceCell<TestApp> = OnceCell::const_new();
static SEQUENCE: AtomicU32 = AtomicU32::new(0);

async fn test_app() -> &'static TestApp {
    APP.get_or_init(|| async {
        let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
        let pg_pool = PgPoolOptions::new().max_connections(20).connect(&url).await.expect("connect");
        demarcate_example_axum_orders::migrate(&pg_pool).await.expect("migrations");
        let notifier = Notifier::new();
        let pool = demarcate::postgres::install(pg_pool.clone()).expect("install");
        let orders = demarcate_example_axum_orders::order_service(&pool, notifier.clone());
        let router = demarcate_example_axum_orders::http::router(orders.clone());
        TestApp { router, orders, pool, pg_pool, notifier }
    })
    .await
}

fn unique(prefix: &str) -> String {
    let started = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    format!("{prefix}-{started}-{}", SEQUENCE.fetch_add(1, Ordering::Relaxed))
}

impl TestApp {
    async fn product(&self, price_cents: i64, stock: i32) -> String {
        let sku = unique("sku");
        sqlx::query("insert into products (sku, price_cents, stock) values ($1, $2, $3)")
            .bind(&sku)
            .bind(price_cents)
            .bind(stock)
            .execute(&self.pg_pool)
            .await
            .unwrap();
        sku
    }

    async fn coupon(&self, discount_cents: i64) -> String {
        let code = unique("coupon");
        sqlx::query("insert into coupons (code, discount_cents) values ($1, $2)")
            .bind(&code)
            .bind(discount_cents)
            .execute(&self.pg_pool)
            .await
            .unwrap();
        code
    }

    async fn stock(&self, sku: &str) -> i32 {
        sqlx::query_scalar("select stock from products where sku = $1")
            .bind(sku)
            .fetch_one(&self.pg_pool)
            .await
            .unwrap()
    }

    async fn count(&self, sql: &'static str, value: &str) -> i64 {
        sqlx::query_scalar(sql).bind(value).fetch_one(&self.pg_pool).await.unwrap()
    }

    async fn send(&self, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(uri);
        let request = match body {
            Some(body) => request.header("content-type", "application/json").body(Body::from(body.to_string())),
            None => request.body(Body::empty()),
        }
        .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }
}

fn received_for(events: &mut Receiver<OrderPlaced>, customer: &str) -> Vec<OrderPlaced> {
    let mut received = Vec::new();
    while let Ok(event) = events.try_recv() {
        if event.customer == customer {
            received.push(event);
        }
    }
    received
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn placing_an_order_commits_it_and_sends_the_confirmation() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let customer = unique("customer");
        let book = app.product(1500, 10).await;
        let pen = app.product(200, 5).await;
        let mut events = app.notifier.subscribe();

        let (status, order) = app
            .send(
                "POST",
                "/orders",
                Some(
                    json!({"customer": customer, "lines": [{"sku": book, "quantity": 2}, {"sku": pen, "quantity": 1}]}),
                ),
            )
            .await;

        assert_eq!(status, StatusCode::CREATED, "{order}");
        assert_eq!(order["status"], "placed");
        assert_eq!(order["subtotal_cents"], 3200);
        assert_eq!(order["total_cents"], 3200);
        assert_eq!(order["lines"].as_array().unwrap().len(), 2);
        assert_eq!(app.stock(&book).await, 8);
        assert_eq!(app.stock(&pen).await, 4);
        let order_id = order["id"].as_i64().unwrap();
        assert_eq!(received_for(&mut events, &customer), [OrderPlaced { order_id, customer: customer.clone() }]);

        let (status, found) = app.send("GET", &format!("/orders/{order_id}"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(found, order);
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn an_order_that_fails_is_rolled_back_as_a_whole_and_its_audit_entry_stays() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let customer = unique("customer");
        let book = app.product(1500, 10).await;
        let pen = app.product(200, 1).await;
        let mut events = app.notifier.subscribe();

        let (status, body) = app
            .send(
                "POST",
                "/orders",
                Some(
                    json!({"customer": customer, "lines": [{"sku": book, "quantity": 2}, {"sku": pen, "quantity": 2}]}),
                ),
            )
            .await;

        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(app.stock(&book).await, 10, "the reservation of the first line is rolled back");
        assert_eq!(app.stock(&pen).await, 1);
        assert_eq!(app.count("select count(*) from orders where customer = $1", &customer).await, 0);
        assert_eq!(
            app.count("select count(*) from audit_log where event = 'order.requested' and detail = $1", &customer)
                .await,
            1,
            "the RequiresNew audit entry is committed although the order rolled back"
        );
        assert!(received_for(&mut events, &customer).is_empty(), "no confirmation for a rolled back order");
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn a_rejected_coupon_undoes_only_the_redemption() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let book = app.product(1500, 10).await;
        let coupon = app.coupon(500).await;
        let first_customer = unique("customer");
        let second_customer = unique("customer");
        let place =
            |customer: &str| json!({"customer": customer, "lines": [{"sku": book, "quantity": 1}], "coupon": coupon});

        let (status, first) = app.send("POST", "/orders", Some(place(&first_customer))).await;
        assert_eq!(status, StatusCode::CREATED, "{first}");
        assert_eq!(first["discount_cents"], 500);
        assert_eq!(first["total_cents"], 1000);

        let (status, second) = app.send("POST", "/orders", Some(place(&second_customer))).await;
        assert_eq!(status, StatusCode::CREATED, "the order goes on without the coupon: {second}");
        assert_eq!(second["discount_cents"], 0);
        assert_eq!(second["total_cents"], 1500);
        assert_eq!(app.stock(&book).await, 8);
        assert_eq!(app.count("select count(*) from orders where customer = $1", &second_customer).await, 1);
        assert_eq!(app.count("select count(*) from coupon_redemptions where code = $1", &coupon).await, 1);
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn a_coupon_that_does_not_exist_or_exceeds_the_amount_does_not_fail_the_order() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let book = app.product(1500, 10).await;
        let large = app.coupon(9000).await;

        let (status, unknown) = app
            .send(
                "POST",
                "/orders",
                Some(json!({"customer": unique("customer"), "lines": [{"sku": book, "quantity": 1}], "coupon": unique("none")})),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{unknown}");
        assert_eq!(unknown["discount_cents"], 0);

        let (status, capped) = app
            .send(
                "POST",
                "/orders",
                Some(json!({"customer": unique("customer"), "lines": [{"sku": book, "quantity": 1}], "coupon": large})),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{capped}");
        assert_eq!(capped["discount_cents"], 1500, "the discount is not larger than the amount");
        assert_eq!(capped["total_cents"], 0);
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn orders_that_take_the_same_products_in_opposite_order_do_not_deadlock() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let first = app.product(100, 1000).await;
        let second = app.product(100, 1000).await;
        let order = |skus: [&str; 2]| {
            let body = json!({"customer": unique("customer"), "lines": [{"sku": skus[0], "quantity": 1}, {"sku": skus[1], "quantity": 1}]});
            async move { app.send("POST", "/orders", Some(body)).await.0 }
        };

        for _ in 0..20 {
            let (forward, backward) = tokio::join!(order([&first, &second]), order([&second, &first]));
            assert_eq!((forward, backward), (StatusCode::CREATED, StatusCode::CREATED));
        }
        assert_eq!(app.stock(&first).await, 960);
        assert_eq!(app.stock(&second).await, 960);
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn cancelling_an_order_returns_the_stock_and_frees_the_coupon() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let customer = unique("customer");
        let book = app.product(1500, 10).await;
        let coupon = app.coupon(500).await;
        let (_, order) = app
            .send(
                "POST",
                "/orders",
                Some(json!({"customer": customer, "lines": [{"sku": book, "quantity": 3}], "coupon": coupon})),
            )
            .await;
        let order_id = order["id"].as_i64().unwrap();
        assert_eq!(app.stock(&book).await, 7);

        let (status, cancelled) = app.send("POST", &format!("/orders/{order_id}/cancel"), None).await;
        assert_eq!(status, StatusCode::OK, "{cancelled}");
        assert_eq!(cancelled["status"], "cancelled");
        assert_eq!(app.stock(&book).await, 10);
        assert_eq!(app.count("select count(*) from coupon_redemptions where code = $1", &coupon).await, 0);

        let (status, _) = app.send("POST", &format!("/orders/{order_id}/cancel"), None).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(app.stock(&book).await, 10, "the second cancellation changes nothing");
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn requests_that_cannot_be_served_get_a_client_error() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let customer = unique("customer");

        let (status, _) = app.send("GET", "/orders/0", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = app.send("POST", "/orders/0/cancel", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = app.send("POST", "/orders", Some(json!({"customer": customer, "lines": []}))).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            app.count("select count(*) from audit_log where detail = $1", &customer).await,
            0,
            "an invalid order is refused before a transaction begins"
        );

        let (status, _) = app
            .send(
                "POST",
                "/orders",
                Some(json!({"customer": customer, "lines": [{"sku": unique("missing"), "quantity": 1}]})),
            )
            .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(app.count("select count(*) from orders where customer = $1", &customer).await, 0);
    });
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn a_repository_refuses_to_run_outside_a_transaction() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let book = app.product(1500, 10).await;
        let stock = Stock::new(&app.pool);

        let result = stock.reserve(&book, 1).await;

        let Err(Error::Database(sqlx::Error::Configuration(cause))) = &result else {
            panic!("expected the error of the pool, got {result:?}");
        };
        assert!(
            matches!(cause.downcast_ref::<TransactionError>(), Some(TransactionError::NoTransaction(_))),
            "{cause:?}"
        );
        assert_eq!(app.stock(&book).await, 10);
    });
}

#[transactional]
async fn place_and_then_fail(orders: &OrderService, request: PlaceOrder) -> Result<(), Error> {
    orders.place(request).await?;
    Err(Error::InvalidOrder("a later step of the use case failed"))
}

#[test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
fn an_order_placed_inside_a_transaction_that_rolls_back_sends_no_confirmation() {
    RUNTIME.block_on(async {
        let app = test_app().await;
        let customer = unique("customer");
        let book = app.product(1500, 10).await;
        let mut events = app.notifier.subscribe();
        let request = PlaceOrder {
            customer: customer.clone(),
            lines: vec![PlaceOrderLine { sku: book.clone(), quantity: 1 }],
            coupon: None,
        };

        let result = place_and_then_fail(&app.orders, request).await;

        assert!(matches!(result, Err(Error::InvalidOrder(_))), "{result:?}");
        assert_eq!(
            app.stock(&book).await,
            10,
            "the order joined the transaction of its caller and rolled back with it"
        );
        assert_eq!(app.count("select count(*) from orders where customer = $1", &customer).await, 0);
        assert!(received_for(&mut events, &customer).is_empty());
    });
}
