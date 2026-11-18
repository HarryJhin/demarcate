create table products (
    sku text primary key,
    price_cents bigint not null,
    stock integer not null
);

create table orders (
    id bigint generated always as identity primary key,
    customer text not null,
    status text not null,
    subtotal_cents bigint not null default 0,
    discount_cents bigint not null default 0
);

create table order_lines (
    order_id bigint not null references orders,
    line_no integer not null,
    sku text not null references products,
    quantity integer not null,
    unit_price_cents bigint not null,
    primary key (order_id, line_no)
);

create table coupons (
    code text primary key,
    discount_cents bigint not null
);

create table coupon_redemptions (
    code text primary key references coupons,
    order_id bigint not null references orders
);

create table audit_log (
    id bigint generated always as identity primary key,
    event text not null,
    detail text not null,
    created_at timestamptz not null default now()
);
