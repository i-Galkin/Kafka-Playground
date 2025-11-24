CREATE TABLE IF NOT EXISTS order_service_keyspace.orders (
    order_id text,
    customer_id text,
    amount decimal,
    created_at timestamp,
    status int,
    processed_at timestamp,
    partition int,
    offset bigint,
    PRIMARY KEY (order_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON order_service_keyspace.orders (customer_id);