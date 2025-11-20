CREATE TABLE IF NOT EXISTS orders (
    order_id text,
    customer_id text,
    amount decimal,
    created_at timestamp,
    status int,
    processed_at timestamp,
    partition int,
    offset bigint,
    PRIMARY KEY (order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders (customer_id);