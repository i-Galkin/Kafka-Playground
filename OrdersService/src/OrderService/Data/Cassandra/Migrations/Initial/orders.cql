USE order_service_keyspace;

DROP TABLE IF EXISTS orders_by_customer;
DROP TABLE IF EXISTS orders;

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
);

CREATE TABLE IF NOT EXISTS orders_by_customer (
    customer_id text,
    order_id text,
    created_at timestamp,
    amount decimal,
    status int,
    processed_at timestamp,
    partition int,
    offset bigint,
    PRIMARY KEY (customer_id, order_id)
);

INSERT INTO orders (order_id, customer_id, amount, created_at, status, processed_at, partition, offset)
VALUES ('order-001', 'customer-1', 150.50, '2024-01-15 10:30:00', 1, '2024-01-15 10:30:00', 0, 100);
INSERT INTO orders_by_customer (customer_id, order_id, created_at, amount, status, processed_at, partition, offset)
VALUES ('customer-1', 'order-001', '2024-01-15 10:30:00', 150.50, 1, '2024-01-15 10:30:00', 0, 100);

INSERT INTO orders (order_id, customer_id, amount, created_at, status, processed_at, partition, offset)
VALUES ('order-002', 'customer-2', 200.00, '2024-01-16 11:00:00', 2, '2024-01-16 11:00:00', 1, 101);
INSERT INTO orders_by_customer (customer_id, order_id, created_at, amount, status, processed_at, partition, offset)
VALUES ('customer-2', 'order-002', '2024-01-16 11:00:00', 200.00, 2, '2024-01-16 11:00:00', 1, 101);
