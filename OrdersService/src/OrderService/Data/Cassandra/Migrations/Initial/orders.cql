USE order_service_keyspace;

CREATE TABLE IF NOT EXISTS orders (
     order_id text,
     created_at timestamp,
     customer_id text,
     amount decimal,
     status int,
     processed_at timestamp,
     partition int,
     offset bigint,
     PRIMARY KEY (order_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS orders_by_customer AS
SELECT * FROM orders
WHERE customer_id IS NOT NULL
  AND order_id IS NOT NULL
  AND created_at IS NOT NULL
PRIMARY KEY (customer_id, created_at, order_id)
WITH CLUSTERING ORDER BY (created_at DESC, order_id DESC);



INSERT INTO orders (order_id, customer_id, amount, created_at, status, partition, offset)
VALUES ('order-001', 'customer-1', 150.50, '2024-01-15 10:30:00', 1, 0, 100);
INSERT INTO orders (order_id, customer_id, amount, created_at, status, partition, offset)
VALUES ('order-002', 'customer-2', 200.00, '2024-01-16 11:00:00', 2, 1, 101);

SELECT * FROM orders WHERE order_id = 'order-001';
SELECT * FROM orders_by_customer WHERE customer_id = 'customer-1';