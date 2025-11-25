USE order_service_keyspace;

DROP TABLE IF EXISTS failed_order_messages_by_topic;
DROP TABLE IF EXISTS failed_order_messages_by_consumer;
DROP TABLE IF EXISTS failed_order_messages;

CREATE TABLE IF NOT EXISTS failed_order_messages (
    id uuid,
    topic text,
    partition int,
    offset bigint,
    key text,
    value text,
    error_message text,
    stack_trace text,
    retry_count int,
    failed_at timestamp,
    consumer_name text,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS failed_order_messages_by_consumer (
    id uuid,
    topic text,
    failed_at timestamp,
    consumer_name text,
    partition int,
    offset bigint,
    key text,
    value text,
    error_message text,
    stack_trace text,
    retry_count int,
    PRIMARY KEY (consumer_name, failed_at, id)
) WITH CLUSTERING ORDER BY (failed_at DESC, id DESC);


CREATE TABLE IF NOT EXISTS failed_order_messages_by_topic (
    id uuid,
    topic text,
    failed_at timestamp,
    consumer_name text,
    partition int,
    offset bigint,
    key text,
    value text,
    error_message text,
    stack_trace text,
    retry_count int,
    PRIMARY KEY (topic, failed_at, id)
) WITH CLUSTERING ORDER BY (failed_at DESC, id DESC);

INSERT INTO failed_order_messages (id, topic, partition, offset, key, value, error_message, stack_trace, retry_count, failed_at, consumer_name)
VALUES (uuid(), 'order-topic', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1, '2024-01-15 10:35:00', 'consumer-1');
INSERT INTO failed_order_messages_by_consumer (id, consumer_name, topic, partition, offset, key, value, error_message, stack_trace, retry_count, failed_at)
VALUES (uuid(), 'consumer-1', 'order-topic', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1, '2024-01-15 10:35:00');
INSERT INTO failed_order_messages_by_topic (id, topic, consumer_name, partition, offset, key, value, error_message, stack_trace, retry_count, failed_at)
VALUES (uuid(), 'order-topic', 'consumer-1', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1, '2024-01-15 10:35:00');
INSERT INTO failed_order_messages (id, topic, partition, offset, key, value, error_message, stack_trace, retry_count, failed_at, consumer_name)
VALUES (uuid(), 'order-topic', 1, 101, 'order-002', '{...}', 'Validation error', 'StackTrace2', 2, '2024-01-16 11:05:00', 'consumer-2');
INSERT INTO failed_order_messages_by_consumer (id, consumer_name, failed_at, topic, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES (uuid(),'consumer-1', '2024-01-15 10:35:00', 'order-topic', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1);
INSERT INTO failed_order_messages_by_consumer (id, consumer_name, failed_at, topic, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES (uuid(),'consumer-2', '2024-01-16 11:05:00', 'order-topic', 1, 101, 'order-002', '{...}', 'Validation error', 'StackTrace2', 2);
INSERT INTO failed_order_messages_by_topic (id, topic, failed_at, consumer_name, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES (uuid(),'order-topic', '2024-01-15 10:35:00', 'consumer-1', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1);
INSERT INTO failed_order_messages_by_topic (id, topic, failed_at, consumer_name, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES (uuid(),'order-topic', '2024-01-16 11:05:00', 'consumer-2', 1, 101, 'order-002', '{...}', 'Validation error', 'StackTrace2', 2);