USE order_service_keyspace;

CREATE TABLE IF NOT EXISTS failed_order_messages (
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
    PRIMARY KEY ((topic, partition), failed_at, offset)
) WITH CLUSTERING ORDER BY (failed_at DESC, offset DESC);

-- Just for educational purposes
-- There is can be a case with collision on primary key
-- if same consumer fails same message multiple times at the same timestamp
-- The best solution would be to add a uuid or a timeuuid column to guarantee uniqueness
-- but for simplicity we will keep it as is
CREATE TABLE IF NOT EXISTS failed_messages_by_consumer (
    consumer_name text,
    failed_at timestamp,
    topic text,
    partition int,
    offset bigint,
    key text,
    value text,
    error_message text,
    stack_trace text,
    retry_count int,
    PRIMARY KEY (consumer_name, failed_at, topic, partition, offset)
) WITH CLUSTERING ORDER BY (failed_at DESC);

CREATE TABLE IF NOT EXISTS failed_order_messages_by_topic (
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
    PRIMARY KEY (topic, failed_at, consumer_name, partition, offset)
) WITH CLUSTERING ORDER BY (failed_at DESC, consumer_name ASC, partition ASC, offset DESC);

INSERT INTO failed_order_messages (topic, partition, offset, key, value, error_message, stack_trace, retry_count, failed_at, consumer_name)
VALUES ('order-topic', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1, '2024-01-15 10:35:00', 'consumer-1');
INSERT INTO failed_order_messages (topic, partition, offset, key, value, error_message, stack_trace, retry_count, failed_at, consumer_name)
VALUES ('order-topic', 1, 101, 'order-002', '{...}', 'Validation error', 'StackTrace2', 2, '2024-01-16 11:05:00', 'consumer-2');

INSERT INTO failed_messages_by_consumer (consumer_name, failed_at, topic, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES ('consumer-1', '2024-01-15 10:35:00', 'order-topic', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1);
INSERT INTO failed_messages_by_consumer (consumer_name, failed_at, topic, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES ('consumer-2', '2024-01-16 11:05:00', 'order-topic', 1, 101, 'order-002', '{...}', 'Validation error', 'StackTrace2', 2);

INSERT INTO failed_order_messages_by_topic (topic, failed_at, consumer_name, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES ('order-topic', '2024-01-15 10:35:00', 'consumer-1', 0, 100, 'order-001', '{...}', 'Timeout error', 'StackTrace1', 1);
INSERT INTO failed_order_messages_by_topic (topic, failed_at, consumer_name, partition, offset, key, value, error_message, stack_trace, retry_count)
VALUES ('order-topic', '2024-01-16 11:05:00', 'consumer-2', 1, 101, 'order-002', '{...}', 'Validation error', 'StackTrace2', 2);