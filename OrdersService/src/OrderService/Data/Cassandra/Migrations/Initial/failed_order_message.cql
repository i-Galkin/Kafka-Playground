// TODO: Understand the keys
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