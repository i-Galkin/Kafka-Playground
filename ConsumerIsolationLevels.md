# Consumers isolation levels
Kafka provides different isolation levels for consumers to control the visibility of messages based on their transactional state.

# 1. IsolationLevel.ReadUncommitted

The consumer reads all messages from the topic, including those that are part of uncommitted or aborted transactions. This is the default isolation level.

**Characteristics:**
- Lowest latency - messages are visible immediately
- May read uncommitted or aborted transaction data
- No transactional guarantees
- Best for scenarios where speed is prioritized over consistency

# 2. IsolationLevel.ReadCommitted

The consumer only reads messages that are part of committed transactions. Messages from ongoing or aborted transactions are not visible until the transaction commits.

**Characteristics:**
- Higher latency - must wait for transaction commit
- Guarantees only committed data is processed
- Essential for exactly-once processing semantics
- Best for scenarios requiring strong consistency

## Comparison

| Aspect | ReadUncommitted | ReadCommitted |
|--------|-----------------|---------------|
| **Latency** | Lowest | Higher |
| **Consistency** | No guarantees | Strong |
| **Duplicates** | Possible | Avoided with transactions |
| **Use Cases** | Monitoring, logs | Financial, critical data |
| **Default** | Yes | No |

## Additional Notes

- The isolation level affects only consumers reading from topics with transactional producers
- For non-transactional producers, both isolation levels behave identically
- Choose based on your application's tolerance for inconsistent vs. delayed data


