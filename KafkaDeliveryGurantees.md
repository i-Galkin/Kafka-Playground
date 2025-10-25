# Kafka delivery guarantees settings
Kafka provides different delivery guarantees that can be configured based on the requirements of your application. The main delivery guarantees are:

1. **At Most Once**:
   - Messages are delivered zero or one time.
   - There is a possibility of message loss, but no duplicates.
   - This is the fastest delivery guarantee.
   - Configuration in .NET: Set the producer's acknowledgments to "none" (no confirmation from Kafka brokers).

2. **At Least Once**:
   - Messages are delivered one or more times.
   - There is a possibility of duplicate messages, but no message loss (unless there is a failure before the message is acknowledged).
   - This is the default and most commonly used guarantee.
   - Configuration in .NET: Set the producer's acknowledgments to "leader" or "all". The consumer should commit offsets only after successfully processing messages.

3. **Exactly Once**:
   - Messages are delivered exactly one time, with no duplicates and no loss.
   - This is the most reliable but also the slowest and most resource-intensive guarantee.
   - Requires idempotent producers and transactional writes.
   - Configuration in .NET: Enable idempotence and transactions on the producer. The producer must have a unique transactional ID. The consumer should commit offsets as part of the transaction.

## Summary Table

| Guarantee       | Message Loss | Duplicates | .NET Producer Setting(s)                        | Notes                                      |
|-----------------|--------------|------------|-------------------------------------------------|--------------------------------------------|
| At Most Once    | Possible     | No         | Acknowledgments set to "none"                   | Fastest, but may lose messages             |
| At Least Once   | No           | Possible   | Acknowledgments set to "leader" or "all"        | Default, may see duplicates                |
| Exactly Once    | No           | No         | Idempotence and transactions enabled, unique transactional ID | Most reliable, requires extra configuration |

## Additional Notes

- The choice of delivery guarantee depends on the application's tolerance for message loss or duplication.
- For critical data, prefer "Exactly Once" or "At Least Once" with deduplication logic on the consumer side.
- Proper error handling and offset management are essential for achieving the desired guarantee.
- When using transactions in .NET, ensure both producing and offset commits are included in the transaction to maintain exactly-once semantics.