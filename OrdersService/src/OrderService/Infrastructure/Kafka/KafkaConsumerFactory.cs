using Confluent.Kafka;
using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Infrastructure.Kafka
{
    public class KafkaConsumerFactory<TKey, TValue> : IKafkaConsumerFactory<TKey, TValue>
    {
        public IConsumer<TKey, TValue> CreateConsumer(ConsumerConfig config, IDeserializer<TValue> valueDeserializer, ILogger logger)
        {
            var consumerBuilder = new ConsumerBuilder<TKey, TValue>(config)
                .SetValueDeserializer(valueDeserializer)
                .SetErrorHandler((_, error) =>
                {
                    logger.LogError("Kafka error: {Reason}", error.Reason);
                })
                .SetPartitionsAssignedHandler((_, partitions) =>
                {
                    var partitionsList = string.Join(", ", partitions.Select(p => $"P{p.Partition}"));
                    logger.LogInformation("Assigned partitions: {Partitions}", partitionsList);
                })
                .SetPartitionsRevokedHandler((_, partitions) =>
                {
                    var partitionsList = string.Join(", ", partitions.Select(p => $"P{p.Partition}"));
                    logger.LogWarning("Revoked partitions: {Partitions}", partitionsList);
                });

            return consumerBuilder.Build();
        }
    }
}
