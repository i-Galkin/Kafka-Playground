using Confluent.Kafka;

namespace OrderService.Infrastructure.Kafka.Interfaces
{
    public interface IKafkaConsumerFactory<TKey, TValue>
    {
        IConsumer<TKey, TValue> CreateConsumer(ConsumerConfig config, IDeserializer<TValue> valueDeserializer, ILogger logger);
    }
}
