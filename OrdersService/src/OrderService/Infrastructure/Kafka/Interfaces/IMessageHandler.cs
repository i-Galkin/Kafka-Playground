using Confluent.Kafka;

namespace OrderService.Infrastructure.Kafka.Interfaces
{
    public interface IMessageHandler<TKey, TValue>
    {
        Task HandleAsync(ConsumeResult<TKey, TValue> message, CancellationToken cancellationToken);
    }
}
