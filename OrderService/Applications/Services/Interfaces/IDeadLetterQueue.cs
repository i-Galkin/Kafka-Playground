using Confluent.Kafka;

namespace OrderService.Applications.Services.Interfaces
{
    public interface IDeadLetterQueue
    {
        Task SendAsync<TKey, TValue>(ConsumeResult<TKey, TValue> message, Exception exception, int retryCount, CancellationToken cancellationToken = default);
    }
}
