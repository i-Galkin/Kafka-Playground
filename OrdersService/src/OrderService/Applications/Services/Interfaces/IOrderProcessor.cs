using Confluent.Kafka;
using OrderService.Domain.Messages;

namespace OrderService.Applications.Services.Interfaces
{
    public interface IOrderProcessor
    {
        Task ProcessAsync(ConsumeResult<string, OrderMessage> message, CancellationToken cancellationToken = default);
    }
}
