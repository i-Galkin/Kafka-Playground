using OrderService.Domain.Models;

namespace OrderService.Data.Interfaces;

public interface IFailedOrderMessageRepository
{
    Task Add(FailedOrderMessage message, CancellationToken cancellationToken);
    Task<FailedOrderMessage> GetById(Guid id, CancellationToken cancellationToken);
    Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken);
    Task<List<FailedOrderMessage>> GetByConsumerName(string consumerName, CancellationToken cancellationToken);
    Task<List<FailedOrderMessage>> GetByDateRange(string consumerName, DateTime from, DateTime to, CancellationToken cancellationToken);
}