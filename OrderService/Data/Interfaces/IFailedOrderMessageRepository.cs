using OrderService.Domain.Models;

namespace OrderService.Data.Interfaces;

public interface IFailedOrderMessageRepository
{
    Task Add(FailedOrderMessage message, CancellationToken cancellationToken);
    Task<FailedOrderMessage> GetById(int id, CancellationToken cancellationToken);
    Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken);
    Task<List<FailedOrderMessage>> GetByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken);
    Task<List<FailedOrderMessage>> GetByRetryCount(int minRetryCount, CancellationToken cancellationToken);
    Task UpdateRetryCount(int id, int newRetryCount, CancellationToken cancellationToken);
}