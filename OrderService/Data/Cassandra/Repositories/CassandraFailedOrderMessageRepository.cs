using OrderService.Data.Interfaces;
using OrderService.Domain.Models;

namespace OrderService.Data.Cassandra.Repositories;

public class CassandraFailedOrderMessageRepository : IFailedOrderMessageRepository
{
    private readonly CassandraContext _context;

    public CassandraFailedOrderMessageRepository(CassandraContext context)
    {
        _context = context;
    }

    public async Task Add(FailedOrderMessage message, CancellationToken cancellationToken)
    {
        await _context.Mapper.InsertAsync(message);
    }

    public Task<FailedOrderMessage> GetById(int id, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }

    public async Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages WHERE topic = ?";

        var messages = await _context.Mapper.FetchAsync<FailedOrderMessage>(cql, topic);
        return messages.ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = @"SELECT * FROM failed_order_messages WHERE failed_at >= ? AND failed_at <= ?";

        var failedOrders = await _context.Mapper.FetchAsync<FailedOrderMessage>(cql, from, to);

        return failedOrders.ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByRetryCount(int minRetryCount, CancellationToken cancellationToken)
    {
        const string cql = @"SELECT * FROM failed_order_messages WHERE retry_count >= ?";

        var failedOrders = await _context.Mapper.FetchAsync<FailedOrderMessage>(cql, minRetryCount);

        return failedOrders.ToList();
    }

    public Task UpdateRetryCount(int id, int newRetryCount, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }
}