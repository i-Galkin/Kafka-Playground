using OrderService.Data.Interfaces;
using OrderService.Domain.Models;
using OrderService.Domain.Models.Mappers;

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

        var byConsumer = message.ToByConsumer();
        await _context.Mapper.InsertAsync(byConsumer);

        var byTopic = message.ToByTopic();
        await _context.Mapper.InsertAsync(byTopic);
    }

    public Task<FailedOrderMessage> GetById(int id, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }

    public Task UpdateRetryCount(int id, int newRetryCount, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }

    public async Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages_by_topic WHERE topic = ?";

        var messages = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, topic);

        return messages.Select(m => m.ToFailedOrderMessage()).ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByConsumerName(string consumerName, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_messages_by_consumer WHERE consumer_name = ?";

        var messages = await _context.Mapper.FetchAsync<FailedMessageByConsumer>(cql, consumerName);

        return messages.Select(m => m.ToFailedOrderMessage()).ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = @"SELECT * FROM failed_order_messages_by_topic WHERE failed_at >= ? AND failed_at <= ? ALLOW FILTERING";

        var failedOrders = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, from, to);

        return failedOrders.Select(m => m.ToFailedOrderMessage()).ToList();
    }

    // TODO:
    public async Task<List<FailedOrderMessage>> GetByRetryCount(int minRetryCount, CancellationToken cancellationToken)
    {
        const string cql = @"SELECT * FROM failed_order_messages";

        var allMessages = await _context.Mapper.FetchAsync<FailedOrderMessage>(cql);

        return allMessages.Where(m => m.RetryCount >= minRetryCount).ToList();
    }
}