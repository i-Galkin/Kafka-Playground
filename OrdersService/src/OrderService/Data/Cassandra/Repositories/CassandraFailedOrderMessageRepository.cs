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

        var byConsumer = new FailedMessageByConsumer
        {
            ConsumerName = message.ConsumerName,
            FailedAt = message.FailedAt,
            Topic = message.Topic,
            Partition = message.Partition,
            Offset = message.Offset,
            Key = message.Key,
            Value = message.Value,
            ErrorMessage = message.ErrorMessage,
            StackTrace = message.StackTrace,
            RetryCount = message.RetryCount
        };
        await _context.Mapper.InsertAsync(byConsumer);

        // Insert into topic table for queries by topic
        var byTopic = new FailedMessageByTopic
        {
            Topic = message.Topic,
            FailedAt = message.FailedAt,
            ConsumerName = message.ConsumerName,
            Partition = message.Partition,
            Offset = message.Offset,
            Key = message.Key,
            Value = message.Value,
            ErrorMessage = message.ErrorMessage,
            StackTrace = message.StackTrace,
            RetryCount = message.RetryCount
        };
        await _context.Mapper.InsertAsync(byTopic);
    }

    public Task<FailedOrderMessage> GetById(int id, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }

    public async Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages_by_topic WHERE topic = ?";

        var messages = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, topic);

        return messages.Select(m => new FailedOrderMessage
        {
            Topic = m.Topic,
            Partition = m.Partition,
            Offset = m.Offset,
            Key = m.Key,
            Value = m.Value,
            ErrorMessage = m.ErrorMessage,
            StackTrace = m.StackTrace,
            RetryCount = m.RetryCount,
            FailedAt = m.FailedAt,
            ConsumerName = m.ConsumerName
        }).ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = @"SELECT * FROM failed_order_messages_by_topic WHERE failed_at >= ? AND failed_at <= ? ALLOW FILTERING";

        var failedOrders = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, from, to);

        return failedOrders.Select(m => new FailedOrderMessage
        {
            Topic = m.Topic,
            Partition = m.Partition,
            Offset = m.Offset,
            Key = m.Key,
            Value = m.Value,
            ErrorMessage = m.ErrorMessage,
            StackTrace = m.StackTrace,
            RetryCount = m.RetryCount,
            FailedAt = m.FailedAt,
            ConsumerName = m.ConsumerName
        }).ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByRetryCount(int minRetryCount, CancellationToken cancellationToken)
    {
        const string cql = @"SELECT * FROM failed_order_messages_by_topic WHERE retry_count >= ? ALLOW FILTERING";

        var failedOrders = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, minRetryCount);

        return failedOrders.Select(m => new FailedOrderMessage
        {
            Topic = m.Topic,
            Partition = m.Partition,
            Offset = m.Offset,
            Key = m.Key,
            Value = m.Value,
            ErrorMessage = m.ErrorMessage,
            StackTrace = m.StackTrace,
            RetryCount = m.RetryCount,
            FailedAt = m.FailedAt,
            ConsumerName = m.ConsumerName
        }).ToList();
    }

    public Task UpdateRetryCount(int id, int newRetryCount, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }
}