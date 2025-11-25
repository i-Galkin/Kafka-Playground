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

    public async Task<FailedOrderMessage> GetById(Guid id, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages WHERE id = ?";
        var message = await _context.Mapper.FirstOrDefaultAsync<FailedOrderMessage>(cql, id);

        return message;
    }

    public async Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages_by_topic WHERE topic = ?";

        var messages = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, topic);

        return messages.Select(m => m.ToFailedOrderMessage()).ToList();
    }

    public async Task<List<FailedOrderMessage>> GetByConsumerName(string consumerName, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages_by_consumer WHERE consumer_name = ?";

        var messages = await _context.Mapper.FetchAsync<FailedMessageByConsumer>(cql, consumerName);

        return messages.Select(m => m.ToFailedOrderMessage()).ToList();
    }

    // TODO
    public async Task<List<FailedOrderMessage>> GetByDateRange(string consumerName, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM failed_order_messages_by_consumer WHERE consumer_name = ? AND failed_at >= ? AND failed_at <= ?";

        var failedOrders = await _context.Mapper.FetchAsync<FailedMessageByTopic>(cql, consumerName, from, to);

        return failedOrders
            .Select(m => m.ToFailedOrderMessage())
            .ToList();
    }
}