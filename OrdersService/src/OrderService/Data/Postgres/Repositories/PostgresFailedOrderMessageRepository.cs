using Microsoft.EntityFrameworkCore;
using OrderService.Data.Interfaces;
using OrderService.Domain.Models;

namespace OrderService.Data.Postgres.Repositories;

public class PostgresFailedOrderMessageRepository : IFailedOrderMessageRepository
{
    private readonly AppDbContext _context;

    public PostgresFailedOrderMessageRepository(AppDbContext context)
    {
        _context = context;
    }

    public async Task Add(FailedOrderMessage message, CancellationToken cancellationToken)
    {
        await _context.FailedOrderMessages.AddAsync(message, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task<FailedOrderMessage> GetById(Guid id, CancellationToken cancellationToken)
    {
        return await _context.FailedOrderMessages
            .FindAsync([id], cancellationToken);
    }

    public async Task<List<FailedOrderMessage>> GetByTopic(string topic, CancellationToken cancellationToken)
    {
        return await _context.FailedOrderMessages
            .Where(m => m.Topic == topic)
            .OrderByDescending(m => m.FailedAt)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<FailedOrderMessage>> GetByConsumerName(string consumerName, CancellationToken cancellationToken)
    {
        return await _context.FailedOrderMessages
            .Where(m => m.ConsumerName == consumerName)
            .OrderByDescending(m => m.FailedAt)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<FailedOrderMessage>> GetByDateRange(string consumerName, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        return await _context.FailedOrderMessages
            .Where(m => consumerName == m.ConsumerName && m.FailedAt >= from && m.FailedAt <= to)
            .OrderBy(m => m.FailedAt)
            .ToListAsync(cancellationToken);
    }
}