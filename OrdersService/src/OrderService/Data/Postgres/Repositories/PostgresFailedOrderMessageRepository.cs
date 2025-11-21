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

    public async Task<FailedOrderMessage> GetById(int id, CancellationToken cancellationToken)
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

    public async Task<List<FailedOrderMessage>> GetByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        return await _context.FailedOrderMessages
            .Where(m => m.FailedAt >= from && m.FailedAt <= to)
            .OrderBy(m => m.FailedAt)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<FailedOrderMessage>> GetByRetryCount(int minRetryCount, CancellationToken cancellationToken)
    {
        return await _context.FailedOrderMessages
            .Where(m => m.RetryCount >= minRetryCount)
            .OrderByDescending(m => m.RetryCount).ThenBy(m => m.FailedAt)
            .ToListAsync(cancellationToken);
    }

    public async Task UpdateRetryCount(int id, int newRetryCount, CancellationToken cancellationToken)
    {
        var message = await _context.FailedOrderMessages.FindAsync([id], cancellationToken);
        if (message != null)
        {
            message.RetryCount = newRetryCount;
            await _context.SaveChangesAsync(cancellationToken);
        }
    }
}