using Microsoft.EntityFrameworkCore;
using OrderService.Data.Interfaces;
using OrderService.Domain.Models;

namespace OrderService.Data.Postgres.Repositories;

public class PostgresOrderRepository : IOrderRepository
{
    private readonly AppDbContext _context;

    public PostgresOrderRepository(AppDbContext context)
    {
        _context = context;
    }

    public async Task Upsert(Order order, CancellationToken cancellationToken)
    {
        var existing = await _context.Orders
            .FirstOrDefaultAsync(o => o.OrderId == order.OrderId, cancellationToken);

        if (existing != null)
        {
            existing.CustomerId = order.CustomerId;
            existing.Amount = order.Amount;
            existing.Status = order.Status;
            existing.ProcessedAt = order.ProcessedAt;
            existing.Partition = order.Partition;
            existing.Offset = order.Offset;
        }
        else
        {
            await _context.Orders.AddAsync(order, cancellationToken);
        }

        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task<Order> GetByOrderId(string orderId, CancellationToken cancellationToken)
    {
        return await _context.Orders
            .FirstOrDefaultAsync(o => o.OrderId == orderId, cancellationToken);
    }

    public async Task<Order> GetById(int id, CancellationToken cancellationToken)
    {
        return await _context.Orders
            .FindAsync([id], cancellationToken);
    }

    // TODO: Add Pagination
    public async Task<List<Order>> GetList(CancellationToken cancellationToken)
    {
        return await _context.Orders.ToListAsync(cancellationToken);
    }

    // TODO: Add Pagination
    public async Task<List<Order>> GetListByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        return await _context.Orders
            .Where(o => o.CreatedAt >= from && o.CreatedAt <= to)
            .OrderBy(o => o.CreatedAt)
            .ToListAsync(cancellationToken);
    }

    public async Task<bool> Exists(string orderId, CancellationToken cancellationToken)
    {
        return await _context.Orders
            .AnyAsync(o => o.OrderId == orderId, cancellationToken);
    }
}