using OrderService.Data.Interfaces;
using OrderService.Domain.Models;

namespace OrderService.Data.Cassandra.Repositories;

public class CassandraOrderRepository : IOrderRepository
{
    private readonly CassandraContext _context;

    public CassandraOrderRepository(CassandraContext context)
    {
        _context = context;
    }

    public async Task Upsert(Order order, CancellationToken cancellationToken)
    {
        await _context.Mapper.InsertAsync(order);
    }

    public async Task<Order> GetByOrderId(string orderId, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders WHERE order_id = ?";
        var order = await _context.Mapper.FirstOrDefaultAsync<Order>(cql, orderId);

        return order;
    }

    public Task<Order> GetById(int id, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }

    // TODO: Implement pagination
    public async Task<List<Order>> GetListByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders WHERE created_at >= ? AND created_at <= ?";
        var orders = await _context.Mapper.FetchAsync<Order>(cql, from, to);

        return orders.ToList();
    }

    // TODO: Implement pagination
    public async Task<List<Order>> GetList(CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders";
        var orders = await _context.Mapper.FetchAsync<Order>(cql);

        return orders.ToList();
    }

    public async Task<bool> Exists(string orderId, CancellationToken cancellationToken)
    {
        var cql = "SELECT order_id FROM orders WHERE order_id = ? LIMIT 1";
        var result = await _context.Mapper.FirstOrDefaultAsync<Order>(cql, orderId);

        return result != null;
    }
}