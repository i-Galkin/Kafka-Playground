using OrderService.Data.Interfaces;
using OrderService.Domain.Models;
using OrderService.Domain.Models.Mappers;

namespace OrderService.Data.Cassandra.Repositories;

public class CassandraOrderRepository : IOrderRepository
{
    private readonly CassandraContext _context;

    public CassandraOrderRepository(CassandraContext context)
    {
        _context = context;
    }

    public Task<Order> GetById(int id, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Cassandra does not support auto-increment Id");
    }

    public async Task Upsert(Order order, CancellationToken cancellationToken)
    {
        await _context.Mapper.InsertAsync(order);
    }

    public async Task<Order> GetByOrderId(string orderId, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders WHERE order_id = ? LIMIT 1";
        var order = await _context.Mapper.FirstOrDefaultAsync<Order>(cql, orderId);

        return order;
    }

    // TODO:
    public async Task<List<Order>> GetListByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders";
        var orders = await _context.Mapper.FetchAsync<Order>(cql);

        return orders.Where(o => o.CreatedAt >= from && o.CreatedAt <= to).ToList();
    }

    public async Task<List<Order>> GetList(CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders";
        var orders = await _context.Mapper.FetchAsync<Order>(cql);

        return orders.ToList();
    }

    public async Task<bool> Exists(string orderId, CancellationToken cancellationToken)
    {
        const string cql = "SELECT order_id FROM orders WHERE order_id = ? LIMIT 1";
        var result = await _context.Mapper.FirstOrDefaultAsync<Order>(cql, orderId);

        return result != null;
    }

    public async Task<List<Order>> GetByCustomerId(string customerId, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders_by_customer WHERE customer_id = ?";
        var ordersByCustomer = await _context.Mapper.FetchAsync<OrderByCustomer>(cql, customerId);

        return ordersByCustomer.Select(o => o.ToOrder()).ToList();
    }

    public async Task<List<Order>> GetByCustomerIdAndDateRange(string customerId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        const string cql = "SELECT * FROM orders_by_customer WHERE customer_id = ? AND created_at >= ? AND created_at <= ?";
        var ordersByCustomer = await _context.Mapper.FetchAsync<OrderByCustomer>(cql, customerId, from, to);

        return ordersByCustomer.Select(o => o.ToOrder()).ToList();
    }
}