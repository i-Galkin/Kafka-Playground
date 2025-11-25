using OrderService.Domain.Models;

namespace OrderService.Data.Interfaces;

public interface IOrderRepository
{
    Task<Order> GetById(int id, CancellationToken cancellationToken );
    Task<Order> GetByOrderId(string orderId, CancellationToken cancellationToken);
    Task<List<Order>> GetList(CancellationToken cancellationToken);
    Task<List<Order>> GetListByDateRange(DateTime from, DateTime to, CancellationToken cancellationToken);
    Task<List<Order>> GetByCustomerId(string customerId, CancellationToken cancellationToken);
    Task<List<Order>> GetByCustomerIdAndDateRange(string customerId, DateTime from, DateTime to, CancellationToken cancellationToken);
    Task Upsert(Order order, CancellationToken cancellationToken);
    Task<bool> Exists(string orderId, CancellationToken cancellationToken);
}