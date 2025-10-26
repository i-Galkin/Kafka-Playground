using OrderService.Applications.Validators.Interfaces;
using OrderService.Domain.Enums;
using OrderService.Domain.Messages;

namespace OrderService.Applications.Validators
{
    public class OrderValidator : IOrderValidator
    {
        public void Validate(OrderMessage order)
        {
            if (order.Amount <= 0)
            {
                throw new InvalidOperationException($"Order amount must be positive, got: {order.Amount}");
            }

            if (string.IsNullOrWhiteSpace(order.CustomerId))
            {
                throw new InvalidOperationException("Customer ID is required");
            }

            if (!Enum.TryParse<OrderStatus>(order.Status, ignoreCase: true, out _))
            {
                throw new InvalidOperationException($"Invalid order status: {order.Status}");
            }
        }
    }
}
