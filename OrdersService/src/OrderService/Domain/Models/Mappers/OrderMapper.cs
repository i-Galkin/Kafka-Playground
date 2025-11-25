using OrderService.Domain.Enums;

namespace OrderService.Domain.Models.Mappers;

public static class OrderMapper
{
    public static OrderByCustomer ToByCustomer(this Order source)
    {
        return new OrderByCustomer
        {
            CustomerId = source.CustomerId,
            CreatedAt = source.CreatedAt,
            OrderId = source.OrderId,
            Amount = source.Amount,
            Status = (int)source.Status,
            ProcessedAt = source.ProcessedAt,
            Partition = source.Partition,
            Offset = source.Offset
        };
    }

    public static Order ToOrder(this OrderByCustomer source)
    {
        return new Order
        {
            OrderId = source.OrderId,
            CustomerId = source.CustomerId,
            Amount = source.Amount,
            CreatedAt = source.CreatedAt,
            Status = (OrderStatus)source.Status,
            ProcessedAt = source.ProcessedAt,
            Partition = source.Partition,
            Offset = source.Offset
        };
    }
}

