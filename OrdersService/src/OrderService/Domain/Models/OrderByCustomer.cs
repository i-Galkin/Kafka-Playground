namespace OrderService.Domain.Models;

public class OrderByCustomer
{
    public string CustomerId { get; set; }
    public DateTime CreatedAt { get; set; }
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public int Status { get; set; }
    public DateTime ProcessedAt { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
}

