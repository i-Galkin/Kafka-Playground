namespace OrderService.Domain.Messages
{
    public class OrderMessage
    {
        public int OrderId { get; set; }
        public string CustomerId { get; set; }
        public decimal Amount { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Status { get; set; }
    }
}
