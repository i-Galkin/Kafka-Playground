namespace OrderService.Domain.Enums
{
    public enum OrderStatus
    {
        Created = 1,
        Processing,
        Completed,
        Cancelled,
        Failed
    }
}
