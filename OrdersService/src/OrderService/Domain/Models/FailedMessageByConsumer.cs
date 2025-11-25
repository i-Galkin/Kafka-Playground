namespace OrderService.Domain.Models;

public class FailedMessageByConsumer
{
    public Guid Id { get; set; }
    public string ConsumerName { get; set; }
    public DateTime FailedAt { get; set; }
    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public string Key { get; set; }
    public string Value { get; set; }
    public string ErrorMessage { get; set; }
    public string StackTrace { get; set; }
    public int RetryCount { get; set; }
}

