namespace OrderService.Domain.Models.Mappers;

public static class FailedMessageMapper
{
    public static FailedMessageByConsumer ToByConsumer(this FailedOrderMessage source)
    {
        return new FailedMessageByConsumer
        {
            Id = source.Id,
            ConsumerName = source.ConsumerName,
            FailedAt = source.FailedAt,
            Topic = source.Topic,
            Partition = source.Partition,
            Offset = source.Offset,
            Key = source.Key,
            Value = source.Value,
            ErrorMessage = source.ErrorMessage,
            StackTrace = source.StackTrace,
            RetryCount = source.RetryCount
        };
    }

    public static FailedMessageByTopic ToByTopic(this FailedOrderMessage source)
    {
        return new FailedMessageByTopic
        {
            Id = source.Id,
            Topic = source.Topic,
            FailedAt = source.FailedAt,
            ConsumerName = source.ConsumerName,
            Partition = source.Partition,
            Offset = source.Offset,
            Key = source.Key,
            Value = source.Value,
            ErrorMessage = source.ErrorMessage,
            StackTrace = source.StackTrace,
            RetryCount = source.RetryCount
        };
    }

    public static FailedOrderMessage ToFailedOrderMessage(this FailedMessageByConsumer source)
    {
        return new FailedOrderMessage
        {
            Id = source.Id,
            ConsumerName = source.ConsumerName,
            FailedAt = source.FailedAt,
            Topic = source.Topic,
            Partition = source.Partition,
            Offset = source.Offset,
            Key = source.Key,
            Value = source.Value,
            ErrorMessage = source.ErrorMessage,
            StackTrace = source.StackTrace,
            RetryCount = source.RetryCount
        };
    }

    public static FailedOrderMessage ToFailedOrderMessage(this FailedMessageByTopic source)
    {
        return new FailedOrderMessage
        {
            Id = source.Id,
            Topic = source.Topic,
            Partition = source.Partition,
            Offset = source.Offset,
            Key = source.Key,
            Value = source.Value,
            ErrorMessage = source.ErrorMessage,
            StackTrace = source.StackTrace,
            RetryCount = source.RetryCount,
            FailedAt = source.FailedAt,
            ConsumerName = source.ConsumerName
        };
    }
}

