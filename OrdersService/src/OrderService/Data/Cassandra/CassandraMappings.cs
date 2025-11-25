using Cassandra.Mapping;
using OrderService.Domain.Models;

namespace OrderService.Data.Cassandra;

public class CassandraMappings : Mappings
{
    public CassandraMappings()
    {
        For<Order>()
            .TableName("orders")
            .PartitionKey(o => o.OrderId)
            .ClusteringKey(o => o.CreatedAt, SortOrder.Descending)
            .Column(o => o.OrderId, cm => cm.WithName("order_id"))
            .Column(o => o.CustomerId, cm => cm.WithName("customer_id"))
            .Column(o => o.Amount, cm => cm.WithName("amount"))
            .Column(o => o.CreatedAt, cm => cm.WithName("created_at"))
            .Column(o => o.Status, cm => cm.WithName("status").WithDbType<int>())
            .Column(o => o.ProcessedAt, cm => cm.WithName("processed_at"))
            .Column(o => o.Partition, cm => cm.WithName("partition"))
            .Column(o => o.Offset, cm => cm.WithName("offset"))
            .Column(o => o.Id, cm => cm.Ignore());

        For<OrderByCustomer>()
            .TableName("orders_by_customer")
            .PartitionKey(o => o.CustomerId)
            .ClusteringKey(o => o.CreatedAt, SortOrder.Descending)
            .ClusteringKey(o => o.OrderId, SortOrder.Descending)
            .Column(o => o.CustomerId, cm => cm.WithName("customer_id"))
            .Column(o => o.CreatedAt, cm => cm.WithName("created_at"))
            .Column(o => o.OrderId, cm => cm.WithName("order_id"))
            .Column(o => o.Amount, cm => cm.WithName("amount"))
            .Column(o => o.Status, cm => cm.WithName("status"))
            .Column(o => o.ProcessedAt, cm => cm.WithName("processed_at"))
            .Column(o => o.Partition, cm => cm.WithName("partition"))
            .Column(o => o.Offset, cm => cm.WithName("offset"));

        For<FailedOrderMessage>()
            .TableName("failed_order_messages")
            .PartitionKey(m => m.Topic, m => m.Partition)
            .ClusteringKey(m => m.FailedAt, SortOrder.Descending)
            .ClusteringKey(m => m.Offset, SortOrder.Descending)
            .Column(m => m.Topic, cm => cm.WithName("topic"))
            .Column(m => m.Partition, cm => cm.WithName("partition"))
            .Column(m => m.Offset, cm => cm.WithName("offset"))
            .Column(m => m.Key, cm => cm.WithName("key"))
            .Column(m => m.Value, cm => cm.WithName("value"))
            .Column(m => m.ErrorMessage, cm => cm.WithName("error_message"))
            .Column(m => m.StackTrace, cm => cm.WithName("stack_trace"))
            .Column(m => m.RetryCount, cm => cm.WithName("retry_count"))
            .Column(m => m.FailedAt, cm => cm.WithName("failed_at"))
            .Column(m => m.ConsumerName, cm => cm.WithName("consumer_name"))
            .Column(m => m.Id, cm => cm.Ignore());

        For<FailedMessageByConsumer>()
            .TableName("failed_messages_by_consumer")
            .PartitionKey(m => m.ConsumerName)
            .ClusteringKey(m => m.FailedAt, SortOrder.Descending)
            .ClusteringKey(m => m.Topic, SortOrder.Ascending)
            .ClusteringKey(m => m.Partition, SortOrder.Ascending)
            .ClusteringKey(m => m.Offset, SortOrder.Descending)
            .Column(m => m.ConsumerName, cm => cm.WithName("consumer_name"))
            .Column(m => m.FailedAt, cm => cm.WithName("failed_at"))
            .Column(m => m.Topic, cm => cm.WithName("topic"))
            .Column(m => m.Partition, cm => cm.WithName("partition"))
            .Column(m => m.Offset, cm => cm.WithName("offset"))
            .Column(m => m.Key, cm => cm.WithName("key"))
            .Column(m => m.Value, cm => cm.WithName("value"))
            .Column(m => m.ErrorMessage, cm => cm.WithName("error_message"))
            .Column(m => m.StackTrace, cm => cm.WithName("stack_trace"))
            .Column(m => m.RetryCount, cm => cm.WithName("retry_count"));

        For<FailedMessageByTopic>()
            .TableName("failed_order_messages_by_topic")
            .PartitionKey(m => m.Topic)
            .ClusteringKey(m => m.FailedAt, SortOrder.Descending)
            .ClusteringKey(m => m.ConsumerName, SortOrder.Ascending)
            .ClusteringKey(m => m.Partition, SortOrder.Ascending)
            .ClusteringKey(m => m.Offset, SortOrder.Descending)
            .Column(m => m.Topic, cm => cm.WithName("topic"))
            .Column(m => m.FailedAt, cm => cm.WithName("failed_at"))
            .Column(m => m.ConsumerName, cm => cm.WithName("consumer_name"))
            .Column(m => m.Partition, cm => cm.WithName("partition"))
            .Column(m => m.Offset, cm => cm.WithName("offset"))
            .Column(m => m.Key, cm => cm.WithName("key"))
            .Column(m => m.Value, cm => cm.WithName("value"))
            .Column(m => m.ErrorMessage, cm => cm.WithName("error_message"))
            .Column(m => m.StackTrace, cm => cm.WithName("stack_trace"))
            .Column(m => m.RetryCount, cm => cm.WithName("retry_count"));
    }
}