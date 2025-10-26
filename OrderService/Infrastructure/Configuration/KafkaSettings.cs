namespace OrderService.Infrastructure.Configuration
{
    public class KafkaSettings
    {
        public const string SectionName = "Kafka";

        public string BootstrapServers { get; set; }
        public string GroupId { get; set; }
        public string AutoOffsetReset { get; set; }
        public bool EnableAutoCommit { get; set; }
        public Dictionary<string, TopicSettings> Topics { get; set; }
    }

    public class TopicSettings
    {
        public string Name { get; set; }
    }
}
