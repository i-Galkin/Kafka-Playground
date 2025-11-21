namespace OrderService.Infrastructure.Configuration
{
    public class RetryPolicySettings
    {
        public const string SectionName = "RetryPolicy";

        public int MaxRetries { get; set; } = 3;
        public int InitialDelaySeconds { get; set; } = 2;
    }
}
