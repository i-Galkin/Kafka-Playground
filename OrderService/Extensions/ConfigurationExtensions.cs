using OrderService.Infrastructure.Configuration;

namespace OrderService.Extensions
{
    public static class ConfigurationExtensions
    {
        public static IServiceCollection AddApplicationConfiguration(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<KafkaSettings>(
                configuration.GetSection(KafkaSettings.SectionName));

            services.Configure<RetryPolicySettings>(
                configuration.GetSection(RetryPolicySettings.SectionName));

            return services;
        }
    }
}
