using OrderService.Infrastructure.Configuration;
using OrderService.Infrastructure.Configuration.Database;

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

            services.Configure<DatabaseSettings>(
                configuration.GetSection(DatabaseSettings.SectionName));

            return services;
        }
    }
}
