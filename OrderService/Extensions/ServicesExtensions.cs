using OrderService.Applications.Services;
using OrderService.Applications.Services.Interfaces;
using OrderService.Applications.Validators;
using OrderService.Applications.Validators.Interfaces;
using OrderService.Infrastructure.Kafka;
using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Extensions
{
    public static class ServicesExtensions
    {
        public static IServiceCollection AddApplicationServices(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddScoped<IOrderProcessor, OrderProcessor>();
            services.AddScoped<IOrderValidator, OrderValidator>();
            services.AddScoped<IRetryManager, RetryManager>();
            services.AddScoped<IDeadLetterQueue, DeadLetterQueueService>();

            return services;
        }
    }
}
