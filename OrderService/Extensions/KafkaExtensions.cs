using Confluent.Kafka;
using Microsoft.Extensions.Options;
using OrderService.Domain.Messages;
using OrderService.Infrastructure.Configuration;
using OrderService.Infrastructure.Kafka;
using OrderService.Infrastructure.Kafka.Handlers;
using OrderService.Infrastructure.Kafka.Interfaces;
using OrderService.Infrastructure.Serialization;

namespace OrderService.Extensions
{
    public static class KafkaExtensions
    {
        public static IServiceCollection AddKafkaConsumers(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(typeof(IKafkaConsumerFactory<,>), typeof(KafkaConsumerFactory<,>));

            services.AddKafkaConsumer<string, OrderMessage, OrderMessageHandler>("Orders");

            return services;
        }

        private static IServiceCollection AddKafkaConsumer<TKey, TMessage, THandler>(this IServiceCollection services, string topicConfigurationKey)
                where THandler : class, IMessageHandler<TKey, TMessage>
        {
            services.AddScoped<IMessageHandler<TKey, TMessage>, THandler>();

            services.AddSingleton<IHostedService>(serviceProvider =>
            {
                var kafkaSettings = serviceProvider.GetRequiredService<IOptions<KafkaSettings>>().Value;
                var scopeFactory = serviceProvider.GetRequiredService<IServiceScopeFactory>();
                var logger = serviceProvider.GetRequiredService<ILogger<KafkaConsumer<TKey, TMessage>>>();
                var factory = serviceProvider.GetRequiredService<IKafkaConsumerFactory<TKey, TMessage>>();

                var valueDeserializer = new JsonDeserializer<TMessage>();

                if (!kafkaSettings.Topics.TryGetValue(topicConfigurationKey, out var topicConfig))
                {
                    throw new InvalidOperationException($"{topicConfigurationKey} topic is not configured");
                }

                if (!topicConfig.Enabled)
                {
                    throw new InvalidOperationException($"Topic '{topicConfigurationKey}' is not enabled");
                }

                var config = new ConsumerConfig
                {
                    BootstrapServers = kafkaSettings.BootstrapServers,
                    GroupId = kafkaSettings.GroupId,
                    AutoOffsetReset = Enum.Parse<AutoOffsetReset>(kafkaSettings.AutoOffsetReset, ignoreCase: true),
                    EnableAutoCommit = kafkaSettings.EnableAutoCommit
                };

                var consumer = factory.CreateConsumer(config, valueDeserializer, logger);

                return new KafkaConsumer<TKey, TMessage>(
                    consumer,
                    topicConfig.Name,
                    logger,
                    scopeFactory
                );
            });

            return services;
        }
    }
}
