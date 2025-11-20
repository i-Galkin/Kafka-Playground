using Confluent.Kafka;
using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Infrastructure.Kafka
{
    public class KafkaConsumer<TKey, TValue> : IHostedService
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<KafkaConsumer<TKey, TValue>> _logger;
        private readonly string _topic;

        public KafkaConsumer(
            IConsumer<TKey, TValue> consumer,
            string topic,
            ILogger<KafkaConsumer<TKey, TValue>> logger,
            IServiceScopeFactory scopeFactory)
        {
            _consumer = consumer;
            _scopeFactory = scopeFactory;
            _topic = topic;
            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topic);

            _logger.LogInformation("Subscribed to topic: {Topic}", _topic);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                        if (consumeResult == null)
                        {
                            continue;
                        }

                        using (var scope = _scopeFactory.CreateScope())
                        {
                            var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TKey, TValue>>();
                            await handler.HandleAsync(consumeResult, cancellationToken);
                        }

                        _consumer.Commit(consumeResult);

                        _logger.LogDebug("Committed offset {Offset} for partition {Partition}",
                            consumeResult.Offset.Value, consumeResult.Partition.Value);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Consume error: {Reason}", ex.Error.Reason);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer operation cancelled");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping Kafka consumer...");
            _consumer.Close();

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _consumer?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
