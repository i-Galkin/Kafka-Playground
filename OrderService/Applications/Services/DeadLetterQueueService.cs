using Confluent.Kafka;
using System.Text.Json;
using OrderService.Infrastructure.Database;
using OrderService.Domain.Models;
using OrderService.Applications.Services.Interfaces;

namespace OrderService.Applications.Services
{
    public class DeadLetterQueueService : IDeadLetterQueue
    {
        private readonly AppDbContext _dbContext;
        private readonly ILogger<DeadLetterQueueService> _logger;
        private readonly string _consumerName;

        public DeadLetterQueueService(AppDbContext dbContext, ILogger<DeadLetterQueueService> logger, IHostEnvironment environment)
        {
            _dbContext = dbContext;
            _logger = logger;
            _consumerName = environment.ApplicationName;
        }

        public async Task SendAsync<TKey, TValue>(ConsumeResult<TKey, TValue> message, Exception exception,
            int retryCount, CancellationToken cancellationToken = default)
        {
            try
            {
                var failedMessage = new FailedOrderMessage
                {
                    Topic = message.Topic,
                    Partition = message.Partition.Value,
                    Offset = message.Offset.Value,
                    Key = message.Message.Key?.ToString(),
                    Value = JsonSerializer.Serialize(message.Message.Value),
                    ErrorMessage = exception.Message,
                    StackTrace = exception.StackTrace,
                    RetryCount = retryCount,
                    FailedAt = DateTime.UtcNow,
                    ConsumerName = _consumerName
                };

                _dbContext.FailedOrderMessages.Add(failedMessage);
                await _dbContext.SaveChangesAsync(cancellationToken);

                _logger.LogWarning("Message sent to DLQ: Topic={Topic}, Partition={Partition}, Offset={Offset}",
                    message.Topic, message.Partition.Value, message.Offset.Value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "CRITICAL: Failed to save message to DLQ");

                throw;
            }
        }
    }
}
