using Confluent.Kafka;
using System.Text.Json;
using OrderService.Domain.Models;
using OrderService.Applications.Services.Interfaces;
using OrderService.Data.Interfaces;

namespace OrderService.Applications.Services
{
    public class DeadLetterQueueService : IDeadLetterQueue
    {
        private readonly ILogger<DeadLetterQueueService> _logger;
        private readonly string _consumerName;
        private readonly IFailedOrderMessageRepository _failedOrderMessageRepository;

        public DeadLetterQueueService(ILogger<DeadLetterQueueService> logger, IHostEnvironment environment, IFailedOrderMessageRepository failedOrderMessageRepository)
        {
            _logger = logger;
            _failedOrderMessageRepository = failedOrderMessageRepository;
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
                    ConsumerName = _consumerName,
                };

                await _failedOrderMessageRepository.Add(failedMessage, cancellationToken);

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
