using Confluent.Kafka;
using OrderService.Applications.Services.Interfaces;
using OrderService.Applications.Validators.Interfaces;
using OrderService.Data.Interfaces;
using OrderService.Domain.Enums;
using OrderService.Domain.Messages;
using OrderService.Domain.Models;
using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Applications.Services
{
    public class OrderProcessor : IOrderProcessor
    {
        private readonly IRetryManager _retryPolicy;
        private readonly IDeadLetterQueue _deadLetterQueue;
        private readonly ILogger<OrderProcessor> _logger;
        private readonly IOrderValidator _orderValidator;
        private readonly IOrderRepository _orderRepository;

        public OrderProcessor(IRetryManager retryPolicy, IDeadLetterQueue deadLetterQueue, ILogger<OrderProcessor> logger, IOrderValidator orderValidator, IOrderRepository orderRepository)
        {
            _retryPolicy = retryPolicy;
            _deadLetterQueue = deadLetterQueue;
            _logger = logger;
            _orderValidator = orderValidator;
            _orderRepository = orderRepository;
        }

        public async Task ProcessAsync(ConsumeResult<string, OrderMessage> message, CancellationToken cancellationToken = default)
        {
            var orderMessage = message.Message.Value;

            _logger.LogInformation("Processing order #{OrderId} from customer {CustomerId}",
                orderMessage.OrderId, orderMessage.CustomerId);

            try
            {
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _orderValidator.Validate(orderMessage);

                    var order = new Order
                    {
                        OrderId = orderMessage.OrderId,
                        CustomerId = orderMessage.CustomerId,
                        Amount = orderMessage.Amount,
                        CreatedAt = orderMessage.CreatedAt,
                        Status = Enum.Parse<OrderStatus>(orderMessage.Status, ignoreCase: true),
                        ProcessedAt = DateTime.UtcNow,
                        Partition = message.Partition.Value,
                        Offset = message.Offset.Value
                    };

                    await _orderRepository.Upsert(order, cancellationToken);

                    _logger.LogInformation("Order #{OrderId} saved successfully", order.OrderId);

                    return true;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process order #{OrderId} after retries", orderMessage.OrderId);

                await _deadLetterQueue.SendAsync(message, ex, 3, cancellationToken);

                throw;
            }
        }
    }
}
