using Confluent.Kafka;
using OrderService.Applications.Services.Interfaces;
using OrderService.Domain.Messages;
using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Infrastructure.Kafka.Handlers
{
    public class OrderMessageHandler : IMessageHandler<string, OrderMessage>
    {
        private readonly IOrderProcessor _orderProcessor;
        private readonly ILogger<OrderMessageHandler> _logger;

        public OrderMessageHandler(IOrderProcessor orderProcessor, ILogger<OrderMessageHandler> logger)
        {
            _orderProcessor = orderProcessor;
            _logger = logger;
        }

        public async Task HandleAsync(ConsumeResult<string, OrderMessage> message, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Handling message from topic {Topic}, partition {Partition}, offset {Offset}",
                message.Topic, message.Partition.Value, message.Offset.Value);

            await _orderProcessor.ProcessAsync(message, cancellationToken);
        }
    }
}
