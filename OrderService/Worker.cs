using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService;

public class Worker : BackgroundService
{
    private readonly IKafkaConsumer _kafkaConsumer;
    private readonly ILogger<Worker> _logger;

    public Worker(IKafkaConsumer kafkaConsumer, ILogger<Worker> logger)
    {
        _kafkaConsumer = kafkaConsumer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker starting at: {Time}", DateTimeOffset.Now);

        try
        {
            await _kafkaConsumer.StartAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Fatal error in Worker");
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker stopping at: {Time}", DateTimeOffset.UtcNow);

        await _kafkaConsumer.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}
