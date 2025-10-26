using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;

    public Worker(IServiceScopeFactory serviceScopeFactory, ILogger<Worker> logger)
    {
        _logger = logger;
    }
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker starting at: {Time}", DateTimeOffset.Now);

        try
        {
            using IServiceScope scope = _serviceScopeFactory.CreateScope();
            var kafkaConsumer = scope.ServiceProvider.GetRequiredService<IKafkaConsumer>();

            await kafkaConsumer.StartAsync(cancellationToken);
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

        await base.StopAsync(cancellationToken);
    }
}
