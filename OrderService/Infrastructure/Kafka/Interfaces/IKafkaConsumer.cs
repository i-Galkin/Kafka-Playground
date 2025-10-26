namespace OrderService.Infrastructure.Kafka.Interfaces
{
    public interface IKafkaConsumer : IDisposable
    {
        Task StartAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);
    }
}
