namespace OrderService.Infrastructure.Kafka.Interfaces
{
    public interface IRetryManager
    {
        Task<T> ExecuteAsync<T>(Func<Task<T>> action, Func<Exception, bool> isTransient = null);
    }
}
