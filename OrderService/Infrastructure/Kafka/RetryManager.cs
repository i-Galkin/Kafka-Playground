using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Infrastructure.Kafka
{
    public class RetryManager : IRetryManager
    {
        private readonly int _maxRetries;
        private readonly TimeSpan _initialDelay;
        private readonly ILogger<RetryManager> _logger;

        public RetryManager(IConfiguration configuration, ILogger<RetryManager> logger)
        {
            _maxRetries = configuration.GetValue("RetryPolicy:MaxRetries", 3); ;
            _initialDelay = TimeSpan.FromSeconds(configuration.GetValue("RetryPolicy:InitialDelaySeconds", 2));
            _logger = logger;
        }

        public async Task<T> ExecuteAsync<T>(Func<Task<T>> action, Func<Exception, bool> isTransient = null)
        {
            int attempt = 0;

            while (true)
            {
                try
                {
                    return await action();
                }
                catch (Exception ex)
                {
                    attempt++;

                    // Check if error is retryable
                    var shouldRetry = isTransient?.Invoke(ex) ?? IsTransientError(ex);
                    if (!shouldRetry || attempt >= _maxRetries)
                    {
                        throw;
                    }

                    // 2s, 4s, 8s, ...
                    var delay = TimeSpan.FromMilliseconds(_initialDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));

                    _logger.LogWarning($"Transient error (attempt {attempt}/{_maxRetries}): {ex.Message}; Retrying in {delay.TotalSeconds} seconds...");

                    await Task.Delay(delay);
                }
            }
        }

        private static bool IsTransientError(Exception ex)
        {
            if (ex.Message.Contains("timeout") ||
                ex.Message.Contains("connection") ||
                ex.Message.Contains("network") ||
                ex.Message.Contains("deadlock"))
            {
                return true;
            }

            if (ex is Npgsql.NpgsqlException npgsqlEx)
            {
                return npgsqlEx.IsTransient;
            }

            return false;
        }
    }
}
