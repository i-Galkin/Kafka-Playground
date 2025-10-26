namespace Consumer.Kafka
{
    public class RetryPolicy
    {
        private readonly int _maxRetries;
        private readonly TimeSpan _initialDelay;

        public RetryPolicy(int maxRetries = 3, TimeSpan? initialDelay = null)
        {
            _maxRetries = maxRetries;
            _initialDelay = initialDelay ?? TimeSpan.FromSeconds(2);
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

                    // Check if error is transient (retryable)
                    var shouldRetry = isTransient?.Invoke(ex) ?? IsTransientError(ex);
                    if (!shouldRetry || attempt >= _maxRetries)
                    {
                        throw;
                    }

                    // 2s, 4s, 8s, ...
                    var delay = TimeSpan.FromMilliseconds(_initialDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));

                    Console.WriteLine($"Transient error (attempt {attempt}/{_maxRetries}): {ex.Message}");
                    Console.WriteLine($"Retrying in {delay.TotalSeconds} seconds...");

                    await Task.Delay(delay);
                }
            }
        }

        private static bool IsTransientError(Exception ex)
        {
            // Database transient errors
            if (ex.Message.Contains("timeout") ||
                ex.Message.Contains("connection") ||
                ex.Message.Contains("network") ||
                ex.Message.Contains("deadlock"))
            {
                return true;
            }

            // Npgsql specific transient errors
            if (ex is Npgsql.NpgsqlException npgsqlEx)
            {
                // Connection errors, timeouts
                return npgsqlEx.IsTransient;
            }

            return false;
        }
    }
}
