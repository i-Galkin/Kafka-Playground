namespace Consumer.Configurations
{
    public static class PostgreConfiguration
    {
        public static string GetConnectionString()
        {
            return $"Host={Environment.GetEnvironmentVariable("POSTGRES_HOST")};" +
                   $"Port={Environment.GetEnvironmentVariable("POSTGRES_PORT")};" +
                   $"Database={Environment.GetEnvironmentVariable("POSTGRES_DB")};" +
                   $"Username={Environment.GetEnvironmentVariable("POSTGRES_USER")};" +
                   $"Password={Environment.GetEnvironmentVariable("POSTGRES_PASSWORD")}";
        }
    }
}
