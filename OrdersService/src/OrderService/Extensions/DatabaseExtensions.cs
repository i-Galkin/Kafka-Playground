using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OrderService.Data.Cassandra;
using OrderService.Data.Cassandra.Repositories;
using OrderService.Data.Interfaces;
using OrderService.Data.Postgres;
using OrderService.Data.Postgres.Repositories;
using OrderService.Infrastructure.Configuration.Database;
using OrderService.Infrastructure.Database;

namespace OrderService.Extensions
{
    public static class DatabaseExtensions
    {
        public static IServiceCollection AddDatabase(this IServiceCollection services, IConfiguration configuration)
        {
            var databaseSettings = configuration
                .GetRequiredSection("Database")
                .Get<DatabaseSettings>() ?? throw new InvalidOperationException("Database settings not found");

            switch (databaseSettings.Type)
            {
                case DatabaseType.Postgres:
                    Console.WriteLine("Using Postgres database");
                    services.UsePostgres(databaseSettings);
                    break;

                case DatabaseType.Cassandra:
                    Console.WriteLine("Using Cassandra database");
                    services.UseCassandra(databaseSettings);
                    break;

                default:
                    throw new InvalidOperationException($"Unsupported database type: {databaseSettings.Type}");
            }

            return services;
        }

        private static IServiceCollection UsePostgres(this IServiceCollection services, DatabaseSettings databaseSettings)
        {
            services.AddDbContext<AppDbContext>(options => options.UseNpgsql(databaseSettings.ConnectionString));

            services.AddScoped<IOrderRepository, PostgresOrderRepository>();
            services.AddScoped<IFailedOrderMessageRepository, PostgresFailedOrderMessageRepository>();

            return services;
        }

        private static IServiceCollection UseCassandra(this IServiceCollection services, DatabaseSettings databaseSettings)
        {
            services.AddSingleton<CassandraContext>(_ => new CassandraContext(databaseSettings));

            services.AddScoped<IOrderRepository, CassandraOrderRepository>();
            services.AddScoped<IFailedOrderMessageRepository, CassandraFailedOrderMessageRepository>();

            return services;
        }

        public static async Task MigrateDatabaseAsync(this IHost host)
        {
            using var scope = host.Services.CreateScope();
            var services = scope.ServiceProvider;
            var logger = services.GetRequiredService<ILogger<Program>>();
            var databaseSettings = services.GetRequiredService<IOptions<DatabaseSettings>>();

            try
            {
                logger.LogInformation("Applying database migrations...");

                switch (databaseSettings.Value.Type)
                {
                    case DatabaseType.Postgres:
                        var dbContext = services.GetRequiredService<AppDbContext>();
                        await dbContext.Database.MigrateAsync();
                        break;

                    case DatabaseType.Cassandra:
                        Console.WriteLine("Cassandra scripts should be applied manually");
                        break;

                    default:
                        throw new InvalidOperationException($"Unsupported database type: {databaseSettings.Value.Type}");
                }

                logger.LogInformation("Database migrations applied successfully");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while migrating the database");
                throw;
            }
        }
    }
}
