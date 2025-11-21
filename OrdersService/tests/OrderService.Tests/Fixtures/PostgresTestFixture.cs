using Microsoft.EntityFrameworkCore;
using OrderService.Data.Postgres;
using Testcontainers.PostgreSql;

namespace OrderService.Tests.Fixtures;

public class PostgresTestFixture : IAsyncLifetime
{
    private PostgreSqlContainer _postgresContainer;
    public AppDbContext Context { get; private set; }

    public async Task InitializeAsync()
    {
        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithDatabase("test_db")
            .WithUsername("test_user")
            .WithPassword("test_password")
            .Build();

        await _postgresContainer.StartAsync();

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(_postgresContainer.GetConnectionString())
            .Options;

        Context = new AppDbContext(options);
        await Context.Database.EnsureCreatedAsync();
    }

    public async Task DisposeAsync()
    {
        if (Context != null)
        {
            await Context.Database.EnsureDeletedAsync();
            await Context.DisposeAsync();
        }

        if (_postgresContainer != null)
        {
            await _postgresContainer.DisposeAsync();
        }
    }

    public async Task CleanupAsync()
    {
        if (Context != null)
        {
            Context.Orders.RemoveRange(Context.Orders);
            Context.FailedOrderMessages.RemoveRange(Context.FailedOrderMessages);
            await Context.SaveChangesAsync();
        }
    }
}

