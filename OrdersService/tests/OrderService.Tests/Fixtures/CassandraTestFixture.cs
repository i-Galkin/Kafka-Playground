using OrderService.Data.Cassandra;
using OrderService.Infrastructure.Configuration.Database;
using Testcontainers.Cassandra;

namespace OrderService.Tests.Fixtures;

public class CassandraTestFixture : IAsyncLifetime
{
    private CassandraContainer _cassandraContainer;
    public CassandraContext Context { get; private set; }

    public async Task InitializeAsync()
    {
        _cassandraContainer = new CassandraBuilder()
            .WithImage("cassandra:5.0")
            .Build();

        await _cassandraContainer.StartAsync();

        // First connect without keyspace to create it
        var cluster = Cassandra.Cluster.Builder()
            .AddContactPoint(_cassandraContainer.Hostname)
            .WithPort(_cassandraContainer.GetMappedPublicPort(9042))
            .WithCredentials("cassandra", "cassandra")
            .WithLoadBalancingPolicy(new Cassandra.DCAwareRoundRobinPolicy("dc1"))
            .Build();

        var tempSession = await cluster.ConnectAsync();

        await tempSession.ExecuteAsync(new Cassandra.SimpleStatement(
            "CREATE KEYSPACE IF NOT EXISTS test_keyspace " +
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));

        tempSession.Dispose();
        cluster.Dispose();

        // Now create context with the keyspace
        var settings = new DatabaseSettings
        {
            Type = DatabaseType.Cassandra,
            CassandraSettings = new CassandraSettings
            {
                ContactPoints = [_cassandraContainer.Hostname],
                Port = _cassandraContainer.GetMappedPublicPort(9042),
                Keyspace = "test_keyspace",
                LocalDatacenter = "dc1",
                Username = "cassandra",
                Password = "cassandra"
            }
        };

        Context = new CassandraContext(settings);

        await CreateOrdersTableAsync();
        await CreateFailedOrderMessagesTableAsync();
    }

    private async Task CreateOrdersTableAsync()
    {
        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement(
            "CREATE TABLE IF NOT EXISTS test_keyspace.orders (" +
            "order_id text PRIMARY KEY, " +
            "customer_id text, " +
            "amount decimal, " +
            "created_at timestamp, " +
            "status int, " +
            "processed_at timestamp, " +
            "partition int, " +
            "offset bigint)"));
    }

    private async Task CreateFailedOrderMessagesTableAsync()
    {
        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement(
            "CREATE TABLE IF NOT EXISTS test_keyspace.failed_order_messages (" +
            "id uuid PRIMARY KEY, " +
            "topic text, " +
            "partition int, " +
            "offset bigint, " +
            "key text, " +
            "value text, " +
            "error_message text, " +
            "stack_trace text, " +
            "retry_count int, " +
            "failed_at timestamp, " +
            "consumer_name text)"));
    }

    public async Task DisposeAsync()
    {
        Context?.Dispose();

        if (_cassandraContainer != null)
        {
            await _cassandraContainer.DisposeAsync();
        }
    }

    public async Task CleanupAsync()
    {
        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement("TRUNCATE test_keyspace.orders"));
        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement("TRUNCATE test_keyspace.failed_order_messages"));
    }
}

