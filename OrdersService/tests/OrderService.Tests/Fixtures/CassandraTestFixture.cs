using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
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
        var testProjectRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
        var cassandraConfigPath = Path.Combine(testProjectRoot, "cassandra.yaml");

        if (!File.Exists(cassandraConfigPath))
        {
            throw new FileNotFoundException($"cassandra.yaml not found at '{cassandraConfigPath}'");
        }

        var configDir = Path.GetDirectoryName(cassandraConfigPath);

        _cassandraContainer = new CassandraBuilder()
            .WithImage("cassandra:5.0")
            .WithEnvironment("JVM_EXTRA_OPTS", "-Dcassandra.experimental=enabled")
            .WithBindMount(configDir, "/tmp/cassandra-config", AccessMode.ReadOnly)
            .WithEntrypoint("/bin/bash", "-c",
                "cp /tmp/cassandra-config/cassandra.yaml /etc/cassandra/cassandra.yaml && " +
                "chown cassandra:cassandra /etc/cassandra/cassandra.yaml && " +
                "exec /usr/local/bin/docker-entrypoint.sh cassandra -f")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Startup complete"))
            .WithCleanUp(true)
            .Build();

        await _cassandraContainer.StartAsync();

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
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id text,
                created_at timestamp,
                customer_id text,
                amount decimal,
                status int,
                processed_at timestamp,
                partition int,
                offset bigint,
                PRIMARY KEY (order_id, created_at)
            ) WITH CLUSTERING ORDER BY (created_at DESC)
            """
        ));

        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement(
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS orders_by_customer AS
            SELECT * FROM orders
            WHERE customer_id IS NOT NULL
              AND order_id IS NOT NULL
              AND created_at IS NOT NULL
            PRIMARY KEY (customer_id, created_at, order_id)
            WITH CLUSTERING ORDER BY (created_at DESC, order_id DESC)
            """
        ));
    }

    private async Task CreateFailedOrderMessagesTableAsync()
    {
        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement(
            """
            CREATE TABLE IF NOT EXISTS failed_order_messages (
                topic text,
                partition int,
                offset bigint,
                key text,
                value text,
                error_message text,
                stack_trace text,
                retry_count int,
                failed_at timestamp,
                consumer_name text,
                PRIMARY KEY ((topic, partition), failed_at, offset)
            ) WITH CLUSTERING ORDER BY (failed_at DESC, offset DESC)
            """
        ));

        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement(
            """
            CREATE TABLE IF NOT EXISTS failed_messages_by_consumer (
                consumer_name text,
                failed_at timestamp,
                topic text,
                partition int,
                offset bigint,
                key text,
                value text,
                error_message text,
                stack_trace text,
                retry_count int,
                PRIMARY KEY (consumer_name, failed_at, topic, partition, offset)
            ) WITH CLUSTERING ORDER BY (failed_at DESC)
            """
        ));

        await Context.Session.ExecuteAsync(new Cassandra.SimpleStatement(
            """
            CREATE TABLE IF NOT EXISTS failed_order_messages_by_topic (
                topic text,
                failed_at timestamp,
                consumer_name text,
                partition int,
                offset bigint,
                key text,
                value text,
                error_message text,
                stack_trace text,
                retry_count int,
                PRIMARY KEY (topic, failed_at, consumer_name, partition, offset)
            ) WITH CLUSTERING ORDER BY (failed_at DESC, consumer_name ASC, partition ASC, offset DESC)
            """
        ));
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
        await Context.Session.ExecuteAsync(
            new Cassandra.SimpleStatement("TRUNCATE test_keyspace.orders"));
        await Context.Session.ExecuteAsync(
            new Cassandra.SimpleStatement("TRUNCATE test_keyspace.failed_order_messages"));
        await Context.Session.ExecuteAsync(
            new Cassandra.SimpleStatement("TRUNCATE test_keyspace.failed_messages_by_consumer"));
        await Context.Session.ExecuteAsync(
            new Cassandra.SimpleStatement("TRUNCATE test_keyspace.failed_order_messages_by_topic"));
    }
}

