using Cassandra;
using Cassandra.Mapping;
using OrderService.Infrastructure.Configuration.Database;

namespace OrderService.Data.Cassandra;

public class CassandraContext : IDisposable
{
    private readonly Cluster _cluster;
    private readonly ISession _session;
    private readonly IMapper _mapper;

    public ISession Session => _session;
    public IMapper Mapper => _mapper;

    public CassandraContext(DatabaseSettings settings)
    {
        if (settings.CassandraSettings == null)
        {
            throw new ArgumentNullException(nameof(settings.CassandraSettings));
        }

        var cassandraSettings = settings.CassandraSettings;

        _cluster = Cluster.Builder()
            .AddContactPoints(cassandraSettings.ContactPoints)
            .WithPort(cassandraSettings.Port)
            .WithDefaultKeyspace(cassandraSettings.Keyspace)
            .WithCredentials(cassandraSettings.Username, cassandraSettings.Password)
            .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy(cassandraSettings.LocalDatacenter))
            .Build();

        _session = _cluster.Connect(cassandraSettings.Keyspace);

        MappingConfiguration.Global.Define<CassandraMappings>();
        _mapper = new Mapper(_session);
    }

    public void Dispose()
    {
        _session?.Dispose();
        _cluster?.Dispose();
    }
}