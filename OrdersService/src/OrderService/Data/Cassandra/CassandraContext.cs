using Cassandra;
using Cassandra.Mapping;
using OrderService.Infrastructure.Configuration.Database;

namespace OrderService.Data.Cassandra;

public class CassandraContext : IDisposable
{
    private static bool _mappingsDefined;
    private static readonly object _mappingsLock = new();

    private readonly Cluster _cluster;
    private readonly ISession _session;
    private readonly IMapper _mapper;

    public ISession Session => _session;
    public IMapper Mapper => _mapper;

    public CassandraContext(DatabaseSettings settings)
    {
        if (settings.CassandraSettings == null)
        {
            throw new ArgumentException("CassandraSettings cannot be null", nameof(settings));
        }

        var cassandraSettings = settings.CassandraSettings;

        _cluster = Cluster.Builder()
            .AddContactPoints(cassandraSettings.ContactPoints)
            .WithPort(cassandraSettings.Port)
            .WithDefaultKeyspace(cassandraSettings.Keyspace)
            .WithCredentials(cassandraSettings.Username, cassandraSettings.Password)
            .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy(cassandraSettings.LocalDatacenter))
            .WithQueryOptions(new QueryOptions()
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum))
            .Build();

        _session = _cluster.Connect(cassandraSettings.Keyspace);

        DefineMappings();

        _mapper = new Mapper(_session);
    }

    private static void DefineMappings()
    {
        if (_mappingsDefined)
        {
            return;
        }

        lock (_mappingsLock)
        {
            if (_mappingsDefined)
            {
                return;
            }

            MappingConfiguration.Global.Define<CassandraMappings>();
            _mappingsDefined = true;
        }
    }

    public void Dispose()
    {
        _session?.Dispose();
        _cluster?.Dispose();
    }
}