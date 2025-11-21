namespace OrderService.Infrastructure.Configuration.Database;

public class CassandraSettings
{
    public int Port { get; set; } = 9042;
    public string Keyspace { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
    public string LocalDatacenter { get; set; }
    public string[] ContactPoints { get; set; }
}