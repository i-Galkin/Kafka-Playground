namespace OrderService.Infrastructure.Configuration.Database;

public class DatabaseSettings
{
    public const string SectionName = "Database";

    public DatabaseType Type { get; set; } = DatabaseType.Postgres;
    public string ConnectionString { get; set; }

    public CassandraSettings CassandraSettings { get; set; }
}