using Consumer.Configurations;
using Microsoft.EntityFrameworkCore.Design;

namespace Consumer.Data
{
    /// <summary>
    /// Design-time factory used by migrations
    /// </summary>
    public class AppDesignTimeDbContextFactory : IDesignTimeDbContextFactory<AppDbContext>
    {
        public AppDbContext CreateDbContext(string[] args)
        {
            DbContextFactory.Initialize(PostgreConfiguration.GetConnectionString());

            return DbContextFactory.Create();
        }
    }
}
