using Microsoft.EntityFrameworkCore;

namespace Consumer.Data
{
    public class DbContextFactory
    {
        private static DbContextOptions<AppDbContext> _options;

        public static void Initialize(string connectionString)
        {
            var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
            optionsBuilder.UseNpgsql(connectionString);
            _options = optionsBuilder.Options;
        }

        public static AppDbContext Create()
        {
            if (_options == null)
            {
                throw new InvalidOperationException("DbContextFactory is not initialized. Call Initialize() first.");
            }

            return new AppDbContext(_options);
        }
    }
}
