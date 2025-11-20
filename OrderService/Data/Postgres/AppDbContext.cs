using Microsoft.EntityFrameworkCore;
using OrderService.Domain.Models;

namespace OrderService.Data.Postgres
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        public DbSet<Order> Orders => Set<Order>();
        public DbSet<FailedOrderMessage> FailedOrderMessages => Set<FailedOrderMessage>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Order>()
                .Property(o => o.Status)
                .HasConversion<int>();

            modelBuilder.Entity<Order>()
                .HasIndex(o => o.CustomerId)
                .IsUnique()
                .HasDatabaseName("idx_orders_customer_id");

            modelBuilder.Entity<Order>()
                .HasIndex(o => o.CreatedAt)
                .HasDatabaseName("idx_orders_created_at");

            modelBuilder.Entity<FailedOrderMessage>()
                .HasIndex(f => f.FailedAt)
                .HasDatabaseName("idx_failed_orders_messages_failed_at");

            modelBuilder.Entity<FailedOrderMessage>()
                .HasIndex(f => new { f.Topic, f.Partition, f.Offset })
                .HasDatabaseName("idx_failed_orders_messages_topic_partition_offset");
        }
    }
}
