using Confluent.Kafka;
using Consumer.Configurations;
using Consumer.Data;
using Consumer.Data.Models;
using Consumer.Deserializers;
using Consumer.Messages;
using Consumer.Models;
using Microsoft.EntityFrameworkCore;

namespace Consumer
{
    public class Program
    {
        private static async Task Main()
        {
            var consumerName = Environment.MachineName;
            Console.WriteLine($"[{consumerName}] ========================================");
            Console.WriteLine($"[{consumerName}] Consumer starts...");
            Console.WriteLine($"[{consumerName}] ========================================");

            var config = new ConsumerConfig
            {
                // Kafka bootstrap servers from environment variable. KAFKA_LISTENERS: INNER
                BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS"),
                // Set to earliest to consume from the beginning of the topic if no offset is present
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Consumer group id
                GroupId = "orders-processing-group",
                // Enable auto commit of offsets. Offsets will be committed automatically at the interval set by AutoCommitIntervalMs
                // If false, offsets need to be committed manually using consumer.Commit()
                EnableAutoCommit = false,

                // Additional configurations can be added here as needed
                // IsolationLevel = IsolationLevel.ReadCommitted,
            };

            DbContextFactory.Initialize(PostgreConfiguration.GetConnectionString());

            Console.WriteLine($"[{consumerName}] Applying database migrations...");
            using (var dbContext = DbContextFactory.Create())
            {
                try
                {
                    dbContext.Database.Migrate();
                    Console.WriteLine($"[{consumerName}] Migrations applied successfully");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{consumerName}] Failed to apply migrations: {ex.Message}");
                    throw;
                }
            }


            Console.WriteLine($"[{consumerName}] Configs:");
            Console.WriteLine($"[{consumerName}]   BootstrapServers: {config.BootstrapServers}");
            Console.WriteLine($"[{consumerName}]   GroupId: {config.GroupId}");

            IConsumer<string, OrderMessage> consumer = null;
            try
            {
                Console.WriteLine($"[{consumerName}] Create Consumer...");
                consumer = new ConsumerBuilder<string, OrderMessage>(config)
                    .SetValueDeserializer(new JsonDeserializer<OrderMessage>())
                    .SetErrorHandler((_, e) =>
                    {
                        Console.WriteLine($"[{consumerName}] Error: {e.Reason}");
                    })
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        var partitionsList = string.Join(", ", partitions.Select(p => $"P{p.Partition}"));
                        Console.WriteLine($"[{consumerName}] Assigned partitions: {partitionsList}");
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        var partitionsList = string.Join(", ", partitions.Select(p => $"P{p.Partition}"));
                        Console.WriteLine($"[{consumerName}] Revoked partitions: {partitionsList}");
                    })
                    .Build();

                Console.WriteLine($"[{consumerName}] Consumer was successfuly created!");
                Console.WriteLine($"[{consumerName}] Subscribe to test-topic...");

                consumer.Subscribe("test-topic");

                Console.WriteLine($"[{consumerName}] Successfuly subscribed, started listening");
                Console.WriteLine($"[{consumerName}] ========================================");

                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
                        if (consumeResult != null)
                        {
                            var orderMessage = consumeResult.Message.Value;
                            Console.WriteLine($"[{consumerName}] Received order: Id={orderMessage.OrderId}, CustomerId={orderMessage.CustomerId}," +
                                $" Amount={orderMessage.Amount}, CreatedAt={orderMessage.CreatedAt}, Status={orderMessage.Status}");

                            try
                            {
                                Console.WriteLine($"[{consumerName}] Order {orderMessage.OrderId} processing...");

                                using var dbContext = DbContextFactory.Create();
                                var existingOrder = await dbContext.Orders.FirstOrDefaultAsync(o => o.OrderId == orderMessage.OrderId);
                                if (existingOrder != null)
                                {
                                    Console.WriteLine($"[{consumerName}] Order #{orderMessage.OrderId} already exists in DB, skipping...");
                                }
                                else
                                {
                                    var orderEntity = new Order
                                    {
                                        OrderId = orderMessage.OrderId,
                                        CustomerId = orderMessage.CustomerId,
                                        Amount = orderMessage.Amount,
                                        CreatedAt = orderMessage.CreatedAt,
                                        Status = orderMessage.Status,
                                        ProcessedAt = DateTime.UtcNow,
                                        Partition = consumeResult.Partition.Value,
                                        Offset = consumeResult.Offset.Value
                                    };

                                    dbContext.Orders.Add(orderEntity);
                                    await dbContext.SaveChangesAsync();

                                    Console.WriteLine($"[{consumerName}] Order #{orderMessage.OrderId} saved to database");
                                }

                                consumer.Commit(consumeResult);
                                Console.WriteLine($"[{consumerName}] Offset committed");
                                Console.WriteLine($"[{consumerName}] Order {orderMessage.OrderId} processed.");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{consumerName}] Error processing order {orderMessage.OrderId}: {ex.Message}");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[{consumerName}] no orders");
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"[{consumerName}] Consume error: {ex.Error.Reason}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{consumerName}] Critical error occurred: {ex.Message}");
            }
            finally
            {
                Console.WriteLine($"[{consumerName}] close the connection...");
                consumer?.Close();
                Console.WriteLine($"[{consumerName}] died");
            }
        }
    }
}
