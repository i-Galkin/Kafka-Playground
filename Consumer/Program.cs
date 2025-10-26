using System.Text.Json;
using Confluent.Kafka;
using Consumer.Configurations;
using Consumer.Data;
using Consumer.Data.Models;
using Consumer.Deserializers;
using Consumer.Kafka;
using Consumer.Messages;
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

                var retryPolicy = new RetryPolicy(maxRetries: 3, initialDelay: TimeSpan.FromSeconds(2));
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

                            bool shouldCommit = false;
                            try
                            {
                                await retryPolicy.ExecuteAsync(async () =>
                                {
                                    Console.WriteLine($"[{consumerName}] Order {orderMessage.OrderId} processing...");

                                    using var dbContext = DbContextFactory.Create();

                                    var existingOrder = await dbContext.Orders.FirstOrDefaultAsync(o => o.OrderId == orderMessage.OrderId);
                                    if (existingOrder != null)
                                    {
                                        Console.WriteLine($"[{consumerName}] Order #{orderMessage.OrderId} already exists in DB, skipping...");
                                        return true;
                                    }

                                    // DLQ test
                                    if (orderMessage.Amount <= 0)
                                    {
                                        throw new InvalidOperationException($"[{consumerName}] Order #{orderMessage.OrderId} Order amount must be positive");
                                    }

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

                                    return true;

                                });

                                shouldCommit = true;

                                consumer.Commit(consumeResult);
                                Console.WriteLine($"[{consumerName}] Offset committed");
                                Console.WriteLine($"[{consumerName}] Order {orderMessage.OrderId} processed.");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{consumerName}] Failed to process order after retries");
                                Console.WriteLine($"[{consumerName}] Error: {ex.Message}");

                                try
                                {
                                    using var dbContext = DbContextFactory.Create();

                                    var failedMessage = new FailedOrderMessage
                                    {
                                        Topic = consumeResult.Topic,
                                        Partition = consumeResult.Partition.Value,
                                        Offset = consumeResult.Offset.Value,
                                        Key = consumeResult.Message.Key,
                                        Value = JsonSerializer.Serialize(orderMessage),
                                        ErrorMessage = ex.Message,
                                        StackTrace = ex.StackTrace,
                                        RetryCount = 3, // Max retries reached
                                        FailedAt = DateTime.UtcNow,
                                        ConsumerName = consumerName
                                    };

                                    dbContext.FailedOrderMessages.Add(failedMessage);
                                    await dbContext.SaveChangesAsync();

                                    Console.WriteLine($"[{consumerName}] Message sent to DLQ");

                                    shouldCommit = true;
                                }
                                catch (Exception dlqEx)
                                {
                                    Console.WriteLine($"[{consumerName}] Failed to save to DLQ: {dlqEx.Message}");
                                    shouldCommit = false;
                                }
                            }

                            if (shouldCommit)
                            {
                                consumer.Commit(consumeResult);
                                Console.WriteLine($"[{consumerName}] Offset committed");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[{consumerName}] Waiting for orders...");
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
