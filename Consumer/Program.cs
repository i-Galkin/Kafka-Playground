using Confluent.Kafka;
using Consumer.Deserializers;
using Producer;

namespace Consumer
{
    internal class Program
    {
        static async Task Main(string[] args)
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

            Console.WriteLine($"[{consumerName}] Configs:");
            Console.WriteLine($"[{consumerName}]   BootstrapServers: {config.BootstrapServers}");
            Console.WriteLine($"[{consumerName}]   GroupId: {config.GroupId}");

            IConsumer<string, Order> consumer = null;
            try
            {
                Console.WriteLine($"[{consumerName}] Create Consumer...");
                consumer = new ConsumerBuilder<string, Order>(config)
                    .SetValueDeserializer(new JsonDeserializer<Order>())
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
                            var order = consumeResult.Message.Value;
                            Console.WriteLine($"[{consumerName}] Received order: Id={order.OrderId}, CustomerId={order.CustomerId}," +
                                $" Amount={order.Amount}, CreatedAt={order.CreatedAt}, Status={order.Status}");

                            try
                            {
                                Console.WriteLine($"[{consumerName}] Order {order.OrderId} processing...");
                                await Task.Delay(1000);
                                Console.WriteLine($"[{consumerName}] Order {order.OrderId} processed.");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{consumerName}] Error processing order {order.OrderId}: {ex.Message}");
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
