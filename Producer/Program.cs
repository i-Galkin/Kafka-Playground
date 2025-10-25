using System.Runtime.InteropServices;
using Confluent.Kafka;
using Producer.Serializer;

namespace Producer
{
    internal class Program
    {
        static async Task Main()
        {
            Console.WriteLine("Producer started...");

            var config = new ProducerConfig
            {
                BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS"),

                // * Acknowledgment settings for message durability *
                Acks = Acks.All,

                // Additional configurations can be added here as needed
                // * Every producer has it own id, every message has its own sequence number *
                // * Kafka will ignore the same messages with the same sequence number for the same producer id *
                EnableIdempotence = true,

                // * Transactions require idempotence to be enabled *
                // * All messages sent in the same transaction will be committed or aborted as a single unit *
                // TransactionalId = "my-unique-transaction-id",
            };

            // Wait for Consumers
            await Task.Delay(20000);

            using var producer = new ProducerBuilder<string, Order>(config)
                .SetValueSerializer(new JsonSerializer<Order>())
                .Build();

            var customers = new List<string>{ "customer-1", "customer-2", "customer-3" };
            try
            {
 
                foreach (var i in Enumerable.Range(0, 50))
                {
                    var customerId = customers[i % customers.Count];

                    var order = new Order
                    {
                        OrderId = i,
                        CustomerId = customerId,
                        Amount = new Random().Next(100, 1000),
                        CreatedAt = DateTime.UtcNow,
                        Status = "Created",
                    };

                    var message = new Message<string, Order>
                    {
                        Key = customerId,
                        Value = order,
                    };

                    var result = await producer.ProduceAsync("test-topic", message);

                    Console.WriteLine($"Produced message to topic {result.Topic}, partition: {result.Partition.Value}, message offset: {result.Offset}," +
                        $" topic partition offset: {result.TopicPartitionOffset.Offset}, message key: {result.Message.Key} , message content: {result.Message.Value}");

                    await Task.Delay(2000);
                }
            }
            catch (ProduceException<string, string> ex)
            {
                Console.WriteLine($"Producer: An error occurred: {ex.Error.Reason}");
            }

            Console.WriteLine("Producer died....");
        }
    }
}
