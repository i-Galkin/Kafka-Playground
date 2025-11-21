using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using OrderService.Applications.Services;
using OrderService.Applications.Validators;
using OrderService.Data.Cassandra.Repositories;
using OrderService.Domain.Enums;
using OrderService.Domain.Messages;
using OrderService.Infrastructure.Kafka;
using OrderService.Tests.Fixtures;

namespace OrderService.Tests.Integration;

public class CassandraOrderProcessorIntegrationTests : IClassFixture<CassandraTestFixture>, IAsyncLifetime
{
    private readonly CassandraTestFixture _fixture;
    private readonly CassandraOrderRepository _orderRepository;
    private readonly CassandraFailedOrderMessageRepository _failedOrderMessageRepository;
    private readonly OrderProcessor _processor;

    public CassandraOrderProcessorIntegrationTests(CassandraTestFixture fixture)
    {
        _fixture = fixture;
        _orderRepository = new CassandraOrderRepository(_fixture.Context);
        _failedOrderMessageRepository = new CassandraFailedOrderMessageRepository(_fixture.Context);

        var loggerMock = new Mock<ILogger<OrderProcessor>>();
        var dlqLoggerMock = new Mock<ILogger<DeadLetterQueueService>>();
        var retryLoggerMock = new Mock<ILogger<RetryManager>>();
        var environmentMock = new Mock<IHostEnvironment>();

        environmentMock.Setup(e => e.ApplicationName).Returns("CassandraIntegrationTestConsumer");

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                { "RetryPolicy:MaxRetries", "3" },
                { "RetryPolicy:InitialDelaySeconds", "0" }
            })
            .Build();

        var retryManager = new RetryManager(configuration, retryLoggerMock.Object);
        var deadLetterQueue = new DeadLetterQueueService(
            dlqLoggerMock.Object,
            environmentMock.Object,
            _failedOrderMessageRepository
        );
        var orderValidator = new OrderValidator();

        _processor = new OrderProcessor(
            retryManager,
            deadLetterQueue,
            loggerMock.Object,
            orderValidator,
            _orderRepository
        );
    }

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _fixture.CleanupAsync();
    }

    [Fact]
    public async Task ProcessAsync_ShouldSaveOrderToCassandra()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = $"CASS-INT-{Guid.NewGuid()}",
            CustomerId = "CASS-CUSTOMER-001",
            Amount = 350.99m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            1000,
            orderMessage.OrderId,
            orderMessage);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        var savedOrder = await _orderRepository.GetByOrderId(orderMessage.OrderId, CancellationToken.None);
        savedOrder.Should().NotBeNull();
        savedOrder.CustomerId.Should().Be("CASS-CUSTOMER-001");
        savedOrder.Amount.Should().Be(350.99m);
        savedOrder.Status.Should().Be(OrderStatus.Created);
        savedOrder.Partition.Should().Be(0);
        savedOrder.Offset.Should().Be(1000);
    }

    [Fact]
    public async Task ProcessAsync_ShouldUpdateExistingOrderInCassandra()
    {
        // Arrange
        var orderId = $"CASS-UPD-{Guid.NewGuid()}";

        var firstMessage = new OrderMessage
        {
            OrderId = orderId,
            CustomerId = "CASS-CUSTOMER-002",
            Amount = 150.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var firstConsumeResult = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            2000,
            orderId,
            firstMessage);
        await _processor.ProcessAsync(firstConsumeResult, CancellationToken.None);

        var updatedMessage = new OrderMessage
        {
            OrderId = orderId,
            CustomerId = "CASS-CUSTOMER-002",
            Amount = 200.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Completed"
        };

        var updatedConsumeResult = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            2001,
            orderId,
            updatedMessage);

        // Act
        await _processor.ProcessAsync(updatedConsumeResult, CancellationToken.None);

        // Assert
        var savedOrder = await _orderRepository.GetByOrderId(orderId, CancellationToken.None);
        savedOrder.Should().NotBeNull();
        savedOrder.Amount.Should().Be(200.00m);
        savedOrder.Status.Should().Be(OrderStatus.Completed);
        savedOrder.Offset.Should().Be(2001);
    }

    [Fact]
    public async Task ProcessAsync_ShouldSendToDLQWhenValidationFails()
    {
        // Arrange
        var invalidMessage = new OrderMessage
        {
            OrderId = $"CASS-INV-{Guid.NewGuid()}",
            CustomerId = "CASS-CUSTOMER-003",
            Amount = -75.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            3000,
            invalidMessage.OrderId,
            invalidMessage);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>();

        var savedOrder = await _orderRepository.GetByOrderId(invalidMessage.OrderId, CancellationToken.None);
        savedOrder.Should().BeNull();
    }

    [Fact]
    public async Task ProcessAsync_ShouldProcessMultipleOrdersInCassandra()
    {
        // Arrange
        var orders = new[]
        {
            new OrderMessage
            {
                OrderId = $"CASS-MULTI-{Guid.NewGuid()}",
                CustomerId = "CUSTOMER-004",
                Amount = 60m,
                CreatedAt = DateTime.UtcNow,
                Status = "Created"
            },
            new OrderMessage
            {
                OrderId = $"CASS-MULTI-{Guid.NewGuid()}",
                CustomerId = "CUSTOMER-005",
                Amount = 85m,
                CreatedAt = DateTime.UtcNow,
                Status = "Processing"
            },
            new OrderMessage
            {
                OrderId = $"CASS-MULTI-{Guid.NewGuid()}",
                CustomerId = "CUSTOMER-006",
                Amount = 110m,
                CreatedAt = DateTime.UtcNow,
                Status = "Completed"
            }
        };

        // Act
        var offset = 4000L;
        foreach (var order in orders)
        {
            var consumeResult = CreateConsumeResult(
                "cassandra-orders-topic",
                0,
                offset,
                order.OrderId,
                order);
            await _processor.ProcessAsync(consumeResult, CancellationToken.None);
            offset++;
        }

        // Assert
        foreach (var order in orders)
        {
            var savedOrder = await _orderRepository.GetByOrderId(order.OrderId, CancellationToken.None);
            savedOrder.Should().NotBeNull();
            savedOrder.CustomerId.Should().Be(order.CustomerId);
        }
    }

    [Fact]
    public async Task ProcessAsync_ShouldHandleAllOrderStatusesInCassandra()
    {
        // Arrange
        var statuses = new[] { "Created", "Processing", "Completed", "Failed" };

        // Act & Assert
        var offset = 5000L;
        foreach (var status in statuses)
        {
            var orderMessage = new OrderMessage
            {
                OrderId = $"CASS-STATUS-{status}-{Guid.NewGuid()}",
                CustomerId = $"CUSTOMER-{status}",
                Amount = 120m,
                CreatedAt = DateTime.UtcNow,
                Status = status
            };

            var consumeResult = CreateConsumeResult(
                "cassandra-orders-topic",
                0,
                offset,
                orderMessage.OrderId,
                orderMessage);
            await _processor.ProcessAsync(consumeResult, CancellationToken.None);

            var savedOrder = await _orderRepository.GetByOrderId(orderMessage.OrderId, CancellationToken.None);
            savedOrder.Should().NotBeNull();
            savedOrder.Status.Should().Be(Enum.Parse<OrderStatus>(status, true));
            offset++;
        }
    }

    [Fact]
    public async Task ProcessAsync_ShouldSaveDifferentPartitionsAndOffsetsInCassandra()
    {
        // Arrange
        var orderMessage1 = new OrderMessage
        {
            OrderId = $"CASS-PART-{Guid.NewGuid()}",
            CustomerId = "CUSTOMER-007",
            Amount = 130m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var orderMessage2 = new OrderMessage
        {
            OrderId = $"CASS-PART-{Guid.NewGuid()}",
            CustomerId = "CUSTOMER-008",
            Amount = 240m,
            CreatedAt = DateTime.UtcNow,
            Status = "Processing"
        };

        var consumeResult1 = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            6000,
            orderMessage1.OrderId,
            orderMessage1);
        var consumeResult2 = CreateConsumeResult(
            "cassandra-orders-topic",
            1,
            7000,
            orderMessage2.OrderId,
            orderMessage2);

        // Act
        await _processor.ProcessAsync(consumeResult1, CancellationToken.None);
        await _processor.ProcessAsync(consumeResult2, CancellationToken.None);

        // Assert
        var order1 = await _orderRepository.GetByOrderId(orderMessage1.OrderId, CancellationToken.None);
        order1.Should().NotBeNull();
        order1.Partition.Should().Be(0);
        order1.Offset.Should().Be(6000);

        var order2 = await _orderRepository.GetByOrderId(orderMessage2.OrderId, CancellationToken.None);
        order2.Should().NotBeNull();
        order2.Partition.Should().Be(1);
        order2.Offset.Should().Be(7000);
    }

    [Fact]
    public async Task ProcessAsync_ShouldHandleEmptyCustomerIdInCassandra()
    {
        // Arrange
        var invalidMessage = new OrderMessage
        {
            OrderId = $"CASS-EMPTY-{Guid.NewGuid()}",
            CustomerId = "",
            Amount = 140m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            8000,
            invalidMessage.OrderId,
            invalidMessage);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Customer ID is required*");
    }

    [Fact]
    public async Task ProcessAsync_ShouldHandleInvalidStatusInCassandra()
    {
        // Arrange
        var invalidMessage = new OrderMessage
        {
            OrderId = $"CASS-BAD-STATUS-{Guid.NewGuid()}",
            CustomerId = "CUSTOMER-010",
            Amount = 150m,
            CreatedAt = DateTime.UtcNow,
            Status = "InvalidStatusValue"
        };

        var consumeResult = CreateConsumeResult(
            "cassandra-orders-topic",
            0,
            9000,
            invalidMessage.OrderId,
            invalidMessage);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Invalid order status*");
    }

    [Fact]
    public async Task ProcessAsync_ShouldQueryOrdersByDateRange()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var orders = new[]
        {
            new OrderMessage
            {
                OrderId = $"CASS-DATE-{Guid.NewGuid()}",
                CustomerId = "CUSTOMER-011",
                Amount = 160m,
                CreatedAt = now.AddDays(-3),
                Status = "Created"
            },
            new OrderMessage
            {
                OrderId = $"CASS-DATE-{Guid.NewGuid()}",
                CustomerId = "CUSTOMER-012",
                Amount = 170m,
                CreatedAt = now.AddDays(-1),
                Status = "Processing"
            },
            new OrderMessage
            {
                OrderId = $"CASS-DATE-{Guid.NewGuid()}",
                CustomerId = "CUSTOMER-013",
                Amount = 180m,
                CreatedAt = now,
                Status = "Completed"
            }
        };

        var offset = 10000L;
        foreach (var order in orders)
        {
            var consumeResult = CreateConsumeResult(
                "cassandra-orders-topic",
                0,
                offset,
                order.OrderId,
                order);
            await _processor.ProcessAsync(consumeResult, CancellationToken.None);
            offset++;
        }

        // Act
        var result = await _orderRepository.GetListByDateRange(
            now.AddDays(-2),
            now.AddHours(1),
            CancellationToken.None);

        // Assert
        result.Should().HaveCountGreaterOrEqualTo(2);
        var recentOrders = result.Where(o =>
            o.OrderId.StartsWith("CASS-DATE") &&
            o.CreatedAt >= now.AddDays(-2)).ToList();
        recentOrders.Should().HaveCountGreaterOrEqualTo(2);
    }

    private ConsumeResult<string, OrderMessage> CreateConsumeResult(
        string topic, int partition, long offset, string key, OrderMessage value)
    {
        return new ConsumeResult<string, OrderMessage>
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, OrderMessage>
            {
                Key = key,
                Value = value
            }
        };
    }
}

