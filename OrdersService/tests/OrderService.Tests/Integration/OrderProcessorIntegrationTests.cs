using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using OrderService.Applications.Services;
using OrderService.Applications.Validators;
using OrderService.Data.Postgres.Repositories;
using OrderService.Domain.Enums;
using OrderService.Domain.Messages;
using OrderService.Infrastructure.Kafka;
using OrderService.Tests.Fixtures;

namespace OrderService.Tests.Integration;

public class OrderProcessorIntegrationTests : IClassFixture<PostgresTestFixture>, IAsyncLifetime
{
    private readonly PostgresTestFixture _fixture;
    private readonly PostgresOrderRepository _orderRepository;
    private readonly PostgresFailedOrderMessageRepository _failedOrderMessageRepository;
    private readonly OrderProcessor _processor;
    private readonly Mock<ILogger<OrderProcessor>> _loggerMock;
    private readonly Mock<ILogger<DeadLetterQueueService>> _dlqLoggerMock;
    private readonly Mock<ILogger<RetryManager>> _retryLoggerMock;
    private readonly Mock<IHostEnvironment> _environmentMock;

    public OrderProcessorIntegrationTests(PostgresTestFixture fixture)
    {
        _fixture = fixture;
        _orderRepository = new PostgresOrderRepository(_fixture.Context);
        _failedOrderMessageRepository = new PostgresFailedOrderMessageRepository(_fixture.Context);

        _loggerMock = new Mock<ILogger<OrderProcessor>>();
        _dlqLoggerMock = new Mock<ILogger<DeadLetterQueueService>>();
        _retryLoggerMock = new Mock<ILogger<RetryManager>>();
        _environmentMock = new Mock<IHostEnvironment>();

        _environmentMock.Setup(e => e.ApplicationName).Returns("IntegrationTestConsumer");

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                { "RetryPolicy:MaxRetries", "3" },
                { "RetryPolicy:InitialDelaySeconds", "0" }
            }!)
            .Build();

        var retryManager = new RetryManager(configuration, _retryLoggerMock.Object);
        var deadLetterQueue = new DeadLetterQueueService(
            _dlqLoggerMock.Object,
            _environmentMock.Object,
            _failedOrderMessageRepository
        );
        var orderValidator = new OrderValidator();

        _processor = new OrderProcessor(
            retryManager,
            deadLetterQueue,
            _loggerMock.Object,
            orderValidator,
            _orderRepository
        );
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _fixture.CleanupAsync();
    }

    [Fact]
    public async Task ProcessAsync_ShouldSaveOrderToDatabase()
    {
        // Arrange

        var orderMessage = new OrderMessage
        {
            OrderId = "INT-ORDER-001",
            CustomerId = "INT-CUSTOMER-001",
            Amount = 250.75m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 0, 1000, "INT-ORDER-001", orderMessage);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        var savedOrder = await _orderRepository.GetByOrderId("INT-ORDER-001", CancellationToken.None);
        savedOrder.Should().NotBeNull();
        savedOrder!.CustomerId.Should().Be("INT-CUSTOMER-001");
        savedOrder.Amount.Should().Be(250.75m);
        savedOrder.Status.Should().Be(OrderStatus.Created);
        savedOrder.Partition.Should().Be(0);
        savedOrder.Offset.Should().Be(1000);
    }

    [Fact]
    public async Task ProcessAsync_ShouldUpdateExistingOrder()
    {
        // Arrange

        var firstMessage = new OrderMessage
        {
            OrderId = "INT-ORDER-002",
            CustomerId = "INT-CUSTOMER-002",
            Amount = 100.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var firstConsumeResult = CreateConsumeResult("orders-topic", 0, 2000, "INT-ORDER-002", firstMessage);
        await _processor.ProcessAsync(firstConsumeResult, CancellationToken.None);

        var updatedMessage = new OrderMessage
        {
            OrderId = "INT-ORDER-002",
            CustomerId = "INT-CUSTOMER-002",
            Amount = 150.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Completed"
        };

        var updatedConsumeResult = CreateConsumeResult("orders-topic", 0, 2001, "INT-ORDER-002", updatedMessage);

        // Act
        await _processor.ProcessAsync(updatedConsumeResult, CancellationToken.None);

        // Assert
        var savedOrder = await _orderRepository.GetByOrderId("INT-ORDER-002", CancellationToken.None);
        savedOrder.Should().NotBeNull();
        savedOrder!.Amount.Should().Be(150.00m);
        savedOrder.Status.Should().Be(OrderStatus.Completed);
        savedOrder.Offset.Should().Be(2001);
    }

    [Fact]
    public async Task ProcessAsync_ShouldSendToDLQ_WhenValidationFails()
    {
        // Arrange
        await _fixture.CleanupAsync();

        var invalidMessage = new OrderMessage
        {
            OrderId = "INT-ORDER-003",
            CustomerId = "INT-CUSTOMER-003",
            Amount = -50.00m, // Invalid amount
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 0, 3000, "INT-ORDER-003", invalidMessage);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>();

        // Verify order was not saved
        var savedOrder = await _orderRepository.GetByOrderId("INT-ORDER-003", CancellationToken.None);
        savedOrder.Should().BeNull();

        // Verify message was sent to DLQ
        var dlqMessages = await _failedOrderMessageRepository.GetByTopic("orders-topic", CancellationToken.None);
        dlqMessages.Should().Contain(m => m.Key == "INT-ORDER-003");
    }

    [Fact]
    public async Task ProcessAsync_ShouldProcessMultipleOrders()
    {
        // Arrange
        await _fixture.CleanupAsync();

        var orders = new[]
        {
            new OrderMessage { OrderId = "INT-ORDER-004", CustomerId = "CUSTOMER-004", Amount = 50m, CreatedAt = DateTime.UtcNow, Status = "Created" },
            new OrderMessage { OrderId = "INT-ORDER-005", CustomerId = "CUSTOMER-005", Amount = 75m, CreatedAt = DateTime.UtcNow, Status = "Processing" },
            new OrderMessage { OrderId = "INT-ORDER-006", CustomerId = "CUSTOMER-006", Amount = 100m, CreatedAt = DateTime.UtcNow, Status = "Completed" }
        };

        // Act
        for (int i = 0; i < orders.Length; i++)
        {
            var consumeResult = CreateConsumeResult("orders-topic", 0, 4000 + i, orders[i].OrderId, orders[i]);
            await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        }

        // Assert
        var savedOrders = await _orderRepository.GetList(CancellationToken.None);
        savedOrders.Should().HaveCountGreaterOrEqualTo(3);
        savedOrders.Should().Contain(o => o.OrderId == "INT-ORDER-004");
        savedOrders.Should().Contain(o => o.OrderId == "INT-ORDER-005");
        savedOrders.Should().Contain(o => o.OrderId == "INT-ORDER-006");
    }

    [Fact]
    public async Task ProcessAsync_ShouldHandleAllOrderStatuses()
    {
        // Arrange
        await _fixture.CleanupAsync();

        var statuses = new[] { "Created", "Processing", "Completed", "Failed" };

        // Act & Assert
        for (int i = 0; i < statuses.Length; i++)
        {
            var orderMessage = new OrderMessage
            {
                OrderId = $"INT-ORDER-STATUS-{i}",
                CustomerId = $"CUSTOMER-{i}",
                Amount = 100m,
                CreatedAt = DateTime.UtcNow,
                Status = statuses[i]
            };

            var consumeResult = CreateConsumeResult("orders-topic", 0, 5000 + i, orderMessage.OrderId, orderMessage);
            await _processor.ProcessAsync(consumeResult, CancellationToken.None);

            var savedOrder = await _orderRepository.GetByOrderId(orderMessage.OrderId, CancellationToken.None);
            savedOrder.Should().NotBeNull();
            savedOrder!.Status.Should().Be(Enum.Parse<OrderStatus>(statuses[i], true));
        }
    }

    [Fact]
    public async Task ProcessAsync_ShouldSaveDifferentPartitionsAndOffsets()
    {
        // Arrange
        await _fixture.CleanupAsync();

        var orderMessage1 = new OrderMessage
        {
            OrderId = "INT-ORDER-007",
            CustomerId = "CUSTOMER-007",
            Amount = 100m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var orderMessage2 = new OrderMessage
        {
            OrderId = "INT-ORDER-008",
            CustomerId = "CUSTOMER-008",
            Amount = 200m,
            CreatedAt = DateTime.UtcNow,
            Status = "Processing"
        };

        var consumeResult1 = CreateConsumeResult("orders-topic", 0, 6000, "INT-ORDER-007", orderMessage1);
        var consumeResult2 = CreateConsumeResult("orders-topic", 1, 7000, "INT-ORDER-008", orderMessage2);

        // Act
        await _processor.ProcessAsync(consumeResult1, CancellationToken.None);
        await _processor.ProcessAsync(consumeResult2, CancellationToken.None);

        // Assert
        var order1 = await _orderRepository.GetByOrderId("INT-ORDER-007", CancellationToken.None);
        order1.Should().NotBeNull();
        order1!.Partition.Should().Be(0);
        order1.Offset.Should().Be(6000);

        var order2 = await _orderRepository.GetByOrderId("INT-ORDER-008", CancellationToken.None);
        order2.Should().NotBeNull();
        order2!.Partition.Should().Be(1);
        order2.Offset.Should().Be(7000);
    }

    [Fact]
    public async Task ProcessAsync_ShouldHandleEmptyCustomerId()
    {
        // Arrange
        await _fixture.CleanupAsync();

        var invalidMessage = new OrderMessage
        {
            OrderId = "INT-ORDER-009",
            CustomerId = "", // Invalid
            Amount = 100m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 0, 8000, "INT-ORDER-009", invalidMessage);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Customer ID is required*");

        // Verify message was sent to DLQ
        var dlqMessages = await _failedOrderMessageRepository.GetByTopic("orders-topic", CancellationToken.None);
        dlqMessages.Should().Contain(m => m.Key == "INT-ORDER-009");
    }

    [Fact]
    public async Task ProcessAsync_ShouldHandleInvalidStatus()
    {
        // Arrange
        await _fixture.CleanupAsync();

        var invalidMessage = new OrderMessage
        {
            OrderId = "INT-ORDER-010",
            CustomerId = "CUSTOMER-010",
            Amount = 100m,
            CreatedAt = DateTime.UtcNow,
            Status = "InvalidStatus"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 0, 9000, "INT-ORDER-010", invalidMessage);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Invalid order status*");
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

