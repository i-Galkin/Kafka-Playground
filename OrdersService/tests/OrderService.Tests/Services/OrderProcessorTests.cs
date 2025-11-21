using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using OrderService.Applications.Services;
using OrderService.Applications.Services.Interfaces;
using OrderService.Applications.Validators.Interfaces;
using OrderService.Data.Interfaces;
using OrderService.Domain.Enums;
using OrderService.Domain.Messages;
using OrderService.Domain.Models;
using OrderService.Infrastructure.Kafka.Interfaces;

namespace OrderService.Tests.Services;

public class OrderProcessorTests
{
    private readonly Mock<IRetryManager> _retryManagerMock;
    private readonly Mock<IDeadLetterQueue> _deadLetterQueueMock;
    private readonly Mock<ILogger<OrderProcessor>> _loggerMock;
    private readonly Mock<IOrderValidator> _orderValidatorMock;
    private readonly Mock<IOrderRepository> _orderRepositoryMock;
    private readonly OrderProcessor _processor;

    public OrderProcessorTests()
    {
        _retryManagerMock = new Mock<IRetryManager>();
        _deadLetterQueueMock = new Mock<IDeadLetterQueue>();
        _loggerMock = new Mock<ILogger<OrderProcessor>>();
        _orderValidatorMock = new Mock<IOrderValidator>();
        _orderRepositoryMock = new Mock<IOrderRepository>();

        _processor = new OrderProcessor(
            _retryManagerMock.Object,
            _deadLetterQueueMock.Object,
            _loggerMock.Object,
            _orderValidatorMock.Object,
            _orderRepositoryMock.Object
        );
    }

    [Fact]
    public async Task ProcessAsync_ShouldProcessOrderSuccessfully()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-001",
            CustomerId = "CUSTOMER-001",
            Amount = 100.50m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 0, 100, "ORDER-001", orderMessage);

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ReturnsAsync(true);

        Order capturedOrder = null;
        _orderRepositoryMock
            .Setup(r => r.Upsert(It.IsAny<Order>(), It.IsAny<CancellationToken>()))
            .Callback<Order, CancellationToken>((order, ct) => capturedOrder = order)
            .Returns(Task.CompletedTask);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        _orderValidatorMock.Verify(v => v.Validate(orderMessage), Times.Once);
        _orderRepositoryMock.Verify(r => r.Upsert(It.IsAny<Order>(), It.IsAny<CancellationToken>()), Times.Once);
        _deadLetterQueueMock.Verify(
            d => d.SendAsync(It.IsAny<ConsumeResult<string, OrderMessage>>(), It.IsAny<Exception>(), It.IsAny<int>(), It.IsAny<CancellationToken>()),
            Times.Never);

        capturedOrder.Should().NotBeNull();
        capturedOrder!.OrderId.Should().Be("ORDER-001");
        capturedOrder.CustomerId.Should().Be("CUSTOMER-001");
        capturedOrder.Amount.Should().Be(100.50m);
        capturedOrder.Status.Should().Be(OrderStatus.Created);
        capturedOrder.Partition.Should().Be(0);
        capturedOrder.Offset.Should().Be(100);
    }

    [Fact]
    public async Task ProcessAsync_ShouldMapStatusCorrectly()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-002",
            CustomerId = "CUSTOMER-002",
            Amount = 50.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "processing"  // lowercase
        };

        var consumeResult = CreateConsumeResult("orders-topic", 1, 200, "ORDER-002", orderMessage);

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ReturnsAsync(true);

        Order capturedOrder = null;
        _orderRepositoryMock
            .Setup(r => r.Upsert(It.IsAny<Order>(), It.IsAny<CancellationToken>()))
            .Callback<Order, CancellationToken>((order, ct) => capturedOrder = order)
            .Returns(Task.CompletedTask);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        capturedOrder.Should().NotBeNull();
        capturedOrder!.Status.Should().Be(OrderStatus.Processing);
    }

    [Fact]
    public async Task ProcessAsync_ShouldSetProcessedAtTimestamp()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-003",
            CustomerId = "CUSTOMER-003",
            Amount = 75.00m,
            CreatedAt = DateTime.UtcNow.AddHours(-1),
            Status = "Completed"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 2, 300, "ORDER-003", orderMessage);

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ReturnsAsync(true);

        Order capturedOrder = null;
        _orderRepositoryMock
            .Setup(r => r.Upsert(It.IsAny<Order>(), It.IsAny<CancellationToken>()))
            .Callback<Order, CancellationToken>((order, ct) => capturedOrder = order)
            .Returns(Task.CompletedTask);

        var beforeProcessing = DateTime.UtcNow;

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        var afterProcessing = DateTime.UtcNow;

        // Assert
        capturedOrder.Should().NotBeNull();
        capturedOrder!.ProcessedAt.Should().BeOnOrAfter(beforeProcessing);
        capturedOrder.ProcessedAt.Should().BeOnOrBefore(afterProcessing);
    }

    [Fact]
    public async Task ProcessAsync_ShouldValidateOrder()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-004",
            CustomerId = "CUSTOMER-004",
            Amount = 25.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 3, 400, "ORDER-004", orderMessage);

        _orderValidatorMock
            .Setup(v => v.Validate(It.IsAny<OrderMessage>()))
            .Throws(new InvalidOperationException("Validation failed"));

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ThrowsAsync(new InvalidOperationException("Validation failed"));

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>();

        _deadLetterQueueMock.Verify(
            d => d.SendAsync(consumeResult, It.IsAny<Exception>(), 3, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessAsync_ShouldSendToDLQ_WhenRetryManagerThrows()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-005",
            CustomerId = "CUSTOMER-005",
            Amount = 10.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Processing"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 4, 500, "ORDER-005", orderMessage);
        var exception = new Exception("Processing failed after retries");

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .ThrowsAsync(exception);

        // Act & Assert
        var act = async () => await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("Processing failed after retries");

        _deadLetterQueueMock.Verify(
            d => d.SendAsync(consumeResult, exception, 3, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessAsync_ShouldLogInformation_WhenProcessingStarts()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-006",
            CustomerId = "CUSTOMER-006",
            Amount = 150.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 5, 600, "ORDER-006", orderMessage);

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ReturnsAsync(true);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Processing order")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task ProcessAsync_ShouldLogInformation_WhenOrderSavedSuccessfully()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-007",
            CustomerId = "CUSTOMER-007",
            Amount = 200.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Completed"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 6, 700, "ORDER-007", orderMessage);

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ReturnsAsync(true);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("saved successfully")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task ProcessAsync_ShouldLogError_WhenProcessingFailsAfterRetries()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-008",
            CustomerId = "CUSTOMER-008",
            Amount = 300.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Failed"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 7, 800, "ORDER-008", orderMessage);
        var exception = new Exception("Database connection failed");

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .ThrowsAsync(exception);

        // Act
        try
        {
            await _processor.ProcessAsync(consumeResult, CancellationToken.None);
        }
        catch
        {
            // Expected
        }

        // Assert
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Failed to process order")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task ProcessAsync_ShouldUseRetryManager()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-009",
            CustomerId = "CUSTOMER-009",
            Amount = 50.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Processing"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 8, 900, "ORDER-009", orderMessage);

        _retryManagerMock
            .Setup(r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()))
            .Callback<Func<Task<bool>>, Func<Exception, bool>>(async (action, _) => await action())
            .ReturnsAsync(true);

        // Act
        await _processor.ProcessAsync(consumeResult, CancellationToken.None);

        // Assert
        _retryManagerMock.Verify(
            r => r.ExecuteAsync(It.IsAny<Func<Task<bool>>>(), It.IsAny<Func<Exception, bool>>()),
            Times.Once);
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

