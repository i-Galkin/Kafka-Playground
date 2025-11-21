using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using OrderService.Applications.Services;
using OrderService.Data.Interfaces;
using OrderService.Domain.Messages;
using OrderService.Domain.Models;

namespace OrderService.Tests.Services;

public class DeadLetterQueueServiceTests
{
    private readonly Mock<ILogger<DeadLetterQueueService>> _loggerMock;
    private readonly Mock<IHostEnvironment> _environmentMock;
    private readonly Mock<IFailedOrderMessageRepository> _repositoryMock;
    private readonly DeadLetterQueueService _service;

    public DeadLetterQueueServiceTests()
    {
        _loggerMock = new Mock<ILogger<DeadLetterQueueService>>();
        _environmentMock = new Mock<IHostEnvironment>();
        _repositoryMock = new Mock<IFailedOrderMessageRepository>();

        _environmentMock.Setup(e => e.ApplicationName).Returns("TestConsumer");

        _service = new DeadLetterQueueService(
            _loggerMock.Object,
            _environmentMock.Object,
            _repositoryMock.Object
        );
    }

    [Fact]
    public async Task SendAsync_ShouldSaveFailedMessageToRepository()
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
        var exception = new Exception("Processing failed");

        FailedOrderMessage capturedMessage = null;
        _repositoryMock
            .Setup(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()))
            .Callback<FailedOrderMessage, CancellationToken>((msg, ct) => capturedMessage = msg)
            .Returns(Task.CompletedTask);

        // Act
        await _service.SendAsync(consumeResult, exception, 3, CancellationToken.None);

        // Assert
        _repositoryMock.Verify(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()), Times.Once);

        capturedMessage.Should().NotBeNull();
        capturedMessage!.Topic.Should().Be("orders-topic");
        capturedMessage.Partition.Should().Be(0);
        capturedMessage.Offset.Should().Be(100);
        capturedMessage.Key.Should().Be("ORDER-001");
        capturedMessage.ErrorMessage.Should().Be("Processing failed");
        capturedMessage.RetryCount.Should().Be(3);
        capturedMessage.ConsumerName.Should().Be("TestConsumer");
    }

    [Fact]
    public async Task SendAsync_ShouldIncludeStackTrace()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-002",
            CustomerId = "CUSTOMER-002",
            Amount = 50.25m,
            CreatedAt = DateTime.UtcNow,
            Status = "Processing"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 1, 200, "ORDER-002", orderMessage);

        // Create an exception with a stack trace by actually throwing it
        Exception exception;
        try
        {
            throw new InvalidOperationException("Validation failed");
        }
        catch (InvalidOperationException ex)
        {
            exception = ex;
        }

        FailedOrderMessage capturedMessage = null;
        _repositoryMock
            .Setup(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()))
            .Callback<FailedOrderMessage, CancellationToken>((msg, ct) => capturedMessage = msg)
            .Returns(Task.CompletedTask);

        // Act
        await _service.SendAsync(consumeResult, exception, 1, CancellationToken.None);

        // Assert
        capturedMessage.Should().NotBeNull();
        capturedMessage!.StackTrace.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task SendAsync_ShouldSerializeMessageValue()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-003",
            CustomerId = "CUSTOMER-003",
            Amount = 75.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Completed"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 2, 300, "ORDER-003", orderMessage);
        var exception = new Exception("Test exception");

        FailedOrderMessage capturedMessage = null;
        _repositoryMock
            .Setup(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()))
            .Callback<FailedOrderMessage, CancellationToken>((msg, ct) => capturedMessage = msg)
            .Returns(Task.CompletedTask);

        // Act
        await _service.SendAsync(consumeResult, exception, 2, CancellationToken.None);

        // Assert
        capturedMessage.Should().NotBeNull();
        capturedMessage!.Value.Should().Contain("ORDER-003");
        capturedMessage.Value.Should().Contain("CUSTOMER-003");
        capturedMessage.Value.Should().Contain("75");
    }

    [Fact]
    public async Task SendAsync_ShouldThrow_WhenRepositoryFails()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-004",
            CustomerId = "CUSTOMER-004",
            Amount = 25.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Failed"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 3, 400, "ORDER-004", orderMessage);
        var exception = new Exception("Processing failed");

        _repositoryMock
            .Setup(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Database connection failed"));

        // Act & Assert
        var act = async () => await _service.SendAsync(consumeResult, exception, 1, CancellationToken.None);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("Database connection failed");
    }

    [Fact]
    public async Task SendAsync_ShouldHandleNullKey()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-005",
            CustomerId = "CUSTOMER-005",
            Amount = 10.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        var consumeResult = CreateConsumeResult<string, OrderMessage>("orders-topic", 4, 500, null, orderMessage);
        var exception = new Exception("Test exception");

        FailedOrderMessage capturedMessage = null;
        _repositoryMock
            .Setup(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()))
            .Callback<FailedOrderMessage, CancellationToken>((msg, ct) => capturedMessage = msg)
            .Returns(Task.CompletedTask);

        // Act
        await _service.SendAsync(consumeResult, exception, 1, CancellationToken.None);

        // Assert
        capturedMessage.Should().NotBeNull();
        capturedMessage!.Key.Should().BeNull();
    }

    [Fact]
    public async Task SendAsync_ShouldLogWarning_WhenMessageSentToDLQ()
    {
        // Arrange
        var orderMessage = new OrderMessage
        {
            OrderId = "ORDER-006",
            CustomerId = "CUSTOMER-006",
            Amount = 150.00m,
            CreatedAt = DateTime.UtcNow,
            Status = "Processing"
        };

        var consumeResult = CreateConsumeResult("orders-topic", 5, 600, "ORDER-006", orderMessage);
        var exception = new Exception("Test exception");

        _repositoryMock
            .Setup(r => r.Add(It.IsAny<FailedOrderMessage>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        await _service.SendAsync(consumeResult, exception, 3, CancellationToken.None);

        // Assert
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Message sent to DLQ")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.Once);
    }

    private ConsumeResult<TKey, TValue> CreateConsumeResult<TKey, TValue>(
        string topic, int partition, long offset, TKey key, TValue value)
    {
        return new ConsumeResult<TKey, TValue>
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            }
        };
    }
}

