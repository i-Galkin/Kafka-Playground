using FluentAssertions;
using OrderService.Data.Postgres.Repositories;
using OrderService.Domain.Models;
using OrderService.Tests.Fixtures;

namespace OrderService.Tests.Repository;

public class PostgresFailedOrderMessageTests : IClassFixture<PostgresTestFixture>, IAsyncLifetime
{
    private readonly PostgresTestFixture _fixture;
    private readonly PostgresFailedOrderMessageRepository _repository;

    public PostgresFailedOrderMessageTests(PostgresTestFixture fixture)
    {
        _fixture = fixture;
        _repository = new PostgresFailedOrderMessageRepository(_fixture.Context);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _fixture.CleanupAsync();
    }

    [Fact]
    public async Task Add_ShouldInsertFailedMessage()
    {
        // Arrange
        var message = new FailedOrderMessage
        {
            Topic = "orders-topic",
            Partition = 0,
            Offset = 100,
            Key = "ORDER-001",
            Value = "{\"orderId\":\"ORDER-001\"}",
            ErrorMessage = "Processing failed",
            StackTrace = "at OrderProcessor.ProcessAsync",
            RetryCount = 3,
            FailedAt = DateTime.UtcNow,
            ConsumerName = "order-consumer"
        };

        // Act
        await _repository.Add(message, CancellationToken.None);

        // Assert
        var saved = await _repository.GetById(message.Id, CancellationToken.None);
        saved.Should().NotBeNull();
        saved!.Topic.Should().Be("orders-topic");
        saved.ErrorMessage.Should().Be("Processing failed");
        saved.RetryCount.Should().Be(3);
    }

    [Fact]
    public async Task GetByTopic_ShouldReturnMessagesForTopic()
    {
        // Arrange
        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = "orders-topic-1",
                Partition = 0,
                Offset = 1,
                Key = "KEY-1",
                Value = "{}",
                ErrorMessage = "Error 1",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow,
                ConsumerName = "consumer-1"
            },
            new FailedOrderMessage
            {
                Topic = "orders-topic-1",
                Partition = 0,
                Offset = 2,
                Key = "KEY-2",
                Value = "{}",
                ErrorMessage = "Error 2",
                StackTrace = "",
                RetryCount = 2,
                FailedAt = DateTime.UtcNow,
                ConsumerName = "consumer-1"
            },
            new FailedOrderMessage
            {
                Topic = "orders-topic-2",
                Partition = 0,
                Offset = 3,
                Key = "KEY-3",
                Value = "{}",
                ErrorMessage = "Error 3",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow,
                ConsumerName = "consumer-2"
            }
        };

        foreach (var msg in messages)
        {
            await _repository.Add(msg, CancellationToken.None);
        }

        // Act
        var result = await _repository.GetByTopic("orders-topic-1", CancellationToken.None);

        // Assert
        result.Should().HaveCount(2);
        result.Should().OnlyContain(m => m.Topic == "orders-topic-1");
    }

    [Fact]
    public async Task GetByDateRange_ShouldReturnMessagesInRange()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = "KEY-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = now.AddDays(-2),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 2,
                Key = "KEY-2",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = now.AddDays(-1),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 3,
                Key = "KEY-3",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = now,
                ConsumerName = "consumer"
            }
        };

        foreach (var msg in messages)
        {
            await _repository.Add(msg, CancellationToken.None);
        }

        // Act
        var result = await _repository.GetByDateRange(now.AddDays(-1.5), now.AddHours(1), CancellationToken.None);

        // Assert
        result.Should().HaveCount(2);
        result.Should().Contain(m => m.Offset == 2);
        result.Should().Contain(m => m.Offset == 3);
    }

    [Fact]
    public async Task GetByRetryCount_ShouldReturnMessagesWithMinRetries()
    {
        // Arrange
        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 1,
                Key = "KEY-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow,
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 2,
                Key = "KEY-2",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 3,
                FailedAt = DateTime.UtcNow,
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 3,
                Key = "KEY-3",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 5,
                FailedAt = DateTime.UtcNow,
                ConsumerName = "consumer"
            }
        };

        foreach (var msg in messages)
        {
            await _repository.Add(msg, CancellationToken.None);
        }

        // Act
        var result = await _repository.GetByRetryCount(3, CancellationToken.None);

        // Assert
        result.Should().HaveCount(2);
        result.Should().OnlyContain(m => m.RetryCount >= 3);
    }

    [Fact]
    public async Task UpdateRetryCount_ShouldUpdateExistingMessage()
    {
        // Arrange
        var message = new FailedOrderMessage
        {
            Topic = "test-topic",
            Partition = 0,
            Offset = 1,
            Key = "KEY-1",
            Value = "{}",
            ErrorMessage = "Error",
            StackTrace = "",
            RetryCount = 1,
            FailedAt = DateTime.UtcNow,
            ConsumerName = "consumer"
        };

        await _repository.Add(message, CancellationToken.None);

        // Act
        await _repository.UpdateRetryCount(message.Id, 5, CancellationToken.None);

        // Assert
        var updated = await _repository.GetById(message.Id, CancellationToken.None);
        updated.Should().NotBeNull();
        updated!.RetryCount.Should().Be(5);
    }

    [Fact]
    public async Task GetById_ShouldReturnNull_WhenNotFound()
    {
        // Act
        var result = await _repository.GetById(99999, CancellationToken.None);

        // Assert
        result.Should().BeNull();
    }
}