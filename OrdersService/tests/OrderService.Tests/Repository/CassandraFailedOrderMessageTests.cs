using FluentAssertions;
using OrderService.Data.Cassandra.Repositories;
using OrderService.Domain.Models;
using OrderService.Tests.Fixtures;

namespace OrderService.Tests.Repository;

public class CassandraFailedOrderMessageTests : IClassFixture<CassandraTestFixture>, IAsyncLifetime
{
    private readonly CassandraTestFixture _fixture;
    private readonly CassandraFailedOrderMessageRepository _repository;

    public CassandraFailedOrderMessageTests(CassandraTestFixture fixture)
    {
        _fixture = fixture;
        _repository = new CassandraFailedOrderMessageRepository(_fixture.Context);
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
        var uniqueTopic = $"test-topic-{Guid.NewGuid()}";
        var message = new FailedOrderMessage
        {
            Topic = uniqueTopic,
            Partition = 0,
            Offset = 100,
            Key = "ORDER-001",
            Value = "{\"orderId\":\"ORDER-001\"}",
            ErrorMessage = "Test error",
            StackTrace = "at Test.Method",
            RetryCount = 1,
            FailedAt = DateTime.UtcNow,
            ConsumerName = "test-consumer"
        };

        // Act
        await _repository.Add(message, CancellationToken.None);

        // Assert - verify by querying
        var results = await _repository.GetByTopic(uniqueTopic, CancellationToken.None);
        results.Should().NotBeEmpty();
        results.Should().Contain(m => m.Key == "ORDER-001" && m.Partition == 0);
    }

    [Fact]
    public async Task GetById_ShouldThrowNotSupportedException()
    {
        // Act
        var act = async () => await _repository.GetById(1, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*auto-increment*");
    }

    [Fact]
    public async Task GetByTopic_ShouldReturnMessagesForTopic()
    {
        // Arrange
        var uniqueTopic1 = $"topic-1-{Guid.NewGuid()}";
        var uniqueTopic2 = $"topic-2-{Guid.NewGuid()}";

        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = uniqueTopic1,
                Partition = 0,
                Offset = 1,
                Key = "KEY-1",
                Value = "{}",
                ErrorMessage = "Error 1",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow.AddMinutes(-5),
                ConsumerName = "consumer-1"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic1,
                Partition = 1,
                Offset = 2,
                Key = "KEY-2",
                Value = "{}",
                ErrorMessage = "Error 2",
                StackTrace = "",
                RetryCount = 2,
                FailedAt = DateTime.UtcNow.AddMinutes(-4),
                ConsumerName = "consumer-1"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic2,
                Partition = 0,
                Offset = 3,
                Key = "KEY-3",
                Value = "{}",
                ErrorMessage = "Error 3",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow.AddMinutes(-3),
                ConsumerName = "consumer-2"
            }
        };

        foreach (var msg in messages)
        {
            await _repository.Add(msg, CancellationToken.None);
        }

        // Act
        var result = await _repository.GetByTopic(uniqueTopic1, CancellationToken.None);

        // Assert
        result.Should().HaveCount(2);
        result.Should().OnlyContain(m => m.Topic == uniqueTopic1);
        result.Should().Contain(m => m.Key == "KEY-1");
        result.Should().Contain(m => m.Key == "KEY-2");
    }

    [Fact]
    public async Task GetByDateRange_ShouldReturnMessagesInRange()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var uniqueTopic = $"date-topic-{Guid.NewGuid()}";

        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 10,
                Key = "DATE-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = now.AddDays(-5),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 20,
                Key = "DATE-2",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = now.AddDays(-2),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 30,
                Key = "DATE-3",
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
        var result = await _repository.GetByDateRange(
            now.AddDays(-3),
            now.AddHours(1),
            CancellationToken.None);

        // Assert
        result.Should().HaveCountGreaterOrEqualTo(2);
        var topicMessages = result.Where(m => m.Topic == uniqueTopic).ToList();
        topicMessages.Should().Contain(m => m.Offset == 20);
        topicMessages.Should().Contain(m => m.Offset == 30);
    }

    [Fact]
    public async Task GetByRetryCount_ShouldReturnMessagesWithMinRetries()
    {
        // Arrange
        var uniqueTopic = $"retry-topic-{Guid.NewGuid()}";

        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 100,
                Key = "RETRY-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow.AddMinutes(-10),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 200,
                Key = "RETRY-2",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 3,
                FailedAt = DateTime.UtcNow.AddMinutes(-9),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 300,
                Key = "RETRY-3",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 5,
                FailedAt = DateTime.UtcNow.AddMinutes(-8),
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
        result.Should().HaveCountGreaterOrEqualTo(2);
        var topicMessages = result.Where(m => m.Topic == uniqueTopic).ToList();
        topicMessages.Should().OnlyContain(m => m.RetryCount >= 3);
        topicMessages.Should().Contain(m => m.Offset == 200);
        topicMessages.Should().Contain(m => m.Offset == 300);
    }

    [Fact]
    public async Task UpdateRetryCount_ShouldThrowNotSupportedException()
    {
        // Act
        var act = async () => await _repository.UpdateRetryCount(1, 5, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*Cassandra*");
    }

    [Fact]
    public async Task Add_ShouldHandleMultiplePartitions()
    {
        // Arrange
        var uniqueTopic = $"multi-partition-{Guid.NewGuid()}";

        var messages = new[]
        {
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 0,
                Offset = 1000,
                Key = "PART-0-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow.AddMinutes(-15),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 1,
                Offset = 2000,
                Key = "PART-1-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow.AddMinutes(-14),
                ConsumerName = "consumer"
            },
            new FailedOrderMessage
            {
                Topic = uniqueTopic,
                Partition = 2,
                Offset = 3000,
                Key = "PART-2-1",
                Value = "{}",
                ErrorMessage = "Error",
                StackTrace = "",
                RetryCount = 1,
                FailedAt = DateTime.UtcNow.AddMinutes(-13),
                ConsumerName = "consumer"
            }
        };

        // Act
        foreach (var msg in messages)
        {
            await _repository.Add(msg, CancellationToken.None);
        }

        // Assert
        var result = await _repository.GetByTopic(uniqueTopic, CancellationToken.None);
        result.Should().HaveCount(3);
        result.Select(m => m.Partition).Distinct().Should().HaveCount(3);
    }

    [Fact]
    public async Task GetByTopic_ShouldReturnEmptyListWhenTopicNotFound()
    {
        // Arrange
        var nonExistentTopic = $"non-existent-{Guid.NewGuid()}";

        // Act
        var result = await _repository.GetByTopic(nonExistentTopic, CancellationToken.None);

        // Assert
        result.Should().BeEmpty();
    }
}

