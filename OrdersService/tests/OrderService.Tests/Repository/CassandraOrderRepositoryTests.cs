using FluentAssertions;
using OrderService.Data.Cassandra.Repositories;
using OrderService.Domain.Enums;
using OrderService.Domain.Models;
using OrderService.Tests.Fixtures;

namespace OrderService.Tests.Repository;

public class CassandraOrderRepositoryTests : IClassFixture<CassandraTestFixture>, IAsyncLifetime
{
    private readonly CassandraTestFixture _fixture;
    private readonly CassandraOrderRepository _repository;

    public CassandraOrderRepositoryTests(CassandraTestFixture fixture)
    {
        _fixture = fixture;
        _repository = new CassandraOrderRepository(_fixture.Context);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _fixture.CleanupAsync();
    }

    [Fact]
    public async Task Upsert_ShouldInsertOrder()
    {
        // Arrange
        var order = new Order
        {
            OrderId = "CASSANDRA-ORDER-001",
            CustomerId = "CASSANDRA-ORDER-001",
            Amount = 150.75m,
            CreatedAt = DateTime.UtcNow,
            Status = OrderStatus.Created,
            Partition = 0,
            Offset = 1
        };

        // Act
        await _repository.Upsert(order, CancellationToken.None);

        // Assert
        var saved = await _repository.GetByOrderId("CASSANDRA-ORDER-001", CancellationToken.None);
        saved.Should().NotBeNull();
        saved!.Amount.Should().Be(150.75m);
    }

    [Fact]
    public async Task GetByDateRange_ShouldReturnOrders()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var order = new Order
        {
            OrderId = "CASSANDRA-ORDER-002",
            CustomerId = "CASSANDRA-CUSTOMER-002",
            Amount = 99.99m,
            CreatedAt = now,
            Status = OrderStatus.Completed,
            Partition = 0,
            Offset = 2
        };
        await _repository.Upsert(order, CancellationToken.None);

        // Act
        var result = await _repository.GetListByDateRange(now.AddHours(-1), now.AddHours(1), CancellationToken.None);

        // Assert
        result.Should().NotBeEmpty();
        result.Should().Contain(o => o.OrderId == "CASSANDRA-ORDER-002");
    }

    [Fact]
    public async Task GetList_ShouldReturnOrders()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var order = new Order
        {
            OrderId = "CASSANDRA-ORDER-003",
            CustomerId = "CASSANDRA-CUSTOMER-003",
            Amount = 99.99m,
            CreatedAt = now,
            Status = OrderStatus.Completed,
            Partition = 0,
            Offset = 2
        };
        await _repository.Upsert(order, CancellationToken.None);

        // Act
        var result = await _repository.GetList(CancellationToken.None);

        // Assert
        result.Should().NotBeEmpty();
        result.Should().Contain(o => o.OrderId == "CASSANDRA-ORDER-003");
    }
}