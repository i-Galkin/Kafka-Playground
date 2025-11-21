using FluentAssertions;
using OrderService.Data.Postgres.Repositories;
using OrderService.Domain.Enums;
using OrderService.Domain.Models;
using OrderService.Tests.Fixtures;

namespace OrderService.Tests.Repository;

public class PostgresOrderRepositoryTests : IClassFixture<PostgresTestFixture>, IAsyncLifetime
{
    private readonly PostgresTestFixture _fixture;
    private readonly PostgresOrderRepository _orderRepository;

    public PostgresOrderRepositoryTests(PostgresTestFixture fixture)
    {
        _fixture = fixture;
        _orderRepository = new PostgresOrderRepository(_fixture.Context);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _fixture.CleanupAsync();
    }

    [Fact]
    public async Task Upsert_ShouldInsertNewOrder()
    {
        // Arrange
        var order = new Order
        {
            OrderId = "ORDER-001",
            CustomerId = "CUSTOMER-001",
            Amount = 100.50m,
            CreatedAt = DateTime.UtcNow,
            Status = OrderStatus.Created,
            Partition = 0,
            Offset = 1
        };

        // Act
        await _orderRepository.Upsert(order, CancellationToken.None);

        // Assert
        var saved = await _orderRepository.GetByOrderId("ORDER-001", CancellationToken.None);
        saved.Should().NotBeNull();
        saved!.Amount.Should().Be(100.50m);
        saved.CustomerId.Should().Be("CUSTOMER-001");
    }

    [Fact]
    public async Task Upsert_ShouldUpdateExistingOrder()
    {
        // Arrange
        var order = new Order
        {
            OrderId = "ORDER-002",
            CustomerId = "CUSTOMER-002",
            Amount = 50.00m,
            CreatedAt = DateTime.UtcNow,
            Status = OrderStatus.Created,
            Partition = 0,
            Offset = 1
        };
        await _orderRepository.Upsert(order, CancellationToken.None);

        // Act - Update
        order.Amount = 75.00m;
        order.Status = OrderStatus.Completed;
        await _orderRepository.Upsert(order, CancellationToken.None);

        // Assert
        var updated = await _orderRepository.GetByOrderId("ORDER-002", CancellationToken.None);
        updated.Should().NotBeNull();
        updated!.Amount.Should().Be(75.00m);
        updated.Status.Should().Be(OrderStatus.Completed);
    }

    [Fact]
    public async Task GetByOrderId_ShouldReturnNull_WhenNotFound()
    {
        // Act
        var result = await _orderRepository.GetByOrderId("RANDOM-ORDER-ID", CancellationToken.None);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task Exists_ShouldReturnTrue_WhenOrderExists()
    {
        // Arrange
        var order = new Order
        {
            OrderId = "ORDER-003",
            CustomerId = "CUSTOMER-003",
            Amount = 25.00m,
            CreatedAt = DateTime.UtcNow,
            Status = OrderStatus.Created,
            Partition = 0,
            Offset = 1
        };
        await _orderRepository.Upsert(order, CancellationToken.None);

        // Act
        var exists = await _orderRepository.Exists("ORDER-003", CancellationToken.None);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task GetListByDateRange_ShouldReturnOrdersInRange()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var orders = new[]
        {
            new Order { OrderId = "ORDER-004", CustomerId = "CUSTOMER-004", Amount = 10, CreatedAt = now.AddDays(-2), Status = OrderStatus.Processing, Partition = 0, Offset = 1 },
            new Order { OrderId = "ORDER-005", CustomerId = "CUSTOMER-005", Amount = 20, CreatedAt = now.AddDays(-1), Status = OrderStatus.Failed, Partition = 0, Offset = 2 },
            new Order { OrderId = "ORDER-006", CustomerId = "CUSTOMER-006", Amount = 30, CreatedAt = now, Status = OrderStatus.Completed, Partition = 0, Offset = 3 }
        };

        foreach (var order in orders)
        {
            await _orderRepository.Upsert(order, CancellationToken.None);
        }

        // Act
        var result = await _orderRepository.GetListByDateRange(now.AddDays(-1.5), now.AddDays(0.5), CancellationToken.None);

        // Assert
        result.Should().HaveCount(2);
        result.Should().Contain(o => o.OrderId == "ORDER-005");
        result.Should().Contain(o => o.OrderId == "ORDER-006");
    }

    [Fact]
    public async Task GetList_ShouldReturnOrdersInRange()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var orders = new[]
        {
            new Order { OrderId = "ORDER-007", CustomerId = "CUSTOMER-007", Amount = 10, CreatedAt = now.AddDays(-2), Status = OrderStatus.Processing, Partition = 0, Offset = 1 },
            new Order { OrderId = "ORDER-008", CustomerId = "CUSTOMER-008", Amount = 20, CreatedAt = now.AddDays(-1), Status = OrderStatus.Failed, Partition = 0, Offset = 2 },
            new Order { OrderId = "ORDER-009", CustomerId = "CUSTOMER-009", Amount = 30, CreatedAt = now, Status = OrderStatus.Completed, Partition = 0, Offset = 3 }
        };

        foreach (var order in orders)
        {
            await _orderRepository.Upsert(order, CancellationToken.None);
        }

        // Act
        var result = await _orderRepository.GetList(CancellationToken.None);

        // Assert
        result.Should().HaveCount(3);
    }
}