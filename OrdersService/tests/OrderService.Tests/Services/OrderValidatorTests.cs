using FluentAssertions;
using OrderService.Applications.Validators;
using OrderService.Domain.Messages;

namespace OrderService.Tests.Services;

public class OrderValidatorTests
{
    private readonly OrderValidator _validator;

    public OrderValidatorTests()
    {
        _validator = new OrderValidator();
    }

    [Fact]
    public void Validate_ShouldPass_WhenOrderIsValid()
    {
        // Arrange
        var order = new OrderMessage
        {
            OrderId = "ORDER-001",
            CustomerId = "CUSTOMER-001",
            Amount = 100.50m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        // Act & Assert
        var act = () => _validator.Validate(order);
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100.50)]
    public void Validate_ShouldThrow_WhenAmountIsNonPositive(decimal amount)
    {
        // Arrange
        var order = new OrderMessage
        {
            OrderId = "ORDER-001",
            CustomerId = "CUSTOMER-001",
            Amount = amount,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        // Act & Assert
        var act = () => _validator.Validate(order);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*amount must be positive*");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Validate_ShouldThrow_WhenCustomerIdIsEmpty(string customerId)
    {
        // Arrange
        var order = new OrderMessage
        {
            OrderId = "ORDER-001",
            CustomerId = customerId,
            Amount = 100.50m,
            CreatedAt = DateTime.UtcNow,
            Status = "Created"
        };

        // Act & Assert
        var act = () => _validator.Validate(order);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Customer ID is required*");
    }

    [Theory]
    [InlineData("InvalidStatus")]
    [InlineData("Unknown")]
    [InlineData("SomeRandomText")]
    public void Validate_ShouldThrow_WhenStatusIsInvalid(string status)
    {
        // Arrange
        var order = new OrderMessage
        {
            OrderId = "ORDER-001",
            CustomerId = "CUSTOMER-001",
            Amount = 100.50m,
            CreatedAt = DateTime.UtcNow,
            Status = status
        };

        // Act & Assert
        var act = () => _validator.Validate(order);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Invalid order status*");
    }

    [Theory]
    [InlineData("Created")]
    [InlineData("created")]
    [InlineData("CREATED")]
    [InlineData("Processing")]
    [InlineData("Completed")]
    [InlineData("Failed")]
    public void Validate_ShouldPass_WhenStatusIsValid(string status)
    {
        // Arrange
        var order = new OrderMessage
        {
            OrderId = "ORDER-001",
            CustomerId = "CUSTOMER-001",
            Amount = 100.50m,
            CreatedAt = DateTime.UtcNow,
            Status = status
        };

        // Act & Assert
        var act = () => _validator.Validate(order);
        act.Should().NotThrow();
    }
}

