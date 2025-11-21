using OrderService.Domain.Messages;

namespace OrderService.Applications.Validators.Interfaces
{
    public interface IOrderValidator
    {
        void Validate(OrderMessage order);
    }
}
