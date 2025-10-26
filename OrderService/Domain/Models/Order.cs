using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using OrderService.Domain.Enums;

namespace OrderService.Domain.Models
{
    // TODO: Use shared library

    /// <summary>
    /// Order Model
    /// </summary>
    [Table("orders")]
    public class Order
    {
        /// <summary>
        /// Order Identifier
        /// </summary>
        [Key]
        [Column("order_id")]
        public int OrderId { get; set; }

        /// <summary>
        /// Customer Identifier
        /// </summary>
        [Column("customer_id")]
        public string CustomerId { get; set; }

        /// <summary>
        /// Order Amount
        /// </summary>
        [Column("amount")]
        public decimal Amount { get; set; }

        /// <summary>
        /// Order Creation Timestamp
        /// </summary>
        [Column("created_at")]
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Order Status
        /// </summary>
        [Column("status")]
        public OrderStatus Status { get; set; }

        /// <summary>
        /// Timestamp when the order was processed
        /// </summary>
        [Column("processed_at")]
        public DateTime ProcessedAt { get; set; }

        /// <summary>
        /// Kafka Partition from which the order was consumed
        /// </summary>
        [Column("partition")]
        public int Partition { get; set; }

        /// <summary>
        /// Kafka Offset from which the order was consumed
        /// </summary>
        [Column("offset")]
        public long Offset { get; set; }
    }
}
