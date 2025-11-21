using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace OrderService.Domain.Models
{
    /// <summary>
    /// Failed Order Message Model
    /// </summary>
    [Table("failed_order_messages")]
    public class FailedOrderMessage
    {
        /// <summary>
        /// Inner Order Identifier
        /// </summary>
        [Key]
        [Column("id")]
        public int Id { get; set; }

        /// <summary>
        /// Kafka Topic
        /// </summary>
        [Column("topic")]
        [MaxLength(255)]
        public string Topic { get; set; }

        /// <summary>
        /// Kafka Partition
        /// </summary>
        [Column("partition")]
        public int Partition { get; set; }

        /// <summary>
        /// Kafka Offset
        /// </summary>
        [Column("offset")]
        public long Offset { get; set; }

        /// <summary>
        /// Message Key
        /// </summary>
        [Column("key")]
        [MaxLength(255)]
        public string Key { get; set; }

        /// <summary>
        /// Message Value
        /// </summary>
        [Column("value")]
        [MaxLength(int.MaxValue)]
        public string Value { get; set; }

        /// <summary>
        /// Error Message
        /// </summary>
        [Column("error_message")]
        [MaxLength(int.MaxValue)]
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Stack Trace
        /// </summary>
        [Column("stack_trace")]
        [MaxLength(int.MaxValue)]
        public string StackTrace { get; set; }

        /// <summary>
        /// Retry Count
        /// </summary>
        [Column("retry_count")]
        public int RetryCount { get; set; }

        /// <summary>
        /// Failed At Timestamp
        /// </summary>
        [Column("failed_at")]
        public DateTime FailedAt { get; set; }

        /// <summary>
        /// Consumer Name
        /// </summary>
        [Column("consumer_name")]
        [MaxLength(255)]
        public string ConsumerName { get; set; }
    }
}
