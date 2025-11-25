using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OrderService.Migrations
{
    /// <inheritdoc />
    public partial class InitialMigrations : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "failed_order_messages",
                columns: table => new
                {
                    id = table.Column<Guid>(type: "uuid", nullable: false),
                    topic = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: true),
                    partition = table.Column<int>(type: "integer", nullable: false),
                    offset = table.Column<long>(type: "bigint", nullable: false),
                    key = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: true),
                    value = table.Column<string>(type: "text", maxLength: 2147483647, nullable: true),
                    error_message = table.Column<string>(type: "text", maxLength: 2147483647, nullable: true),
                    stack_trace = table.Column<string>(type: "text", maxLength: 2147483647, nullable: true),
                    retry_count = table.Column<int>(type: "integer", nullable: false),
                    failed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    consumer_name = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_failed_order_messages", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "orders",
                columns: table => new
                {
                    order_id = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: false),
                    customer_id = table.Column<string>(type: "character varying(255)", maxLength: 255, nullable: true),
                    amount = table.Column<decimal>(type: "numeric(18,2)", precision: 18, scale: 2, nullable: false),
                    created_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    status = table.Column<int>(type: "integer", nullable: false),
                    processed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    partition = table.Column<int>(type: "integer", nullable: false),
                    offset = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_orders", x => x.order_id);
                });

            migrationBuilder.CreateIndex(
                name: "idx_failed_orders_messages_failed_at",
                table: "failed_order_messages",
                column: "failed_at");

            migrationBuilder.CreateIndex(
                name: "idx_failed_orders_messages_topic_partition_offset",
                table: "failed_order_messages",
                columns: new[] { "topic", "partition", "offset" });

            migrationBuilder.CreateIndex(
                name: "idx_orders_created_at",
                table: "orders",
                column: "created_at");

            migrationBuilder.CreateIndex(
                name: "idx_orders_customer_id",
                table: "orders",
                column: "customer_id");

            migrationBuilder.CreateIndex(
                name: "idx_orders_order_id",
                table: "orders",
                column: "order_id");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "failed_order_messages");

            migrationBuilder.DropTable(
                name: "orders");
        }
    }
}
