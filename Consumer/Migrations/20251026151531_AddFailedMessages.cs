using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Consumer.Migrations
{
    /// <inheritdoc />
    public partial class AddFailedMessages : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "failed_order_messages",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    topic = table.Column<string>(type: "text", nullable: true),
                    partition = table.Column<int>(type: "integer", nullable: false),
                    offset = table.Column<long>(type: "bigint", nullable: false),
                    key = table.Column<string>(type: "text", nullable: true),
                    value = table.Column<string>(type: "text", nullable: true),
                    error_message = table.Column<string>(type: "text", nullable: true),
                    stack_trace = table.Column<string>(type: "text", nullable: true),
                    retry_count = table.Column<int>(type: "integer", nullable: false),
                    failed_at = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    consumer_name = table.Column<string>(type: "text", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_failed_order_messages", x => x.id);
                });

            migrationBuilder.CreateIndex(
                name: "idx_failed_orders_messages_failed_at",
                table: "failed_order_messages",
                column: "failed_at");

            migrationBuilder.CreateIndex(
                name: "idx_failed_orders_messages_topic_partition_offset",
                table: "failed_order_messages",
                columns: new[] { "topic", "partition", "offset" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "failed_order_messages");
        }
    }
}
