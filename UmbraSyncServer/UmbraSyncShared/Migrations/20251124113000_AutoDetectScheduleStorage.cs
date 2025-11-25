using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MareSynchronosServer.Migrations
{
    /// <inheritdoc />
    public partial class AutoDetectScheduleStorage : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "autodetect_schedules",
                columns: table => new
                {
                    group_gid = table.Column<string>(type: "character varying(20)", nullable: false),
                    recurring = table.Column<bool>(type: "boolean", nullable: false),
                    display_duration_hours = table.Column<int>(type: "integer", nullable: true),
                    active_weekdays = table.Column<int[]>(type: "integer[]", nullable: true),
                    time_start_local = table.Column<string>(type: "text", nullable: true),
                    time_end_local = table.Column<string>(type: "text", nullable: true),
                    time_zone = table.Column<string>(type: "text", nullable: true),
                    last_activated_utc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_autodetect_schedules", x => x.group_gid);
                    table.ForeignKey(
                        name: "fk_autodetect_schedules_groups_group_gid",
                        column: x => x.group_gid,
                        principalTable: "groups",
                        principalColumn: "gid",
                        onDelete: ReferentialAction.Cascade);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "autodetect_schedules");
        }
    }
}
