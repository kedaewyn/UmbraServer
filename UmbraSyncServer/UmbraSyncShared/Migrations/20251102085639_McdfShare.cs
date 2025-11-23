using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MareSynchronosServer.Migrations
{
    /// <inheritdoc />
    public partial class McdfShare : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "mcdf_shares",
                columns: table => new
                {
                    id = table.Column<Guid>(type: "uuid", nullable: false),
                    owner_uid = table.Column<string>(type: "character varying(10)", nullable: false),
                    description = table.Column<string>(type: "text", nullable: false),
                    cipher_data = table.Column<byte[]>(type: "bytea", nullable: false),
                    nonce = table.Column<byte[]>(type: "bytea", nullable: false),
                    salt = table.Column<byte[]>(type: "bytea", nullable: false),
                    tag = table.Column<byte[]>(type: "bytea", nullable: false),
                    created_utc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    updated_utc = table.Column<DateTime?>(type: "timestamp with time zone", nullable: true),
                    expires_at_utc = table.Column<DateTime?>(type: "timestamp with time zone", nullable: true),
                    download_count = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_mcdf_shares", x => x.id);
                    table.ForeignKey(
                        name: "fk_mcdf_shares_users_owner_uid",
                        column: x => x.owner_uid,
                        principalTable: "users",
                        principalColumn: "uid",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "mcdf_share_allowed_groups",
                columns: table => new
                {
                    share_id = table.Column<Guid>(type: "uuid", nullable: false),
                    allowed_group_gid = table.Column<string>(type: "character varying(20)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_mcdf_share_allowed_groups", x => new { x.share_id, x.allowed_group_gid });
                    table.ForeignKey(
                        name: "fk_mcdf_share_allowed_groups_mcdf_shares_share_id",
                        column: x => x.share_id,
                        principalTable: "mcdf_shares",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "mcdf_share_allowed_users",
                columns: table => new
                {
                    share_id = table.Column<Guid>(type: "uuid", nullable: false),
                    allowed_individual_uid = table.Column<string>(type: "character varying(10)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_mcdf_share_allowed_users", x => new { x.share_id, x.allowed_individual_uid });
                    table.ForeignKey(
                        name: "fk_mcdf_share_allowed_users_mcdf_shares_share_id",
                        column: x => x.share_id,
                        principalTable: "mcdf_shares",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "ix_mcdf_share_allowed_groups_allowed_group_gid",
                table: "mcdf_share_allowed_groups",
                column: "allowed_group_gid");

            migrationBuilder.CreateIndex(
                name: "ix_mcdf_share_allowed_users_allowed_individual_uid",
                table: "mcdf_share_allowed_users",
                column: "allowed_individual_uid");

            migrationBuilder.CreateIndex(
                name: "ix_mcdf_shares_owner_uid",
                table: "mcdf_shares",
                column: "owner_uid");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "mcdf_share_allowed_groups");

            migrationBuilder.DropTable(
                name: "mcdf_share_allowed_users");

            migrationBuilder.DropTable(
                name: "mcdf_shares");
        }
    }
}
