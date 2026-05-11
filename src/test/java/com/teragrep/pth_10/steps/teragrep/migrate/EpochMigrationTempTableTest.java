/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_10.steps.teragrep.migrate;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep1;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.SelectWhereStep;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationTempTableTest {

    @Container
    private final MariaDBContainer<?> mariadb = new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5"))
            .withPrivilegedMode(false)
            .withDatabaseName("journaldb")
            .withUsername("username")
            .withPassword("test");

    @BeforeEach
    public void setupDatabase() {
        Assertions.assertDoesNotThrow(() -> {
            try (
                    final Connection conn = DriverManager
                            .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
            ) {
                final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
                ctx.dropTableIfExists("logfile").execute();
                ctx.dropTableIfExists("object_format").execute();
                ctx
                        .createTableIfNotExists("object_format")
                        .column("id", SQLDataType.BIGINT.nullable(false).identity(true))
                        .column("name", SQLDataType.VARCHAR(255).nullable(true))
                        .constraint(DSL.constraint("pk_object_format").primaryKey("id"))
                        .execute();
                ctx
                        .createTableIfNotExists("logfile")
                        .column("id", SQLDataType.BIGINT.nullable(false).identity(true))
                        .column("epoch_hour", SQLDataType.BIGINT.nullable(true))
                        .column("object_format_id", SQLDataType.BIGINT.nullable(true))
                        .constraints(DSL.constraint("pk_logfile").primaryKey("id"), DSL.constraint("fk_object_format").foreignKey("object_format_id").references("object_format", "id")).execute();

                final Field<Long> idField = DSL.field("id", Long.class);
                InsertValuesStep1<Record, Long> insertQuery = ctx.insertInto(DSL.table("logfile"), idField);
                for (long i = 1; i <= 10; i++) {
                    insertQuery = insertQuery.values(i);
                }
                insertQuery.execute();
                final Path path = Paths.get("src/test/resources/sql/epoch_migration_procedure.sql");
                final String createProcedureSQL = Files.readString(path);
                ctx.execute(createProcedureSQL);
            }
        });
    }

    @Test
    public void testInsert() {
        final Connection conn = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
        final DSLContext ctx = DSL.using(conn, SQLDialect.MARIADB);
        final EpochMigrationTempTable epochMigrationTempTable = new EpochMigrationTempTable(ctx, "journaldb");
        final BatchBindStep batchBindStep = epochMigrationTempTable.insertBatch();
        epochMigrationTempTable.create();
        final BatchBindStep testBatch = batchBindStep.bind(1L, 1L, "test_format");
        Assertions.assertDoesNotThrow(testBatch::execute);
        final SelectWhereStep<Record> records = ctx
                .selectFrom(DSL.table(DSL.name("journaldb", "epoch_migration_temp_table")));
        for (final Record record : records) {
            final Object id = record.get("logfile_id");
            final Object epochHour = record.get("epoch_hour");
            final Object objectFormat = record.get("object_format");
            System.out.println(id + " " + epochHour + " " + objectFormat);
        }
    }

    @Test
    public void testMigrationProcedure() {
        final Connection conn = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
        final DSLContext ctx = DSL.using(conn, SQLDialect.MARIADB);
        final EpochMigrationTempTable epochMigrationTempTable = new EpochMigrationTempTable(ctx, "journaldb");
        epochMigrationTempTable.create();
        ctx.insertInto(DSL.table("logfile"));
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EpochMigrationTempTable.class).verify();
    }
}
