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

import com.teragrep.pth_10.steps.teragrep.connection.TestingConnectionSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep1;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterAll;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationForeachPartitionFunctionTest {

    @Container
    private final MariaDBContainer<?> mariadb = new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5"))
            .withPrivilegedMode(false)
            .withDatabaseName("journaldb")
            .withUsername("username")
            .withPassword("test");

    private TestingConnectionSource connectionSource;

    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });

    @BeforeEach
    void setup() {
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
                InsertValuesStep1<Record, Long> query = ctx.insertInto(DSL.table("logfile"), idField);

                for (long i = 1; i <= 10; i++) {
                    query = query.values(i);
                }
                query.execute();
                final Path path = Paths.get("src/test/resources/sql/epoch_migration_procedure.sql");
                final String createProcedureSQL = Files.readString(path);
                ctx.execute(createProcedureSQL);
            }
        });

        final Map<String, String> optsMap = new HashMap<>();
        optsMap.put("dpl.pth_06.archive.db.username", mariadb.getUsername());
        optsMap.put("dpl.pth_06.archive.db.password", mariadb.getPassword());
        optsMap.put("dpl.pth_06.archive.db.url", mariadb.getJdbcUrl());
        final Config config = ConfigFactory.parseMap(optsMap);
        this.connectionSource = new TestingConnectionSource(config);
    }

    @AfterAll
    void close() {
        connectionSource.close();
    }

    @Test
    public void testFullBatchesExecuted() {
        final EpochMigrationForeachPartitionFunction foreachPartitionFunction = new EpochMigrationForeachPartitionFunction(
                connectionSource,
                "journaldb",
                3,
                new Settings()
        );
        foreachPartitionFunction
                .call(Arrays.asList(genericResultRow(1, 100), genericResultRow(2, 200), genericResultRow(3, 300)).iterator());
        final Map<Long, Long> results = Assertions.assertDoesNotThrow(this::nonNullLogfilesMap);
        Assertions.assertDoesNotThrow(() -> printLogfile());
        Assertions.assertIterableEquals(Arrays.asList(1L, 2L, 3L), results.keySet());
        Assertions.assertIterableEquals(Arrays.asList(100L, 200L, 300L), results.values());
    }

    @Test
    public void testMultipleBatchesExecuted() {
        final EpochMigrationForeachPartitionFunction foreachPartitionFunction = new EpochMigrationForeachPartitionFunction(
                connectionSource,
                "journaldb",
                2,
                new Settings()
        );
        foreachPartitionFunction
                .call(Arrays.asList(genericResultRow(1, 100), genericResultRow(2, 200), genericResultRow(3, 300), genericResultRow(4, 400), genericResultRow(5, 500), genericResultRow(6, 600)).iterator());
        final Map<Long, Long> results = Assertions.assertDoesNotThrow(this::nonNullLogfilesMap);
        Assertions.assertIterableEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), results.keySet());
        Assertions.assertIterableEquals(Arrays.asList(100L, 200L, 300L, 400L, 500L, 600L), results.values());
    }

    @Test
    public void testEmptyIteratorCallDoesNothing() {
        final Map<Long, Long> resultsBefore = Assertions.assertDoesNotThrow(this::nonNullLogfilesMap);
        Assertions.assertTrue(resultsBefore.isEmpty());
        final EpochMigrationForeachPartitionFunction foreachPartitionFunction = new EpochMigrationForeachPartitionFunction(
                connectionSource,
                "journaldb",
                3,
                new Settings()
        );
        final List<Row> emptyList = Collections.emptyList();
        foreachPartitionFunction.call(emptyList.iterator());
        final Map<Long, Long> resultsAfter = Assertions.assertDoesNotThrow(this::nonNullLogfilesMap);
        Assertions.assertTrue(resultsAfter.isEmpty());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EpochMigrationForeachPartitionFunction.class).withIgnoredFields("LOGGER").verify();
    }

    private Row genericResultRow(long id, long epochSeconds) {
        final String syslogJsonResult = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"2007/10-08/epochHour/migration/test.logGLOB-2007100814.log.gz\",\"partition\":\"id\"},\"timestamp\":{\"rfc5424timestamp\":\"2014-06-20T09:14:07.12345+00:00\",\"epoch\":1403255647123450,\"path-extracted\":\"2007-10-08T14:00+03:00[Europe/Helsinki]\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
        return new GenericRowWithSchema(new Object[] {
                Timestamp.from(Instant.ofEpochSecond(epochSeconds)),
                syslogJsonResult,
                "index",
                "sourcetype",
                "host",
                "source",
                String.valueOf(id),
                1L
        }, testSchema);
    }

    // key=id, value=epoch
    private Map<Long, Long> nonNullLogfilesMap() throws SQLException {
        final Map<Long, Long> resultMap = new HashMap<>();
        final String selectEpochs = "SELECT id, epoch_hour FROM journaldb.logfile WHERE epoch_hour IS NOT NULL";
        try (final Connection connection = connectionSource.get()) {
            try (final PreparedStatement statement = connection.prepareStatement(selectEpochs)) {
                final ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    resultMap.put(resultSet.getLong(1), resultSet.getLong(2));
                }
            }
        }
        return resultMap;
    }

    private void printLogfile() throws SQLException {
        final String selectEpochs = "SELECT l.id, l.epoch_hour, f.name FROM journaldb.logfile l LEFT JOIN journaldb.object_format f ON l.object_format_id = f.id";
        try (final Connection connection = connectionSource.get()) {
            try (final PreparedStatement statement = connection.prepareStatement(selectEpochs)) {
                final ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    final String id = resultSet.getString("id");
                    final String epochHour = resultSet.getString("epoch_hour");
                    final String name = resultSet.getString("name");
                    System.out.println("id:" + id + "\t" + "epoch_hour:" + epochHour + "\t" + "object_format:" + name);
                }
            }
        }
    }
}
