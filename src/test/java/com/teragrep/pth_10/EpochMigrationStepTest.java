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
package com.teragrep.pth_10;

import com.teragrep.pth_10.steps.teragrep.connection.ConnectionPoolSingleton;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationStepTest {

    private final String testFile = "src/test/resources/epochMigrationData*.jsonl"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });
    private StreamingTestUtil streamingTestUtil;

    @Container
    private final MariaDBContainer<?> mariadb = new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5"))
            .withPrivilegedMode(false)
            .withDatabaseName("journaldb")
            .withUsername("username")
            .withPassword("test");

    @BeforeAll
    void setEnv() {
        streamingTestUtil = new StreamingTestUtil(testSchema);
        streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        Map<String, String> opts = new HashMap<>();
        opts.put("dpl.pth_06.archive.db.username", mariadb.getUsername());
        opts.put("dpl.pth_06.archive.db.password", mariadb.getPassword());
        opts.put("dpl.pth_06.archive.db.url", mariadb.getJdbcUrl());

        final Connection conn = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement("DROP TABLE IF EXISTS journaldb.logfile").execute());
        Assertions
                .assertDoesNotThrow(() -> conn.prepareStatement("DROP TABLE IF EXISTS journaldb.object_format").execute());
        final String createLogfileTable = "CREATE TABLE journaldb.logfile (id BIGINT NOT NULL, epoch_hour BIGINT NULL, object_format_id BIGINT NULL, PRIMARY KEY(id))";
        final String createObjectFormatTable = "CREATE TABLE journaldb.object_format (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(255) NULL, PRIMARY KEY(id))";
        Assertions.assertDoesNotThrow(() -> {
            try (
                    final PreparedStatement logfilePreparedStatement = conn.prepareStatement(createLogfileTable); final PreparedStatement objectFormatPreparedStatement = conn.prepareStatement(createObjectFormatTable)
            ) {
                logfilePreparedStatement.execute();
                objectFormatPreparedStatement.execute();
            }
        });
        final String insertIDs = "INSERT INTO journaldb.logfile (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);";
        Assertions.assertDoesNotThrow(() -> {
            try (final PreparedStatement insertLogfilePreparedStatement = conn.prepareStatement(insertIDs)) {
                Assertions.assertEquals(10, insertLogfilePreparedStatement.executeUpdate());
            }
        });
        final Path path = Paths.get("src/test/resources/sql/epoch_migration_procedure.sql");
        final String createProcedureSQL = Assertions.assertDoesNotThrow(() -> Files.readString(path));
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement(createProcedureSQL).execute());
        streamingTestUtil.setUp();
        streamingTestUtil.setCustomConfigOptions(opts);
        ConnectionPoolSingleton.resetForTest();
    }

    @AfterEach
    void tearDown() {
        streamingTestUtil.tearDown();
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMigrateEpochCommandUpdatesEpochValuesToSQLMetadata() {
        streamingTestUtil.performDPLTest("index=index_A | teragrep exec migrate epoch", testFile, ds -> {
            final String selectOffsets = "SELECT l.id, l.epoch_hour, f.name FROM journaldb.logfile l LEFT JOIN journaldb.object_format f ON l.object_format_id = f.id ORDER BY l.id";
            final List<Long> updatedEpochs = new ArrayList<>();
            final List<String> updatedObjectFormats = new ArrayList<>();
            Assertions.assertDoesNotThrow(() -> {
                final Connection conn = DriverManager
                        .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword());
                try (final PreparedStatement preparedStatement = conn.prepareStatement(selectOffsets)) {
                    final ResultSet resultSet = preparedStatement.executeQuery();
                    int loops = 0;
                    while (resultSet.next()) {
                        final Long epochHour = resultSet.getObject("epoch_hour", Long.class);
                        final String objectFormat = resultSet.getObject("name", String.class);
                        updatedEpochs.add(epochHour);
                        updatedObjectFormats.add(objectFormat);
                        loops++;
                    }
                    Assertions.assertEquals(10, loops);
                }
            });
            final List<Long> expectedEpochs = Arrays
                    .asList(
                            1693904400L, 1693908000L, 1693911600L, 1693915200L, 1693918800L, 1693922400L, 1693926000L,
                            1693929600L, 1693933200L, 1693936800L
                    );
            Assertions.assertEquals(expectedEpochs, updatedEpochs);
            final List<String> expectedObjectFormatIds = Arrays
                    .asList(
                            "rfc5424", "rfc5424", "rfc5424", "rfc5424", "rfc5424", "rfc5424", "rfc5424", "rfc5424",
                            "rfc5424", "rfc5424"
                    );
            Assertions.assertEquals(expectedEpochs, updatedEpochs);
            Assertions.assertEquals(expectedObjectFormatIds, updatedObjectFormats);
            // assert only completion message visible in results
            final List<String> resultPrint = ds
                    .select("_raw")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(1, resultPrint.size());
            final String result = resultPrint.get(0);
            final String expected = "Epoch migration was completed.";
            Assertions.assertEquals(expected, result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMigrateNullTimeColumnThrowsException() {
        // tests that rows with null _time value are not silently accepted
        final RuntimeException exception = streamingTestUtil
                .performThrowingDPLTest(
                        RuntimeException.class, "index=index_B | teragrep exec migrate epoch", testFile, ds -> {
                        }
                );
        final String expected = "Column '_time' was null, cannot convert to epoch seconds";
        Assertions.assertTrue(exception.getMessage().contains(expected));
    }
}
