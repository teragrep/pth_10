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
import org.jooq.conf.Settings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationForeachPartitionFunctionTest {

    private final String user = "testuser";
    private final String password = "testpass";
    private final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1";
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

    @BeforeAll
    void setup() {
        final Map<String, String> optsMap = new HashMap<>();
        optsMap.put("dpl.pth_10.bloom.db.username", user);
        optsMap.put("dpl.pth_10.bloom.db.password", password);
        optsMap.put("dpl.pth_06.bloom.db.url", url);
        final Config config = ConfigFactory.parseMap(optsMap);
        this.connectionSource = new TestingConnectionSource(config);
    }

    @AfterAll
    void close() {
        connectionSource.close();
    }

    @BeforeEach
    public void populateDatabase() {
        final Connection conn = Assertions.assertDoesNotThrow(() -> connectionSource.get());
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS journaldb").execute());
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement("DROP TABLE IF EXISTS journaldb.logfile").execute());
        final String createLogfileTable = "CREATE TABLE journaldb.logfile (id BIGINT NOT NULL, epoch_hour BIGINT NULL, PRIMARY KEY(id))";
        Assertions.assertDoesNotThrow(() -> {
            try (final PreparedStatement ps = conn.prepareStatement(createLogfileTable)) {
                ps.execute();
            }
        });
        final String insertIDs = "INSERT INTO journaldb.logfile (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);";
        Assertions.assertDoesNotThrow(() -> {
            try (final PreparedStatement ps = conn.prepareStatement(insertIDs)) {
                Assertions.assertEquals(10, ps.executeUpdate());
            }
        });
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testPartialBatchIsExecuted() {
        final EpochMigrationForeachPartitionFunction foreachPartitionFunction = new EpochMigrationForeachPartitionFunction(
                connectionSource,
                "journaldb",
                100,
                new Settings()
        );
        foreachPartitionFunction
                .call(Arrays.asList(genericResultRow(1, 100), genericResultRow(2, 200), genericResultRow(3, 300)).iterator());

        final Map<Long, Long> results = Assertions.assertDoesNotThrow(this::nonNullLogfilesMap);
        Assertions.assertIterableEquals(Arrays.asList(1L, 2L, 3L), results.keySet());
        Assertions.assertIterableEquals(Arrays.asList(100L, 200L, 300L), results.values());
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
    public void testRollbackOnException() {
        final EpochMigrationForeachPartitionFunction foreachPartitionFunction = new EpochMigrationForeachPartitionFunction(
                connectionSource,
                "journaldb",
                2,
                new Settings()
        );
        final String syslogJsonResult = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"2007/10-08/epochHour/migration/test.logGLOB-2007100814.log.gz\",\"partition\":\"id\"},\"timestamp\":{\"rfc5242timestamp\":\"2014-06-20T09:14:07.12345+00:00\",\"epochHour\":1403255647123450,\"path-extracted\":\"2007-10-08T14:00+03:00[Europe/Helsinki]\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
        Row badRow = new GenericRowWithSchema(new Object[] {
                null, // throws exception
                syslogJsonResult,
                "index",
                "sourcetype",
                "host",
                "source",
                String.valueOf(4),
                1L
        }, testSchema);
        Assertions.assertThrows(RuntimeException.class, () -> {
            foreachPartitionFunction
                    .call(Arrays.asList(genericResultRow(1, 100), genericResultRow(2, 200), genericResultRow(3, 300), badRow).iterator());
        });
        final Map<Long, Long> results = Assertions.assertDoesNotThrow(this::nonNullLogfilesMap);
        // batch size is 2 so the two first rows should be present while the third row should be rolled back
        Assertions.assertIterableEquals(Arrays.asList(1L, 2L), results.keySet());
        Assertions.assertIterableEquals(Arrays.asList(100L, 200L), results.values());
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
        final String syslogJsonResult = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"2007/10-08/epochHour/migration/test.logGLOB-2007100814.log.gz\",\"partition\":\"id\"},\"timestamp\":{\"rfc5242timestamp\":\"2014-06-20T09:14:07.12345+00:00\",\"epochHour\":1403255647123450,\"path-extracted\":\"2007-10-08T14:00+03:00[Europe/Helsinki]\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
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
}
