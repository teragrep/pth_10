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
import com.teragrep.pth_10.steps.teragrep.migrate.TeragrepEpochMigrationStep;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    private final String username = "testuser";
    private final String password = "testpass";
    private final String url = "jdbc:h2:mem:test_" + UUID.randomUUID() + ";MODE=MariaDB;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    private Connection conn;
    private Map<String, String> opts;

    @BeforeAll
    void setEnv() {
        conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(url, username, password));
        opts = new HashMap<>();
        opts.put("dpl.pth_06.bloom.db.url", url);
        opts.put("dpl.pth_10.bloom.db.username", username);
        opts.put("dpl.pth_10.bloom.db.password", password);
        opts.put("dpl.archive.db.journaldb.name", "journaldb");
        streamingTestUtil = new StreamingTestUtil(testSchema);
        streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS journaldb").execute());
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement("USE journaldb").execute());
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement("DROP TABLE IF EXISTS logfile").execute());
        final String createLogfileTable = "CREATE TABLE logfile (id BIGINT NOT NULL, epoch_hour BIGINT NULL, PRIMARY KEY(id))";
        Assertions.assertDoesNotThrow(() -> {
            try (final PreparedStatement ps = conn.prepareStatement(createLogfileTable)) {
                ps.execute();
            }
        });
        final String insertIDs = "INSERT INTO logfile (id) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);";
        Assertions.assertDoesNotThrow(() -> {
            try (final PreparedStatement ps = conn.prepareStatement(insertIDs)) {
                Assertions.assertEquals(10, ps.executeUpdate());
            }
        });
        streamingTestUtil.setUp();
        streamingTestUtil.setCustomConfigOptions(opts);
        ConnectionPoolSingleton.resetForTest();
    }

    @AfterEach
    void tearDown() {
        streamingTestUtil.tearDown();
    }

    @AfterAll
    public void close() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMigrateEpochCommandUpdatesEpochValuesToSQLMetadata() {
        streamingTestUtil.performDPLTest("index=index_A | teragrep exec migrate epoch", testFile, ds -> {
            final String selectOffsets = "SELECT id, epoch_hour FROM logfile ORDER BY id";
            final List<Long> updatedEpochs = new ArrayList<>();
            Assertions.assertDoesNotThrow(() -> {
                try (PreparedStatement preparedStatement = conn.prepareStatement(selectOffsets)) {
                    final ResultSet resultSet = preparedStatement.executeQuery();
                    int loops = 0;
                    while (resultSet.next()) {
                        final Long epochHour = resultSet.getObject("epoch_hour", Long.class);
                        updatedEpochs.add(epochHour);
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
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMigrateNullTimeColumnThrowsException() {
        // tests that rows with null _time value are not silently accepted
        streamingTestUtil
                .performThrowingDPLTest(
                        StreamingQueryException.class, "index=index_B | teragrep exec migrate epoch", testFile, ds -> {
                        }
                );
    }

    @Test
    public void testContract() {
        // ignore fields from superclass dpf_02 AbstractStep that are not part of the equality
        EqualsVerifier
                .forClass(TeragrepEpochMigrationStep.class)
                .withIgnoredFields("LOGGER", "properties", "aggregatesUsedBefore")
                .verify();
    }
}
