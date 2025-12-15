/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2025 Suomen Kanuuna Oy
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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EpochMigrationStepTest {

    private final String testFile = "src/test/resources/IplocationTransformationTest_data*.jsonl"; // * to make the path into a directory path
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

    private final String tableName = "epoch_migration_test";
    private final String username = "sa";
    private final String password = "";
    private final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    private final Connection conn = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(url, username, password));

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
        this.streamingTestUtil.setEnv();
        String createLogfileTable = "CREATE TABLE IF NOT EXISTS " + tableName + "( " + "id BIGINT UNSIGNED NOT NULL,"
                + " epoch_hour BIGINT UNSIGNED NULL," + " PRIMARY KEY(id" + ")";
        Assertions.assertDoesNotThrow(() -> {
            try (PreparedStatement ps = conn.prepareStatement(createLogfileTable)) {
                Assertions.assertEquals(1, ps.executeUpdate());
            }
        });
        String insertIDs = "INSERT INTO" + tableName + "(id) VALUES (1),(2),(3),(4),(5);";
        Assertions.assertDoesNotThrow(() -> {
            try (PreparedStatement ps = conn.prepareStatement(insertIDs)) {
                Assertions.assertEquals(5, ps.executeUpdate());
            }
        });
    }

    @BeforeEach
    void setUp() {
        streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        streamingTestUtil.tearDown();
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @Disabled("pth_03 support for command missing")
    public void testEpochMigrationStep() {
        streamingTestUtil
                .performDPLTest("index=index_A | teragrep exec epochMigration table " + tableName, testFile, ds -> {
                    String selectOffsets = "SELECT id, epoch_hour FROM " + tableName + " ORDER BY id";
                    Assertions.assertDoesNotThrow(() -> {
                        try (PreparedStatement preparedStatement = conn.prepareStatement(selectOffsets)) {
                            ResultSet resultSet = preparedStatement.executeQuery();
                            int loops = 0;
                            while (resultSet.next()) {
                                loops++;
                                long id = resultSet.getLong("id");
                                long epochHour = resultSet.getObject("epoch_hour", Long.class);
                                // TODO add assertions for epoch_hour values
                            }
                            Assertions.assertEquals(5, loops);
                        }
                    });
                });
    }
}
