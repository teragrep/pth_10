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
package com.teragrep.pth10;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AggregateAfterSequentialCommandTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateAfterSequentialCommandTest.class);

    private final String testFile = "src/test/resources/rexTransformationTest_data*.jsonl"; // * to make the path into a directory path
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

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void aggregateAfterDedupTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | spath path=rainfall_rate | dedup rainfall_rate | stats sum(rainfall_rate)",
                        testFile, ds -> {
                            Assertions.assertEquals("139.875", ds.select("sum(rainfall_rate)").first().getString(0));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void aggregateBeforeSeqModeAndAfter() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | spath path=rainfall_rate | stats count(rainfall_rate) as cr by _raw | dedup cr | stats sum(cr)",
                        testFile, ds -> {
                            Assertions.assertEquals("5", ds.select("sum(cr)").first().getString(0));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void aggregateAfterHdfsLoadTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | spath | teragrep exec hdfs save /tmp/pth_10/aggregateAfterHdfsLoadTest overwrite=true",
                        testFile, ds -> {
                            Assertions.assertEquals(new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("wind_speed", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("latitude", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("rainfall_rate", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("atmosphere_cloud_liquid_water_content", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("atmosphere_water_vapor_content", DataTypes.StringType, true, new MetadataBuilder().build())
                            }), ds.schema());
                        }
                );
        this.streamingTestUtil.setUp(); // reset for 2nd query
        streamingTestUtil
                .performDPLTest(
                        "| teragrep exec hdfs load /tmp/pth_10/aggregateAfterHdfsLoadTest | dedup rainfall_rate | stats sum(rainfall_rate)",
                        testFile, ds -> {
                            Assertions.assertEquals("139.875", ds.select("sum(rainfall_rate)").first().getString(0));
                        }
                );
    }
}
