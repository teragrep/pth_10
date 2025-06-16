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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for the new ProcessingStack implementation Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TimechartStreamingTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimechartStreamingTest.class);

    private final String testFile = "src/test/resources/dedup_test_data*.jsonl"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
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
    public void testTimechartBinSizeForMonthSpan() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-01-01T00:00:00Z latest=2021-01-01T00:00:00Z | timechart span=1mon count(_raw) as craw by sourcetype",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            false,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            false,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("craw", DataTypes.LongType, false, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            List<Row> listOfTime = ds.select("_time").collectAsList();

                            // span buckets one per month (one extra due to timezones)
                            Assertions.assertEquals(13, listOfTime.size());
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTimechartBinSizeForMinuteSpan() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-12-12T00:00:00Z latest=2020-12-12T00:30:00Z | timechart span=1min count(_raw) as craw by sourcetype",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            false,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            false,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("craw", DataTypes.LongType, false, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            List<Row> listOfTime = ds.select("_time").collectAsList();

                            // span buckets one per minute for 30mins
                            Assertions.assertEquals(31, listOfTime.size());
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTimechartSpanWithSplitBY() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | timechart span=1min count(_raw) as craw by sourcetype", testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            false,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            false,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("craw", DataTypes.LongType, false, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            List<String> listOfSourcetype = ds
                                    .select("sourcetype")
                                    .na()
                                    .drop("any")
                                    .dropDuplicates()
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .filter(str -> !str.equals("0"))
                                    .collect(Collectors.toList());
                            System.out.println(listOfSourcetype);

                            Assertions
                                    .assertTrue(listOfSourcetype.contains("stream1") && listOfSourcetype.contains("stream2"));
                            Assertions.assertEquals(2, listOfSourcetype.size());
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTimechartSplitBy() {
        streamingTestUtil.performDPLTest("index=index_A | timechart count by host", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, false, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );

            List<String> listOfHosts = ds
                    .select("host")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .filter(str -> !str.equals("0"))
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, listOfHosts.size());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTimechartBasicCount() {
        streamingTestUtil.performDPLTest("index=index_A | timechart count", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, false, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );

            List<String> listOfCount = ds
                    .select("count")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .filter(str -> !str.equals("0"))
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, listOfCount.size());
            Assertions.assertEquals("10", listOfCount.get(0));
        });
    }
}
