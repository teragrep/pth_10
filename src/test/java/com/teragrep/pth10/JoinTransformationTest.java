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

/**
 * Tests for the new ProcessingStack implementation Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JoinTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformationTest.class);

    private String testFile = "src/test/resources/joinTransformationTest_data*.jsonl"; // * to make the path into a directory path
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
    public void joinRightSideHdfsLoadTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=-100y | eval a=12345 | teragrep exec hdfs save /tmp/join0 overwrite=true",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("a", DataTypes.IntegerType, false, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Row r = ds.select("a").distinct().first();
                            Assertions.assertEquals("12345", r.getAs(0).toString());
                        }
                );
        this.streamingTestUtil.setUp(); // reset for another run
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=-100y | join partition [ | teragrep exec hdfs load /tmp/join0 | where partition >= 0 ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.IntegerType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Row r = ds.select("R_a").distinct().first();
                            Assertions.assertEquals("12345", r.getAs(0).toString());
                        }
                );
    }

    // type=left max=3
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinTypeLeftMax3Test() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | join type=left max=3 offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ] <!--| fields + _time offset a ]-->",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            List<Row> listOfRows = ds.collectAsList();

                            // 3 rows should be not null, since only three subsearch matches are requested using max=3
                            int notNulls = 0;
                            for (Row r : listOfRows) {
                                if (r.getAs("R_a") != null) {
                                    notNulls++;
                                }
                            }

                            Assertions.assertEquals(3, notNulls, "subsearch limit 3, so 3 should be not null");
                        }
                );
    }

    // max=2
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinMax2TypeImplicitTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | join max=2 offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Assertions.assertEquals(2, ds.count(), "Should return 2 rows");
                        }
                );
    }

    // max=0 overwrite=true
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void join0MaxOverwriteExplicitTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream3\", \"2\") | join max=0 overwrite=true offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Assertions.assertEquals(10, ds.count(), "Should return 10 rows");

                            List<Row> listOfAColumn = ds.select("a").collectAsList();

                            for (Row r : listOfAColumn) {
                                String val = r.getString(0);
                                Assertions.assertTrue(val != null, "All rows should have a valid value (non-null) !");
                            }
                        }
                );
    }

    // no params
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinNoExtraParamsTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | join offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Assertions.assertEquals(1, ds.count(), "Should return 1 row");
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinNoExtraCommandsOnMainSearchTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | join max=0 overwrite=true offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            List<Row> listOfRows = ds.collectAsList();
                            Assertions
                                    .assertEquals(
                                            10, listOfRows.size(),
                                            "Should return 10 rows, instead returned: " + listOfRows.size()
                                    );
                        }
                );
    }

    // max=0, usetime=true, earlier=true
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinMax0UsetimeTrueEarlierTrueTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | join max=0 usetime=true earlier=true offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Assertions.assertEquals(10, ds.count(), "Should return 10 rows");
                        }
                );
    }

    // max=0, usetime=true, earlier=false, overwrite=false
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinMax0UsetimeTrueEarlierFalseOverwriteFalseTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | join max=0 usetime=true earlier=false overwrite=false offset [ search index=index_A | eval a=case(sourcetype=\"stream1\", \"1\", sourcetype=\"stream2\", \"2\") ]",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
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
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R__time", DataTypes.TimestampType, true, new MetadataBuilder().build()), new StructField("R_id", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("R__raw", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("R_index", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("R_sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("R_host", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("R_source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("R_partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("R_a", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            List<Row> listOfRows = ds.collectAsList();
                            Assertions
                                    .assertEquals(
                                            10, listOfRows.size(),
                                            "Should return 10 rows, instead returned: " + listOfRows.size()
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void joinInvalidRightSideTest() {
        RuntimeException rte = this.streamingTestUtil
                .performThrowingDPLTest(
                        RuntimeException.class, "| makeresults count=1 | eval a=1 | join a [search]", testFile, ds -> {
                        }
                );

        Assertions
                .assertEquals(
                        "Join command encountered an error: Subsearch dataset (right side) missing expected field 'a'",
                        rte.getMessage()
                );
    }
}
