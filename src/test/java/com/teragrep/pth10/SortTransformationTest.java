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

import java.util.Arrays;
import java.util.List;

/**
 * Tests for the new ProcessingStack implementation Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SortTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortTransformationTest.class);

    private final String testFile = "src/test/resources/sortTransformationTest_data*.jsonl"; // * to make the path into a directory path

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

    // FIXME fix these when sort command is fixed on parser side
    // (spaces before fields or auto(), num(), etc.)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // ascending auto sortByType with desc override
    public void testSortPlusAutoWithDesc() {
        streamingTestUtil.performDPLTest("index=index_A | sort + auto(offset) desc", testFile, ds -> {
            List<Row> listOfOffset = ds.select("offset").collectAsList();
            long firstOffset = listOfOffset.get(0).getLong(0);
            long lastOffset = listOfOffset.get(listOfOffset.size() - 1).getLong(0);

            Assertions.assertEquals(10, firstOffset);
            Assertions.assertEquals(1, lastOffset);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // override default/auto sorting
    public void testSortPlusStrPlainIntegerLimit() {
        streamingTestUtil.performDPLTest("index=index_A | sort 10 + str(offset)", testFile, ds -> {
            List<Row> listOfOffset = ds.select("offset").collectAsList();

            Assertions
                    .assertEquals(
                            "[1, 10, 2, 3, 4, 5, 6, 7, 8, 9]", Arrays.toString(listOfOffset.stream().map(r -> r.getAs(0).toString()).toArray())
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // descending sourcetype (pth03 parsing issue?, seems to be ascending)
    public void testSortPLusAndLimit() {
        streamingTestUtil.performDPLTest("index=index_A | sort limit=0 + sourcetype", testFile, ds -> {
            List<Row> listOfSourcetype = ds.select("sourcetype").collectAsList();

            Assertions
                    .assertEquals(
                            "[stream1, stream1, stream1, stream1, stream1, stream2, stream2, stream2, stream2, stream2]",
                            Arrays.toString(listOfSourcetype.stream().map(r -> r.getAs(0).toString()).toArray())
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // descending sort by ip address type
    public void testSortMinusIp() {
        streamingTestUtil.performDPLTest("index=index_A | sort - ip(source)", testFile, ds -> {
            List<Row> listOfSource = ds.select("source").collectAsList();

            Assertions
                    .assertEquals(
                            "[127.9.9.9, 127.8.8.8, 127.7.7.7, 127.6.6.6, 127.5.5.5, 127.4.4.4, 127.3.3.3, 127.2.2.2, 127.1.1.1, 127.0.0.0]",
                            Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // sort with aggregate
    public void testSortPlusNumAfterAgg() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats count(offset) as count_offset avg(offset) as avg_offset by sourcetype | sort +num(avg_offset)",
                        testFile, ds -> {
                            List<Row> listOfSource = ds.select("avg_offset").collectAsList();

                            Assertions
                                    .assertEquals("[5.0, 6.0]", Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray()));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // chained sort
    public void testSortMinusNum() {
        streamingTestUtil.performDPLTest("index=index_A | sort - num(offset)", testFile, ds -> {
            List<Row> listOfSource = ds.select("offset").collectAsList();

            Assertions
                    .assertEquals(
                            "[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]", Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // sort with a group by aggregate, descending sort
    public void testSortMinusNumAfterAgg() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats max(offset) AS max_off by id | sort -num(max_off)", testFile, ds -> {
                            List<Row> listOfSource = ds.select("max_off").collectAsList();

                            Assertions
                                    .assertEquals(
                                            "[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]",
                                            Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // sort with a group by aggregate, ascending sort
    public void testSortPlusNumAfterSplitBy() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats max(offset) AS max_off by id | sort +num(max_off)", testFile, ds -> {
                            List<Row> listOfSource = ds.select("max_off").collectAsList();

                            Assertions
                                    .assertEquals(
                                            "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]",
                                            Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // sort with a group by aggregate with auto sort
    public void testSortMinusAutoAfterSplitBy() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats max(offset) AS max_off by id | sort -auto(max_off)", testFile, ds -> {
                            List<Row> listOfSource = ds.select("max_off").collectAsList();

                            Assertions
                                    .assertEquals(
                                            "[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]",
                                            Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // auto sort after eval
    public void TestSortMinusAutoAfterEval() {
        streamingTestUtil.performDPLTest("index=index_A | eval a = offset + 4 | sort -auto(a)", testFile, ds -> {
            List<Row> listOfSource = ds.select("a").collectAsList();

            Assertions
                    .assertEquals(
                            "[14, 13, 12, 11, 10, 9, 8, 7, 6, 5]",
                            Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // auto sort strings
    public void testSortPlusAutoOnStrings() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval a = if ( offset < 6, \"abc\", \"bcd\") | sort +auto(a)", testFile, ds -> {
                            List<Row> listOfSource = ds.select("a").collectAsList();

                            Assertions
                                    .assertEquals(
                                            "[[abc], [abc], [abc], [abc], [abc], [bcd], [bcd], [bcd], [bcd], [bcd]]",
                                            Arrays.toString(listOfSource.stream().map(r -> r.getList(0).toString()).toArray())
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // auto sort ip addresses
    public void testSortPlusAutoOnIp() {
        streamingTestUtil.performDPLTest("index=index_A | sort +auto(source)", testFile, ds -> {
            List<Row> listOfSource = ds.select("source").collectAsList();

            Assertions
                    .assertEquals(
                            "[127.0.0.0, 127.1.1.1, 127.2.2.2, 127.3.3.3, 127.4.4.4, 127.5.5.5, 127.6.6.6, 127.7.7.7, 127.8.8.8, 127.9.9.9]",
                            Arrays.toString(listOfSource.stream().map(r -> r.getAs(0).toString()).toArray())
                    );
        });
    }
}
