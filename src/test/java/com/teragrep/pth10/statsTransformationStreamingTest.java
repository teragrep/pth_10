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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class statsTransformationStreamingTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(statsTransformationStreamingTest.class);

    private final String testFile = "src/test/resources/predictTransformationTest_data*.jsonl"; // * to make the path into a directory path
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
    public void statsTransform_Streaming_AggDistinctCount_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats dc(offset) AS stats_test_result", testFile, ds -> {
            List<String> listOfResult = ds
                    .select("stats_test_result")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions
                    .assertEquals(Arrays.asList("25"), listOfResult, "Batch consumer dataset did not contain the expected values !");
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void statsTransform_Streaming_AggEarliest_Test() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats earliest(offset) AS stats_test_result", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("stats_test_result")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions
                            .assertEquals(Arrays.asList("15"), listOfResult, "Batch consumer dataset did not contain the expected values !");
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSplittingByTime() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats avg(offset) AS stats_test_result BY _time", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("stats_test_result")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    List<String> expected = Arrays
                            .asList(
                                    "15.0", "16.0", "17.0", "18.0", "19.0", "20.0", "21.0", "22.0", "23.0", "24.0",
                                    "13.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0", "11.0",
                                    "12.0", "13.0", "14.0"
                            ); // weird timestamps in the JSON file
                    Assertions
                            .assertEquals(
                                    expected, listOfResult,
                                    "Batch consumer dataset did not contain the expected values !"
                            );
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSplittingByString() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats avg(offset) AS stats_test_result BY sourcetype", testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("stats_test_result")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expected = Arrays.asList("3.0", "8.0", "13.0", "18.0", "23.0");
                            Assertions
                                    .assertEquals(
                                            expected, listOfResult,
                                            "Batch consumer dataset did not contain the expected values !"
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSplittingByNumber() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats avg(offset) AS stats_test_result BY id", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("stats_test_result")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    List<String> expected = Arrays
                            .asList(
                                    "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0", "11.0",
                                    "12.0", "13.0", "14.0", "15.0", "16.0", "17.0", "18.0", "19.0", "20.0", "21.0",
                                    "22.0", "23.0", "24.0", "25.0"
                            );
                    Assertions
                            .assertEquals(
                                    expected, listOfResult,
                                    "Batch consumer dataset did not contain the expected values !"
                            );
                });
    }

    // Sorts first by sourcetype and then in those sourcetypes it sorts by _time
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSplittingByMultipleColumns() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats avg(offset) AS stats_test_result BY sourcetype _time", testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("stats_test_result")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expected = Arrays
                                    .asList(
                                            "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0",
                                            "15.0", "11.0", "12.0", "13.0", "14.0", "16.0", "17.0", "18.0", "19.0",
                                            "20.0", "21.0", "22.0", "23.0", "24.0", "25.0"
                                    );
                            Assertions
                                    .assertEquals(
                                            expected, listOfResult,
                                            "Batch consumer dataset did not contain the expected values !"
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSplittingByNumericalStrings() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval a = offset + 0 | stats avg(offset) AS stats_test_result BY a", testFile,
                        ds -> {
                            List<String> listOfResult = ds
                                    .select("stats_test_result")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expected = Arrays
                                    .asList(
                                            "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0",
                                            "11.0", "12.0", "13.0", "14.0", "15.0", "16.0", "17.0", "18.0", "19.0",
                                            "20.0", "21.0", "22.0", "23.0", "24.0", "25.0"
                                    );
                            Assertions
                                    .assertEquals(
                                            expected, listOfResult,
                                            "Batch consumer dataset did not contain the expected values !"
                                    );
                        }
                );
    }

    @Test
    public void statsTransform_Streaming_AggValues_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats values(offset) AS stats_test_result", testFile, ds -> {
            List<String> listOfResult = ds
                    .select("stats_test_result")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions
                    .assertEquals(
                            Collections
                                    .singletonList(
                                            "1\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n2\n20\n21\n22\n23\n24\n25\n3\n4\n5\n6\n7\n8\n9"
                                    ),
                            listOfResult, "Batch consumer dataset did not contain the expected values !"
                    );
        });
    }

    @Test
    public void statsTransform_Streaming_AggExactPerc_Test() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats exactperc50(offset) AS stats_test_result", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("stats_test_result")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions
                            .assertEquals(Collections.singletonList("13.0"), listOfResult, "Batch consumer dataset did not contain the expected values !");
                });
    }
}
