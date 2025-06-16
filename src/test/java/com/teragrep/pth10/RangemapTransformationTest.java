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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RangemapTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RangemapTransformationTest.class);

    private final String testFile = "src/test/resources/numberData_0*.jsonl"; // * to make the path into a directory path
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
    public void testRangemap() {
        streamingTestUtil.performDPLTest("index=* | rangemap field=_raw", testFile, ds -> {
            List<Row> result = ds.select("range").distinct().collectAsList();
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals("None", result.get(0).getList(0).get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testRangemapDefault() {
        streamingTestUtil.performDPLTest("index=* | rangemap field=_raw default=xyz", testFile, ds -> {
            List<Row> result = ds.select("range").distinct().collectAsList();
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals("xyz", result.get(0).getList(0).get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testRangemapAttributeName() {
        streamingTestUtil
                .performDPLTest("index=* | rangemap field=_raw lo=0-5 med=6-34 hi=35-48 vlo=-20--10", testFile, ds -> {
                    List<Row> result = ds.select("_raw", "range").collectAsList();
                    Assertions.assertEquals(5, result.size());
                    result.forEach(r -> {
                        double val = Double.parseDouble(r.getAs(0).toString());
                        if (val == 35d) {
                            Assertions.assertEquals("hi", r.getList(1).get(0));
                        }
                        else if (val == 10d) {
                            Assertions.assertEquals("med", r.getList(1).get(0));
                        }
                        else if (val == -10d) {
                            Assertions.assertEquals("vlo", r.getList(1).get(0));
                        }
                        else if (val == 0d) {
                            Assertions.assertEquals("lo", r.getList(1).get(0));
                        }
                        else if (val == 47.2d) {
                            Assertions.assertEquals("hi", r.getList(1).get(0));
                        }
                        else {
                            Assertions.fail("Unexpected _raw value: " + val);
                        }
                    });
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testRangemapNoFieldParameter() {
        IllegalArgumentException iae = this.streamingTestUtil
                .performThrowingDPLTest(IllegalArgumentException.class, "index=* | rangemap", testFile, ds -> {
                });
        Assertions.assertEquals("Field parameter is required!", iae.getMessage());
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testRangemapWithUnmatchedRange() {
        streamingTestUtil
                .performDPLTest(
                        "| makeresults | eval _raw = \"string\" | rangemap field=_raw r0=0-10 r1=11-20", testFile,
                        ds -> {
                            // strings result in default value
                            List<Row> result = ds.select("range").distinct().collectAsList();
                            Assertions.assertEquals(1, result.size());
                            Assertions.assertEquals("None", result.get(0).getList(0).get(0));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testrangemapMultiValue() {
        streamingTestUtil
                .performDPLTest(
                        "index=* | eval a = mvappend(\"1\",\"3\",\"3\",\"a\") |rangemap field=a lo=1-2 hi=3-4",
                        testFile, ds -> {
                            List<Row> result = ds.select("range").distinct().collectAsList();
                            Assertions.assertEquals(1, result.size());
                            List<String> resultList = result.get(0).getList(0);
                            Assertions.assertEquals(2, resultList.size());
                            List<String> expected = Arrays.asList("lo", "hi");

                            for (String res : resultList) {
                                if (!expected.contains(res)) {
                                    Assertions.fail("Expected values did not contain result value: " + res);
                                }
                            }
                        }
                );
    }
}
