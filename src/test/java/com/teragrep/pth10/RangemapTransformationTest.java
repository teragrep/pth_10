/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RangemapTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RangemapTransformationTest.class);

    private final String testFile = "src/test/resources/numberData_0*.json"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
            }
    );

    private StreamingTestUtil streamingTestUtil;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
        this.streamingTestUtil.setEnv();
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rangemapTest0() {
        streamingTestUtil.performDPLTest(
            "index=* | rangemap field=_raw",
            testFile,
            ds -> {
                List<Row> result = ds.select("range").distinct().collectAsList();
                assertEquals(1, result.size());
                assertEquals("None", result.get(0).getList(0).get(0));
            }
        );
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rangemapTest1() {
        streamingTestUtil.performDPLTest(
            "index=* | rangemap field=_raw default=xyz",
            testFile,
            ds -> {
                List<Row> result = ds.select("range").distinct().collectAsList();
                assertEquals(1, result.size());
                assertEquals("xyz", result.get(0).getList(0).get(0));
            }
        );
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rangemapTest2() {
        streamingTestUtil.performDPLTest(
            "index=* | rangemap field=_raw lo=0-5 med=6-34 hi=35-48 vlo=-20--10",
            testFile,
            ds -> {
                List<Row> result = ds.select("_raw", "range").collectAsList();
                assertEquals(5, result.size());
                result.forEach(r -> {
                    double val = Double.parseDouble(r.getAs(0).toString());
                    if (val == 35d) {
                        assertEquals("hi", r.getList(1).get(0));
                    } else if (val == 10d) {
                        assertEquals("med", r.getList(1).get(0));
                    } else if (val == -10d) {
                        assertEquals("vlo", r.getList(1).get(0));
                    } else if (val == 0d) {
                        assertEquals("lo", r.getList(1).get(0));
                    } else if (val == 47.2d) {
                        assertEquals("hi", r.getList(1).get(0));
                    } else {
                        fail("Unexpected _raw value: " + val);
                    }
                });
            }
        );
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rangemapTest3() {
        IllegalArgumentException iae = this.streamingTestUtil.performThrowingDPLTest(IllegalArgumentException.class, "index=* | rangemap", testFile, ds -> {});
        assertEquals("Field parameter is required!", iae.getMessage());
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rangemapTest4() {
        streamingTestUtil.performDPLTest(
            "| makeresults | eval _raw = \"string\" | rangemap field=_raw r0=0-10 r1=11-20",
            testFile,
            ds -> {
                // strings result in default value
                List<Row> result = ds.select("range").distinct().collectAsList();
                assertEquals(1, result.size());
                assertEquals("None", result.get(0).getList(0).get(0));
            }
        );
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rangemapMultiValueTest() {
        streamingTestUtil.performDPLTest(
            "index=* | eval a = mvappend(\"1\",\"3\",\"3\",\"a\") |rangemap field=a lo=1-2 hi=3-4",
            testFile,
            ds -> {
                List<Row> result = ds.select("range").distinct().collectAsList();
                assertEquals(1, result.size());
                List<String> resultList = result.get(0).getList(0);
                assertEquals(2, resultList.size());
                List<String> expected = Arrays.asList("lo", "hi");

                for (String res : resultList) {
                    if (!expected.contains(res)) {
                        fail("Expected values did not contain result value: " + res);
                    }
                }
            }
        );
    }
}


