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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FormatTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FormatTransformationTest.class);
    private final String testFile = "src/test/resources/strcatTransformationTest_data*.json"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
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


    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void formatTransformationTest0() {
        String q = "index=index_A | format ";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            // Check if result contains the column that was created for format result
            assertTrue(Arrays.toString(res.columns()).contains("search"));

            // List of expected values for the format destination field
            List<String> expectedValues = Collections.singletonList(
                    "( " +
                            "( " +
                                "_time=\"2023-09-06 11:22:31.0\" " +
                                "AND " +
                                "id=\"1\" " +
                                "AND " +
                                "_raw=\"raw 01\" " +
                                "AND " +
                                "index=\"index_A\" " +
                                "AND " +
                                "sourcetype=\"A:X:0\" " +
                                "AND " +
                                "host=\"host\" " +
                                "AND " +
                                "source=\"input\" " +
                                "AND " +
                                "partition=\"0\" " +
                                "AND offset=\"1\"" +
                            " ) " +
                            "OR " +
                            "( " +
                                "_time=\"2023-09-06 12:22:31.0\" " +
                                "AND " +
                                "id=\"2\" " +
                                "AND " +
                                "_raw=\"raw 02\" " +
                                "AND " +
                                "index=\"index_A\" " +
                                "AND " +
                                "sourcetype=\"A:X:0\" " +
                                "AND " +
                                "host=\"host\" " +
                                "AND " +
                                "source=\"input\" " +
                                "AND " +
                                "partition=\"0\" " +
                                "AND " +
                                "offset=\"2\" " +
                            ") " +
                            "OR " +
                            "( " +
                                "_time=\"2023-09-06 13:22:31.0\" " +
                                "AND id=\"3\" " +
                                "AND " +
                                "_raw=\"raw 03\" " +
                                "AND " +
                                "index=\"index_A\" " +
                                "AND " +
                                "sourcetype=\"A:Y:0\" " +
                                "AND " +
                                "host=\"host\" " +
                                "AND " +
                                "source=\"input\" " +
                                "AND " +
                                "partition=\"0\" " +
                                "AND " +
                                "offset=\"3\" " +
                            ") " +
                            "OR " +
                            "( " +
                                "_time=\"2023-09-06 14:22:31.0\" " +
                                "AND " +
                                "id=\"4\" " +
                                "AND " +
                                "_raw=\"raw 04\" " +
                                "AND " +
                                "index=\"index_A\" " +
                                "AND " +
                                "sourcetype=\"A:Y:0\" " +
                                "AND " +
                                "host=\"host\" " +
                                "AND " +
                                "source=\"input\" " +
                                "AND " +
                                "partition=\"0\" " +
                                "AND " +
                                "offset=\"4\" " +
                            ") " +
                            "OR " +
                            "( " +
                                "_time=\"2023-09-06 15:22:31.0\" " +
                                "AND " +
                                "id=\"5\" " +
                                "AND " +
                                "_raw=\"raw 05\" " +
                                "AND " +
                                "index=\"index_A\" " +
                                "AND " +
                                "sourcetype=\"A:Y:0\" " +
                                "AND " +
                                "host=\"host\" " +
                                "AND " +
                                "source=\"input\" " +
                                "AND " +
                                "partition=\"0\" " +
                                "AND " +
                                "offset=\"5\" " +
                            ") " +
                    ")"
            );

            // Destination field from result dataset
            List<String> searchAsList = res
                    .select("search")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            // Assert search field contents as equals with expected contents
            assertEquals(expectedValues, searchAsList);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void formatTransformationTest1() {
        String q = "index=index_A | eval a=mvappend(\"1\", \"2\") | format maxresults=1 ";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            // Check if result contains the column that was created for format result
            assertTrue(Arrays.toString(res.columns()).contains("search"));

            // List of expected values for the format destination field
            List<String> expectedValues = Collections.singletonList(
                    "( " +
                            "( " +
                                "_time=\"2023-09-06 11:22:31.0\" " +
                                "AND " +
                                "id=\"1\" " +
                                "AND " +
                                "_raw=\"raw 01\" " +
                                "AND " +
                                "index=\"index_A\" " +
                                "AND " +
                                "sourcetype=\"A:X:0\" " +
                                "AND " +
                                "host=\"host\" " +
                                "AND " +
                                "source=\"input\" " +
                                "AND " +
                                "partition=\"0\" " +
                                "AND " +
                                "offset=\"1\" " +
                                "AND " +
                                "( " +
                                    "a=\"1\" " +
                                    "OR " +
                                    "a=\"2\" " +
                                ") " +
                            ") " +
                    ")"
            );

            // Destination field from result dataset
            List<String> searchAsList = res
                    .select("search")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            // Assert search field contents as equals with expected contents
            assertEquals(expectedValues, searchAsList);
        });
    }


    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void formatTransformationTest2() {
        String q = "index=index_A | format maxresults=2 \"ROWPRE\" \"COLPRE\" \"COLSEP\" \"COLSUF\"\"ROWSEP\" \"ROWSUF\" ";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            // Check if result contains the column that was created for format result
            assertTrue(Arrays.toString(res.columns()).contains("search"));

            // List of expected values for the format destination field
            List<String> expectedValues = Collections.singletonList(
                    "ROWPRE " +
                            "COLPRE " +
                            "_time=\"2023-09-06 11:22:31.0\" " +
                            "COLSEP " +
                            "id=\"1\" " +
                            "COLSEP " +
                            "_raw=\"raw 01\" " +
                            "COLSEP " +
                            "index=\"index_A\" " +
                            "COLSEP " +
                            "sourcetype=\"A:X:0\" " +
                            "COLSEP " +
                            "host=\"host\" " +
                            "COLSEP " +
                            "source=\"input\" " +
                            "COLSEP " +
                            "partition=\"0\" " +
                            "COLSEP " +
                            "offset=\"1\"" +
                            " COLSUF " +
                            "ROWSEP " +
                            "COLPRE " +
                            "_time=\"2023-09-06 12:22:31.0\" " +
                            "COLSEP " +
                            "id=\"2\" " +
                            "COLSEP " +
                            "_raw=\"raw 02\" " +
                            "COLSEP " +
                            "index=\"index_A\" " +
                            "COLSEP " +
                            "sourcetype=\"A:X:0\" " +
                            "COLSEP " +
                            "host=\"host\" " +
                            "COLSEP " +
                            "source=\"input\" " +
                            "COLSEP " +
                            "partition=\"0\" " +
                            "COLSEP " +
                            "offset=\"2\" " +
                            "COLSUF " +
                            "ROWSUF"
            );

            // Destination field from result dataset
            List<String> searchAsList = res
                    .select("search")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            // Assert search field contents as equals with expected contents
            assertEquals(expectedValues, searchAsList);
        });
    }
}
