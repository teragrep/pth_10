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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StrcatTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrcatTransformationTest.class);
    private final String testFile = "src/test/resources/strcatTransformationTest_data*.jsonl"; // * to make the path into a directory path
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

    // --- Catalyst emit mode tests ---

    // strcat without allRequired parameter provided (defaults to allRequired=f)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void strcatTransformTest() {
        String q = "index=index_A | strcat _raw sourcetype \"literal\" dest";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("dest", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            // check if result contains the column that was created for strcat result
            Assertions.assertEquals(res.schema(), expectedSchema);

            // List of expected values for the strcat destination field
            List<String> expectedValues = new ArrayList<>(
                    Arrays
                            .asList(
                                    "raw 01A:X:0literal", "raw 02A:X:0literal", "raw 03A:Y:0literal",
                                    "raw 04A:Y:0literal", "raw 05A:Y:0literal"
                            )
            );

            // Destination field from result dataset<row>
            List<String> destAsList = res
                    .select("dest")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Collections.sort(expectedValues);
            Collections.sort(destAsList);

            // assert dest field contents as equals with expected contents
            Assertions.assertEquals(expectedValues, destAsList);
        });
    }

    // strcat with allRequired=True
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void strcatTransformAllRequiredTrueTest() {
        String q = "index=index_A | strcat allrequired=t _raw \"literal\" sourcetype dest";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("dest", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            // check if result contains the column that was created for strcat result
            Assertions.assertEquals(res.schema(), expectedSchema);

            // List of expected values for the strcat destination field
            List<String> expectedValues = new ArrayList<>(
                    Arrays
                            .asList(
                                    "raw 01literalA:X:0", "raw 02literalA:X:0", "raw 03literalA:Y:0",
                                    "raw 04literalA:Y:0", "raw 05literalA:Y:0"
                            )
            );

            // Destination field from result dataset<row>
            List<String> destAsList = res
                    .select("dest")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Collections.sort(expectedValues);
            Collections.sort(destAsList);

            // assert dest field contents as equals with expected contents
            Assertions.assertEquals(expectedValues, destAsList);
        });
    }

    // strcat with allRequired=False
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void strcatTransformAllRequiredFalseTest() {
        String q = "index=index_A | strcat allrequired=f _raw sourcetype \"hello world\" dest";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("dest", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            // check if result contains the column that was created for strcat result
            Assertions.assertEquals(res.schema(), expectedSchema);

            // List of expected values for the strcat destination field
            List<String> expectedValues = new ArrayList<>(
                    Arrays
                            .asList(
                                    "raw 01A:X:0hello world", "raw 02A:X:0hello world", "raw 03A:Y:0hello world",
                                    "raw 04A:Y:0hello world", "raw 05A:Y:0hello world"
                            )
            );

            // Destination field from result dataset<row>
            List<String> destAsList = res
                    .select("dest")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Collections.sort(expectedValues);
            Collections.sort(destAsList);

            // assert dest field contents as equals with expected contents
            Assertions.assertEquals(expectedValues, destAsList);
        });

    }

    // strcat with allRequired=True AND missing(incorrect) field
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void strcatTransformAllRequiredTrueWithMissingFieldTest() {
        String q = "index=index_A | strcat allrequired=t _raw sourcetype NOT_A_REAL_FIELD \"literal\" dest";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("dest", DataTypes.NullType, true, new MetadataBuilder().build())
            });
            // check if result contains the column that was created for strcat result
            Assertions.assertEquals(res.schema(), expectedSchema);

            // List of expected values for the strcat destination field
            List<String> expectedValues = new ArrayList<>(Arrays.asList(null, null, null, null, null));

            // Destination field from result dataset<row>
            List<String> destAsList = res
                    .select("dest")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            // assert dest field contents as equals with expected contents
            Assertions.assertEquals(expectedValues, destAsList);
        });
    }

    // strcat with allRequired=False AND missing(incorrect) field
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void strcatTransformAllRequiredFalseWithMissingFieldTest() {
        String q = "index=index_A | strcat allrequired=f _raw sourcetype \"literal\" NOT_A_REAL_FIELD dest";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("dest", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            // check if result contains the column that was created for strcat result
            Assertions.assertEquals(res.schema(), expectedSchema);

            // List of expected values for the strcat destination field
            List<String> expectedValues = new ArrayList<>(
                    Arrays
                            .asList(
                                    "raw 01A:X:0literal", "raw 02A:X:0literal", "raw 03A:Y:0literal",
                                    "raw 04A:Y:0literal", "raw 05A:Y:0literal"
                            )
            );

            // Destination field from result dataset<row>
            List<String> destAsList = res
                    .select("dest")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Collections.sort(expectedValues);
            Collections.sort(destAsList);

            // assert dest field contents as equals with expected contents
            Assertions.assertEquals(expectedValues, destAsList);
        });
    }

    // strcat with allRequired=False AND three fields and two literals
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void strcatTransformWithMoreThanTwoFields() {
        String q = "index=index_A | strcat allrequired=f _raw \",\" sourcetype \",\" index dest";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("dest", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            // check if result contains the column that was created for strcat result
            Assertions.assertEquals(res.schema(), expectedSchema);

            // List of expected values for the strcat destination field
            List<String> expectedValues = new ArrayList<>(
                    Arrays
                            .asList(
                                    "raw 01,A:X:0,index_A", "raw 02,A:X:0,index_A", "raw 03,A:Y:0,index_A",
                                    "raw 04,A:Y:0,index_A", "raw 05,A:Y:0,index_A"
                            )
            );

            // Destination field from result dataset<row>
            List<String> destAsList = res
                    .select("dest")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Collections.sort(expectedValues);
            Collections.sort(destAsList);

            // assert dest field contents as equals with expected contents
            Assertions.assertEquals(expectedValues, destAsList);
        });
    }
}
