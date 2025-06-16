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
public class SearchTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchTransformationTest.class);
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
    public void searchTest_FieldComparison() {
        String query = "index=index_A | search sourcetype!=stream2";
        String testFile = "src/test/resources/joinTransformationTest_data*.jsonl"; // * to make the path into a directory path

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expectedValues = Arrays.asList("stream1", "stream1", "stream1", "stream1", "stream1");
            Assertions
                    .assertEquals(
                            expectedValues, listOfResult, "Batch consumer dataset did not contain the expected values !"
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchTest_Boolean() {
        String query = "index=index_A | search sourcetype=stream1 AND (id = 1 OR id = 3)";
        String testFile = "src/test/resources/joinTransformationTest_data*.jsonl"; // * to make the path into a directory path

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expectedValues = Arrays.asList("stream1", "stream1");
            Assertions
                    .assertEquals(
                            expectedValues, listOfResult, "Batch consumer dataset did not contain the expected values !"
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchTest_TextSearchFromRaw() {
        String query = "index=index_A | search \"nothing\"";
        String testFile = "src/test/resources/joinTransformationTest_data*.jsonl"; // * to make the path into a directory path

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expectedValues = Collections.emptyList();
            Assertions
                    .assertEquals(
                            expectedValues, listOfResult, "Batch consumer dataset did not contain the expected values !"
                    );
        });
    }

    // Tests compareStatement after spath (spath makes all data into String)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchAfterSpath_ComparisonTest() {
        String query = "index=index_A | spath path= json | search json > 40";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> json = ds
                    .select("json")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = new ArrayList<>(Arrays.asList("50", "60", "70", "80", "90", "100"));

            Assertions.assertEquals(expected, json);
        });
    }

    // Tests compareStatement after spath (spath makes all data into String)
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchAfterSpath_ComparisonTest2() {
        String query = "index=index_A | spath path= json | search json <= 40";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> json = ds
                    .select("json")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = new ArrayList<>(Arrays.asList("7", "8", "9", "40"));

            Assertions.assertEquals(expected, json);
        });
    }

    // Tests search with equals
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonEqTest() {
        String query = "index=index_A | search sourcetype = stream1";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> sourcetype = ds
                    .select("sourcetype")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays.asList("stream1", "stream1", "stream1", "stream1", "stream1");

            Assertions.assertEquals(expected, sourcetype);
        });
    }

    // Tests search with equals and wildcard
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonEqWildcardTest() {
        String query = "index=index_A | search sourcetype = stream*";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> sourcetype = ds
                    .select("sourcetype")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays
                    .asList(
                            "stream1", "stream2", "stream1", "stream2", "stream1", "stream2", "stream1", "stream2",
                            "stream1", "stream2"
                    );

            Assertions.assertEquals(expected, sourcetype);
        });
    }

    // Test search with not equals
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonNeqTest() {
        String query = "index=index_A | search id != 10";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> id = ds
                    .select("id")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");

            Assertions.assertEquals(expected, id);
        });
    }

    // Tests search with greater than
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonGtTest() {
        String query = "index=index_A | search id > 9";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> id = ds
                    .select("id")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Collections.singletonList("10");

            Assertions.assertEquals(expected, id);
        });
    }

    // Tests search with greater than or equal to
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonGteTest() {
        String query = "index=index_A | search id >= 9";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> id = ds
                    .select("id")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays.asList("9", "10");

            Assertions.assertEquals(expected, id);
        });
    }

    // Tests search with less than
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonLtTest() {
        String query = "index=index_A | search id < 10";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> id = ds
                    .select("id")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9");

            Assertions.assertEquals(expected, id);
        });
    }

    // Tests search with less than or equal to
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonLteTest() {
        String query = "index=index_A | search id <= 10";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> id = ds
                    .select("id")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

            Assertions.assertEquals(expected, id);
        });
    }

    // Tests search compare with a string and a number. Should be a lexicographical comparison
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchComparisonMixedInputTest() {
        String query = "index=index_A | search \"source\" < 2";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.jsonl";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            List<String> json = ds
                    .select("source")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expected = Arrays
                    .asList(
                            "127.0.0.0", "127.1.1.1", "127.2.2.2", "127.3.3.3", "127.4.4.4", "127.5.5.5", "127.6.6.6",
                            "127.7.7.7", "127.8.8.8", "127.9.9.9"
                    );

            Assertions.assertEquals(expected, json);
        });
    }
}
