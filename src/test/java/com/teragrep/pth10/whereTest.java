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
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class whereTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(whereTest.class);

    private final String testFile = "src/test/resources/regexTransformationTest_data*.jsonl"; // * to make the path into a directory path
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
    void whereWithEqualsTest() {
        String query = "index = index_A | where sourcetype = \"stream2\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> resultList = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals(Arrays.asList("stream2", "stream2", "stream2", "stream2", "stream2"), resultList); // correct column contents
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void whereWithOrOperatorTest() {
        String query = "index = index_A  | where sourcetype = \"stream1\" OR offset = 10";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> offsetList = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> sourcetypeList = ds
                    .select("sourcetype")
                    .orderBy("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(6, ds.count());
            Assertions.assertEquals(Arrays.asList("1", "3", "5", "7", "9", "10"), offsetList); // correct column contents
            Assertions
                    .assertEquals(Arrays.asList("stream1", "stream1", "stream1", "stream1", "stream1", "stream2"), sourcetypeList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void whereWithAndOperatorTest() {
        String query = "index = index_A  | where sourcetype = \"stream2\" AND offset < 5";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> offsetList = ds
                    .select("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> sourcetypeList = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(2, ds.count());
            Assertions.assertEquals(Arrays.asList("2", "4"), offsetList); // correct column contents
            Assertions.assertEquals(Arrays.asList("stream2", "stream2"), sourcetypeList);

        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void whereWithMultipleLogicalOperatorsTest() {
        String query = "index = index_A  | where (offset > 0 AND offset < 4) OR (offset >= 8 AND offset <= 10) AND sourcetype != \"stream2\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> offsetList = ds
                    .select("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> sourcetypeList = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(3, ds.count());
            Assertions.assertEquals(Arrays.asList("1", "3", "9"), offsetList); // correct column contents
            Assertions.assertEquals(Arrays.asList("stream1", "stream1", "stream1"), sourcetypeList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void whereWithLikeTest() {
        String query = "index = index_A  | where like(sourcetype, \"stream2\")";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> sourcetypeList = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(5, ds.count());
            Assertions
                    .assertEquals(Arrays.asList("stream2", "stream2", "stream2", "stream2", "stream2"), sourcetypeList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void whereWithNotLikeTest() {
        String query = "index = index_A  | where NOT like(sourcetype, \"stream1\")";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> sourcetypeList = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(5, ds.count());
            Assertions
                    .assertEquals(Arrays.asList("stream2", "stream2", "stream2", "stream2", "stream2"), sourcetypeList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void whereWithLikeAndWildcardTest() {
        String query = "index = index_A  | where like(source, \"127.7%\")";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            List<String> sourceList = ds
                    .select("source")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, ds.count());
            Assertions.assertEquals(Arrays.asList("127.7.7.7"), sourceList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void whereTestIntegerColumnLessThan() {
        streamingTestUtil.performDPLTest("index=index_A | where offset < 3", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );

            Assertions.assertEquals(2, ds.collectAsList().size());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void whereTestIntegerColumnGreaterThanAfterChart() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A " + "| chart avg(offset) as aoffset" + "| chart values(aoffset) as voffset"
                                + "| chart sum(voffset) as soffset" + "| where soffset > 3",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "soffset",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    )
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            Assertions.assertEquals(1, ds.collectAsList().size());
                        }
                );
    }
}
