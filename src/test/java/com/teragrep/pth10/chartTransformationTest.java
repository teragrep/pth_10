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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class chartTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(chartTransformationTest.class);

    String testFile = "src/test/resources/xmlWalkerTestDataStreaming/xmlWalkerTestDataStreaming*";
    StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
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

    @Test
    public void testChartCountAs() {
        String query = "index = index_A | chart count(_raw) as count";

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, res.schema()); // check that the schema is correct

            List<String> resultList = res
                    .select("count")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, resultList.size()); // only one row of data
            Assertions.assertEquals("5", resultList.get(0)); // the one row has the value 5
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testChartCountWithAsAndSplitBy() {
        String q = "( index = index_A OR index = index_B ) _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count by offset";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(expectedSchema, res.schema()); // At least schema is correct

            // 3 first rows are earlier than where _index_earliest is set to
            List<String> expectedValues = new ArrayList<>();
            for (int i = 4; i < 11; i++) {
                expectedValues.add(i + ",1");
            }

            List<String> resultList = res
                    .collectAsList()
                    .stream()
                    .map(r -> r.mkString(","))
                    .collect(Collectors.toList());

            // sort the lists, as the order of rows doesn't matter with this aggregation
            Collections.sort(expectedValues);
            Collections.sort(resultList);

            Assertions.assertEquals(expectedValues, resultList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testChartCountAsFieldIsUsable() {
        String q = "index = index_A _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count by offset | where count > 0";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {

            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(expectedSchema, res.schema()); // At least schema is correct

            List<String> expectedValues = new ArrayList<>();
            // Only first 5 rows have index: index_A
            // and only the latter 2 have _time after index_earliest
            expectedValues.add(4 + ",1");
            expectedValues.add(5 + ",1");

            List<String> resultList = res
                    .collectAsList()
                    .stream()
                    .map(r -> r.mkString(","))
                    .collect(Collectors.toList());

            // sort the lists, as the order of rows doesn't matter with this aggregation
            Collections.sort(expectedValues);
            Collections.sort(resultList);

            Assertions.assertEquals(expectedValues, resultList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testChartCount() {
        String q = "index = index_B _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw)";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("count(_raw)", DataTypes.LongType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(expectedSchema, res.schema()); // At least schema is correct

            List<String> expectedValues = new ArrayList<>();
            expectedValues.add("5"); // only last 5 rows have index: index_B

            List<String> resultList = res
                    .collectAsList()
                    .stream()
                    .map(r -> r.mkString(","))
                    .collect(Collectors.toList());

            // sort the lists, as the order of rows doesn't matter with this aggregation
            Collections.sort(expectedValues);
            Collections.sort(resultList);

            Assertions.assertEquals(expectedValues, resultList);
        });
    }

    // multiple chart aggregations
    // specifically for issue #184
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testChartMultipleAggregations() {
        String q = "index=* | chart count(_raw), min(offset), max(offset) by index";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count(_raw)", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("min(offset)", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("max(offset)", DataTypes.StringType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(expectedSchema, res.schema());

            // assert contents
            List<Row> count = res.select("count(_raw)").collectAsList();
            List<Row> min = res.select("min(offset)").collectAsList();
            List<Row> max = res.select("max(offset)").collectAsList();

            Row cr = count.get(0);
            Row minr = min.get(0);
            Row maxr = max.get(0);

            Row cr2 = count.get(1);
            Row minr2 = min.get(1);
            Row maxr2 = max.get(1);

            Assertions.assertEquals("5", cr.getAs(0).toString());
            Assertions.assertEquals("1", minr.getAs(0).toString());
            Assertions.assertEquals("5", maxr.getAs(0).toString());

            Assertions.assertEquals("5", cr2.getAs(0).toString());
            Assertions.assertEquals("6", minr2.getAs(0).toString());
            Assertions.assertEquals("10", maxr2.getAs(0).toString());
        });
    }

    // Check that is AggregatesUsed returns true
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testAggregateUsed() {
        String q = "index = jla02logger | chart count(_raw)";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
            Assertions.assertTrue(aggregates);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSplittingByTime() {
        String q = "index=* | chart avg(offset) by _time";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("avg(offset)", DataTypes.DoubleType, true, new MetadataBuilder().build()),
            });

            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> time = res.select("_time").collectAsList();
            List<Row> offset = res.select("avg(offset)").collectAsList();

            // assert correct ordering, old to new
            String[] expectedTime = new String[] {
                    "2001-01-01T01:01:01.010+03:00",
                    "2002-02-02T02:02:02.020+03:00",
                    "2003-03-03T03:03:03.030+03:00",
                    "2004-04-04T04:04:04.040+03:00",
                    "2005-05-05T05:05:05.050+03:00",
                    "2006-06-06T06:06:06.060+03:00",
                    "2007-07-07T07:07:07.070+03:00",
                    "2008-08-08T08:08:08.080+03:00",
                    "2009-09-09T09:09:09.090+03:00",
                    "2010-10-10T10:10:10.100+03:00"
            };
            String[] expectedOffset = new String[] {
                    "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0"
            };

            Assertions.assertArrayEquals(expectedTime, time.stream().map(r -> r.getAs(0).toString()).toArray());
            Assertions.assertArrayEquals(expectedOffset, offset.stream().map(r -> r.getAs(0).toString()).toArray());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSplittingByString() {
        String q = "index=* | chart avg(offset) by sourcetype";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("avg(offset)", DataTypes.DoubleType, true, new MetadataBuilder().build()),
            });

            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> sourcetype = res.select("sourcetype").collectAsList();
            List<Row> offset = res.select("avg(offset)").collectAsList();

            // ascending ordering for strings
            String[] expectedSourcetype = new String[] {
                    "A:X:0", "A:Y:0", "B:X:0", "B:Y:0"
            };
            String[] expectedOffset = new String[] {
                    "1.5", "4.0", "7.0", "9.5"
            };

            Assertions
                    .assertArrayEquals(expectedSourcetype, sourcetype.stream().map(r -> r.getAs(0).toString()).toArray());
            Assertions.assertArrayEquals(expectedOffset, offset.stream().map(r -> r.getAs(0).toString()).toArray());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSplittingByNumber() {
        String q = "index=* | chart count(offset) by offset";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("count(offset)", DataTypes.LongType, true, new MetadataBuilder().build()),
            });

            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> offset = res.select("offset").collectAsList();
            List<Row> count = res.select("count(offset)").collectAsList();

            // assert correct ascending ordering
            String[] expectedOffset = new String[] {
                    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
            };
            String[] expectedCount = new String[] {
                    "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"
            };

            Assertions.assertArrayEquals(expectedOffset, offset.stream().map(r -> r.getAs(0).toString()).toArray());
            Assertions.assertArrayEquals(expectedCount, count.stream().map(r -> r.getAs(0).toString()).toArray());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSplittingByNumericalString() {
        String q = "index=* | eval a = offset + 0 | chart count(offset) by a";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("a", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count(offset)", DataTypes.LongType, true, new MetadataBuilder().build()),
            });

            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> a = res.select("a").collectAsList();
            List<Row> count = res.select("count(offset)").collectAsList();

            // assert correct ascending ordering
            String[] expectedA = new String[] {
                    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
            };
            String[] expectedCount = new String[] {
                    "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"
            };

            Assertions.assertArrayEquals(expectedA, a.stream().map(r -> r.getAs(0).toString()).toArray());
            Assertions.assertArrayEquals(expectedCount, count.stream().map(r -> r.getAs(0).toString()).toArray());
        });
    }
}
