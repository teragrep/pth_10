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

import java.util.Comparator;
import java.util.List;

/**
 * Tests for the new ProcessingStack implementation Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DedupTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DedupTransformationTest.class);

    // Use this file for  dataset initialization
    private String testFile = "src/test/resources/dedup_test_data*.jsonl"; // * to make the path into a directory path

    private StreamingTestUtil streamingTestUtil;

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

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // basic dedup
    @Disabled(value = "Pending on issue #645 - https://github.com/teragrep/pth_10/issues/645")
    public void dedupTest_NoParams() {
        this.streamingTestUtil.performDPLTest("index=index_A | dedup _raw", this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("_raw", "offset").collectAsList();
            listOfRaw.sort(Comparator.comparingLong(r -> r.getAs("offset")));
            Assertions.assertEquals(2, listOfRaw.size());
            String first = listOfRaw.get(0).get(0).toString();
            String second = listOfRaw.get(1).get(0).toString();

            Assertions.assertEquals("1", first);
            Assertions.assertEquals("2", second);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // consecutive=true
    @Disabled(value = "Test disabled: Consecutive parameter is not implemented")
    public void dedupTest_Consecutive() {
        String query = "index=index_A | dedup _raw consecutive= true";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("_raw", "offset").collectAsList();
            listOfRaw.sort(Comparator.comparingLong(r -> r.getAs("offset")));
            Assertions.assertEquals(10, listOfRaw.size());
            for (int i = 0; i < listOfRaw.size(); i = i + 2) {
                Assertions.assertEquals("1", listOfRaw.get(i).get(0).toString());
                Assertions.assertEquals("2", listOfRaw.get(i + 1).get(0).toString());
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // sort descending as numbers
    @Disabled(value = "Test disabled: sortby parameter is not implemented")
    public void dedupTest_SortNum() {
        String query = "index=index_A | dedup _raw sortby - num(_raw)";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("_raw", "offset").collectAsList();
            Assertions.assertEquals(2, listOfRaw.size());
            Assertions.assertEquals("2", listOfRaw.get(0).get(0).toString());
            Assertions.assertEquals("1", listOfRaw.get(1).get(0).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // keep duplicate events with nulls
    @Disabled(value = "Test disabled: keepevents parameter is not implemented")
    public void dedupTest_KeepEvents() {
        String query = "index=index_A | dedup _raw keepevents= true";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("_raw", "offset").collectAsList();
            listOfRaw.sort(Comparator.comparingLong(r -> r.getAs("offset")));
            Assertions.assertEquals(10, listOfRaw.size());
            Assertions.assertEquals("1", listOfRaw.get(0).get(0));
            Assertions.assertEquals("2", listOfRaw.get(1).get(0));
            int loops = 0;
            for (int i = 2; i < 10; i++) {
                Assertions.assertNull(listOfRaw.get(i).get(0));
                loops++;
            }
            Assertions.assertEquals(8, loops);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // keep null values
    @Disabled(value = "Test disabled: keepevents parameter is not implemented")
    public void dedupTest_KeepEmpty() {
        // first use keepevents=true to make null values in the dataset
        String query = "index=index_A | dedup _raw keepevents= true | dedup _raw keepempty= true";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("_raw", "offset").collectAsList();
            listOfRaw.sort(Comparator.comparingLong(r -> r.getAs("offset")));
            Assertions.assertEquals(3, listOfRaw.size());
            Assertions.assertEquals("1", listOfRaw.get(0).get(0));
            Assertions.assertEquals("2", listOfRaw.get(1).get(0));
            Assertions.assertNull(listOfRaw.get(2).get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // deduplicate based on _raw, sourcetype and partition
    @Disabled(value = "Pending on issue #645 - https://github.com/teragrep/pth_10/issues/645")
    public void dedupTest_MultiColumn() {
        String query = "index=index_A | dedup _raw, sourcetype, partition";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("_raw", "offset").collectAsList();
            listOfRaw.sort(Comparator.comparingLong(r -> r.getAs("offset")));
            Assertions.assertEquals(2, listOfRaw.size());
            Assertions.assertEquals("1", listOfRaw.get(0).get(0));
            Assertions.assertEquals("2", listOfRaw.get(1).get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // deduplicate based on _raw, sourcetype and partition
    @Disabled(value = "Pending on issue #645 - https://github.com/teragrep/pth_10/issues/645")
    public void dedupTest_AggAfter() {
        String query = "index=index_A | dedup _raw | timechart count(_raw)";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("count(_raw)", DataTypes.LongType, true, new MetadataBuilder().build()),

            });
            // Columns should be the same. Order can be different because of .jsonl file readStream might shuffle them
            Assertions.assertEquals(expectedSchema, res.schema());

            List<Row> listOfRaw = res.select("count(_raw)").collectAsList();
            Assertions.assertEquals(1, listOfRaw.size());
            Assertions.assertEquals(2L, listOfRaw.get(0).get(0));
        });
    }
}
