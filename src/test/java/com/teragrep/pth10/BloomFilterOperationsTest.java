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

import com.teragrep.pth10.steps.teragrep.TeragrepBloomStep;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BloomFilterOperationsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterOperationsTest.class);
    private final String testFile = "src/test/resources/xmlWalkerTestDataStreaming/bloomTeragrepStep_data*.jsonl";
    private final String aggregateFile = "src/test/resources/xmlWalkerTestDataStreaming/bloomTeragrepStep_aggregation_data*.jsonl";

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
        streamingTestUtil = new StreamingTestUtil(this.testSchema);
        streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void estimateTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-01-01T00:00:00Z latest=2023-01-01T00:00:00Z | teragrep exec tokenizer | teragrep exec bloom estimate",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField(
                                            "estimate(tokens)",
                                            DataTypes.LongType,
                                            false,
                                            new MetadataBuilder().build()
                                    )
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );
                            List<Integer> results = ds
                                    .select("estimate(tokens)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> Integer.parseInt(r.get(0).toString()))
                                    .collect(Collectors.toList());

                            Assertions.assertEquals(results.get(0), 1);
                            Assertions.assertTrue(results.get(1) > 1);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testEstimateOnEmptyArray() {
        streamingTestUtil
                .performDPLTest(
                        // index_Empty _raw = "" so tokenizer step will produce an empty array
                        "index=index_Empty earliest=2020-01-01T00:00:00Z latest=2023-01-01T00:00:00Z | teragrep exec tokenizer | teragrep exec bloom estimate",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField(
                                            "estimate(tokens)",
                                            DataTypes.LongType,
                                            false,
                                            new MetadataBuilder().build()
                                    )
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );
                            List<Integer> results = ds
                                    .select("estimate(tokens)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> Integer.parseInt(r.get(0).toString()))
                                    .collect(Collectors.toList());

                            // assert that a row is produced and not an empty dataframe
                            Assertions.assertEquals(1, results.size());
                            // assert that estimate is 0 and not empty or null
                            Assertions.assertEquals(0, results.get(0));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testAggregateWithTokenizerFormatBytes() {
        final String id = UUID.randomUUID().toString();
        final Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.db.fields", "[ {expected: 100, fpp: 0.01}]");
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-01-01T00:00:00Z latest=2023-01-01T00:00:00Z "
                                + "| teragrep exec tokenizer format bytes "
                                + "| teragrep exec hdfs save overwrite=true /tmp/pth_10_hdfs/aggregatorTokenBytes/"
                                + id,
                        aggregateFile, ds -> {
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil
                .performDPLTest(
                        "| teragrep exec hdfs load /tmp/pth_10_hdfs/aggregatorTokenBytes/" + id + " "
                                + "| teragrep exec bloom estimate "
                                + "| teragrep exec hdfs save overwrite=true /tmp/pth_10_hdfs/aggregatorEstimate/" + id,
                        aggregateFile, ds -> {
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil
                .performDPLTest(
                        "| teragrep exec hdfs load /tmp/pth_10_hdfs/aggregatorTokenBytes/" + id
                                + "| join type=inner max=0 partition [| teragrep exec hdfs load /tmp/pth_10_hdfs/aggregatorEstimate/"
                                + id,
                        aggregateFile, ds -> {
                            Config config = ConfigFactory.parseProperties(properties);
                            TeragrepBloomStep step = new TeragrepBloomStep(
                                    config,
                                    TeragrepBloomStep.BloomMode.AGGREGATE,
                                    "tokens",
                                    "bloomfilter",
                                    "R_estimate(tokens)"
                            );
                            ds = step.aggregate(ds);
                            List<byte[]> listOfResult = ds
                                    .select("bloomfilter")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> (byte[]) r.get(0))
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(2, listOfResult.size());
                            BloomFilter filter1 = Assertions
                                    .assertDoesNotThrow(
                                            () -> BloomFilter.readFrom(new ByteArrayInputStream(listOfResult.get(0)))
                                    );
                            // should find all tokens
                            Assertions.assertTrue(filter1.mightContain("bc113100-b859-4041-b272-88b849f6d6db"));
                            Assertions.assertTrue(filter1.mightContain("userid"));
                            Assertions.assertTrue(filter1.mightContain("userid="));
                            Assertions.assertTrue(filter1.mightContain("userid=bc113100-b859-4041-b272-88b849f6d6db"));

                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testAggregateUsingRegexExtract() {
        final Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.db.fields", "[ {expected: 100, fpp: 0.01}]");
        final String id = UUID.randomUUID().toString();
        final String regex = "\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}";
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-01-01T00:00:00Z latest=2023-01-01T00:00:00Z "
                                + "| teragrep exec regexextract regex " + regex
                                + "| teragrep exec hdfs save overwrite=true /tmp/pth_10_hdfs/aggregatorTokenBytes/"
                                + id,
                        aggregateFile, ds -> {
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil
                .performDPLTest(
                        "| teragrep exec hdfs load /tmp/pth_10_hdfs/aggregatorTokenBytes/" + id + " "
                                + "| teragrep exec bloom estimate "
                                + "| teragrep exec hdfs save overwrite=true /tmp/pth_10_hdfs/aggregatorEstimate/" + id,
                        aggregateFile, ds -> {
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil
                .performDPLTest(
                        "| teragrep exec hdfs load /tmp/pth_10_hdfs/aggregatorTokenBytes/" + id
                                + "| join type=inner max=0 partition [| teragrep exec hdfs load /tmp/pth_10_hdfs/aggregatorEstimate/"
                                + id,
                        aggregateFile, ds -> {
                            Config config = ConfigFactory.parseProperties(properties);
                            TeragrepBloomStep step = new TeragrepBloomStep(
                                    config,
                                    TeragrepBloomStep.BloomMode.AGGREGATE,
                                    "tokens",
                                    "bloomfilter",
                                    "R_estimate(tokens)"
                            );
                            ds = step.aggregate(ds);
                            List<byte[]> listOfResult = ds
                                    .select("bloomfilter")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> (byte[]) r.get(0))
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(2, listOfResult.size());
                            BloomFilter filter1 = Assertions
                                    .assertDoesNotThrow(
                                            () -> BloomFilter.readFrom(new ByteArrayInputStream(listOfResult.get(0)))
                                    );
                            BloomFilter filter2 = Assertions
                                    .assertDoesNotThrow(
                                            () -> BloomFilter.readFrom(new ByteArrayInputStream(listOfResult.get(1)))
                                    );
                            // should only find regex match tokens
                            Assertions.assertTrue(filter1.mightContain("bc113100-b859-4041-b272-88b849f6d6db"));
                            Assertions.assertFalse(filter1.mightContain("962e5f8c-fffe-4ea6-a164-b39e0ce4ceb4"));
                            Assertions.assertFalse(filter1.mightContain("userid"));
                            Assertions.assertFalse(filter1.mightContain("userid="));
                            Assertions.assertFalse(filter1.mightContain("uuid"));
                            Assertions.assertFalse(filter1.mightContain("uuid="));
                            Assertions.assertFalse(filter1.mightContain("uuid=962e5f8c-fffe-4ea6-a164-b39e0ce4ceb4"));
                            // check that filter 2 found both UUIDs
                            Assertions.assertTrue(filter2.mightContain("bc113100-b859-4041-b272-88b849f6d6db"));
                            Assertions.assertTrue(filter2.mightContain("962e5f8c-fffe-4ea6-a164-b39e0ce4ceb4"));
                        }
                );
    }
}
