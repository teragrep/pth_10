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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for spath command Uses streaming datasets
 *
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SpathTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpathTransformationTest.class);

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

    // * to make the path into a directory path
    final String JSON_DATA_1 = "src/test/resources/spath/spathTransformationTest_json1*.jsonl";

    final String JSON_DATA_NESTED = "src/test/resources/spath/spathTransformationTest_json_nested*.jsonl";
    final String XML_DATA_1 = "src/test/resources/spath/spathTransformationTest_xml1*.jsonl";
    final String XML_DATA_2 = "src/test/resources/spath/spathTransformationTest_xml2*.jsonl";
    final String INVALID_DATA = "src/test/resources/spath/spathTransformationTest_invalid*.jsonl";

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestXml() {
        streamingTestUtil.performDPLTest("index=index_A | spath input=_raw path=\"main.sub.item\"", XML_DATA_1, ds -> {
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
                    new StructField("main.sub.item", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );

            String result = ds
                    .select("`main.sub.item`")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("Hello world", result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestXmlWithOutput() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | spath input=_raw output=OUT path=\"main.sub.item\"", XML_DATA_1, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("OUT", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            String result = ds
                                    .select("OUT")
                                    .dropDuplicates()
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList())
                                    .get(0);
                            Assertions.assertEquals("Hello world", result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestXmlWithOutput_MultipleTagsOnSameLevel() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | spath input=_raw output=OUT path=\"main.sub[1].item\"", XML_DATA_2, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()), new StructField("OUT", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );

                            String result = ds
                                    .select("OUT")
                                    .dropDuplicates()
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList())
                                    .get(0);
                            Assertions.assertEquals("Hello", result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestJson() {
        streamingTestUtil.performDPLTest("index=index_A | spath input=_raw path=json", JSON_DATA_1, ds -> {
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
                    new StructField("json", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            String result = ds
                    .select("json")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("debugo", result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestJsonWithOutput() {
        streamingTestUtil.performDPLTest("index=index_A | spath input=_raw output=OUT path=json", JSON_DATA_1, ds -> {
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
                    new StructField("OUT", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            String result = ds
                    .select("OUT")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("debugo", result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestJsonNoPath() {
        streamingTestUtil.performDPLTest("index=index_A | spath input=_raw", JSON_DATA_1, ds -> {
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
                    new StructField("json", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("lil", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );

            List<Row> result = ds.select("json", "lil").dropDuplicates().collectAsList();
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals("debugo", result.get(0).getAs("json"));
            Assertions.assertEquals("xml", result.get(0).getAs("lil"));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestJsonInvalidInput() {
        RuntimeException sqe = this.streamingTestUtil
                .performThrowingDPLTest(
                        RuntimeException.class, "index=index_A | eval a = \"12.34\" | spath input=a", JSON_DATA_1,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());
                        }
                );

        String causeStr = this.streamingTestUtil.getInternalCauseString(sqe.getCause(), IllegalStateException.class);
        Assertions.assertEquals("Caused by: java.lang.IllegalStateException: Not a JSON Object: 12.34", causeStr);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    // auto extract with xml and makeresults in front
    public void spathTestXmlWithMakeResultsAndAutoExtraction() {
        streamingTestUtil
                .performDPLTest(
                        "| makeresults count=10 | eval a = \"<main><sub>Hello</sub><sub>World</sub></main>\" | spath input=a",
                        XML_DATA_2, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("a", DataTypes.StringType, false, new MetadataBuilder().build()),
                                    new StructField(
                                            "main.sub",
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
                            String result = ds
                                    .select("`main.sub`")
                                    .dropDuplicates()
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList())
                                    .get(0);
                            Assertions.assertEquals("Hello\nWorld", result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestAutoExtractionXml() {
        streamingTestUtil.performDPLTest("index=index_A | spath", XML_DATA_2, ds -> {
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
                    new StructField("main.sub.item", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            String result = ds
                    .select("`main.sub.item`")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("Hello\nHello2\n1", result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestAutoExtractionJson() {
        streamingTestUtil.performDPLTest("index=index_A | spath", JSON_DATA_1, ds -> {
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
                    new StructField("json", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("lil", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            String result = ds
                    .select("lil")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("xml", result);
            String result2 = ds
                    .select("json")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("debugo", result2);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTestNestedJsonData() {
        streamingTestUtil.performDPLTest("index=index_A | spath output=log path=.log", JSON_DATA_NESTED, ds -> {
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
                    new StructField("log", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            String result = ds
                    .select("log")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions
                    .assertEquals(
                            "{\"auditID\":\"x\",\"requestURI\":\"/path\",\"user\":{\"name\":\"sys\",\"group\":[\"admins\",\"nosucherror\"]},\"method\":\"GET\",\"remoteAddr\":\"127.0.0.123:1025\",\"requestTimestamp\":\"2022-12-14T11:56:13Z\",\"responseTimestamp\":\"2022-12-14T11:56:13Z\",\"responseCode\":503,\"requestHeader\":{\"Accept-Encoding\":[\"gzip\"],\"User-Agent\":[\"Go-http-client/2.0\"]}}",
                            result
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSpathEvaledJsonData() {
        String query = "index=index_A | eval catworld = \"{\\\"kissa\\\" : \\\"fluff\\\"}\" | spath input=catworld output=cat path=kissa";
        streamingTestUtil.performDPLTest(query, JSON_DATA_1, ds -> {
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
                    new StructField("catworld", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("cat", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Dataset<Row> res = ds.select("cat").orderBy("offset").distinct();
            List<Row> catList = res.collectAsList();

            Assertions.assertEquals(1, catList.size());
            Assertions.assertEquals(expectedSchema, ds.schema());
            Assertions.assertEquals("fluff", catList.get(0).getString(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTest_invalidInput() {
        streamingTestUtil.performDPLTest("index=index_A | spath path=abc", INVALID_DATA, ds -> {
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
                    new StructField("abc", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            Object result = ds
                    .select("abc")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0))
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertNull(result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTest_invalidInputAutoExtraction() {
        streamingTestUtil.performDPLTest("index=index_A | spath", INVALID_DATA, ds -> {
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
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTest_invalidInputManualExtraction() {
        streamingTestUtil
                .performDPLTest("index=index_A | spath path=\"randomPathThatDoesNotExist\"", INVALID_DATA, ds -> {
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
                            new StructField(
                                    "randomPathThatDoesNotExist",
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
                    // all should be nulls, so distinct() returns 1 row
                    List<Row> rows = ds.select("randomPathThatDoesNotExist").distinct().collectAsList();
                    Assertions.assertEquals(1, rows.size());
                    // make sure it is null
                    Assertions.assertEquals(streamingTestUtil.getCtx().nullValue.value(), rows.get(0).get(0));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void spathTest_ImplicitPath() {
        streamingTestUtil.performDPLTest("index=index_A | spath json", JSON_DATA_1, ds -> {
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
                    new StructField("json", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("lil", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions
                    .assertEquals(
                            expectedSchema, ds.schema(),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );
            String json = ds
                    .select("json")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            String lil = ds
                    .select("lil")
                    .dropDuplicates()
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList())
                    .get(0);
            Assertions.assertEquals("debugo", json);
            Assertions.assertEquals("xml", lil);
        });
    }
}
