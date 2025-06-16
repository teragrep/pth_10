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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TeragrepTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepTransformationTest.class);

    private final String testFile = "src/test/resources/IplocationTransformationTest_data*.jsonl"; // * to make the path into a directory path
    private String testResourcesPath;
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
        testResourcesPath = this.streamingTestUtil.getTestResourcesPath();
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
    public void tgHdfsSaveLoadTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id
                                + " | regex _raw = \"\" | teragrep exec hdfs load /tmp/pth_10_hdfs/" + id,
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("offset")
                                    .orderBy("offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveLoadCsvTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id + "/ format=CSV"
                                + " | regex _raw = \"\" | teragrep exec hdfs load /tmp/pth_10_hdfs/" + id
                                + " format=CSV",
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("offset")
                                    .orderBy("offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsLoadEmptyAvroDatasetTest() {
        final String id = UUID.randomUUID().toString();
        this.streamingTestUtil
                .performDPLTest(
                        "index=index_B | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id + "/ format=avro"
                                + " | regex _raw = \"\" | teragrep exec hdfs load /tmp/pth_10_hdfs/" + id
                                + " format=avro",
                        this.testFile, ds -> {
                            Assertions.assertTrue(ds.isEmpty());
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsLoadEmptyCsvDatasetTest() {
        final String id = UUID.randomUUID().toString();
        this.streamingTestUtil
                .performDPLTest(
                        "index=index_B | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id + "/ format=CSV"
                                + " | regex _raw = \"\" | teragrep exec hdfs load /tmp/pth_10_hdfs/" + id
                                + " format=CSV",
                        this.testFile, ds -> {
                            Assertions.assertTrue(ds.isEmpty());
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsLoadCustomCsvTest() {
        String dir = testResourcesPath.concat("/csv/hdfs.csv");
        if (!Files.exists(Paths.get(dir))) {
            Assertions.fail("Expected file does not exist: " + dir);
        }

        streamingTestUtil
                .performDPLTest("| teragrep exec hdfs load " + dir + " format=CSV header=FALSE", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("_raw")
                            .orderBy("_raw")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions
                            .assertEquals(
                                    Arrays
                                            .asList(
                                                    "2023-01-01T00:00:00z,stuff,1",
                                                    "2023-01-02T00:00:00z,other stuff,2",
                                                    "2023-01-03T00:00:00z,more other stuff,3",
                                                    "2023-01-04T00:00:00z,even more stuff,4",
                                                    "2023-01-05T00:00:00z,more otherer stuff,5", "_time,_raw,offset"
                                            ),
                                    listOfResult
                            );
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsLoadCustomCsvWithHeaderTest() {
        String dir = testResourcesPath.concat("/csv/hdfs.csv");
        if (!Files.exists(Paths.get(dir))) {
            Assertions.fail("Expected file does not exist: " + dir);
        }

        streamingTestUtil
                .performDPLTest("| teragrep exec hdfs load " + dir + " format=CSV header=TRUE", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("_raw")
                            .orderBy("_raw")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions
                            .assertEquals(
                                    Arrays
                                            .asList(
                                                    "2023-01-01T00:00:00z,stuff,1",
                                                    "2023-01-02T00:00:00z,other stuff,2",
                                                    "2023-01-03T00:00:00z,more other stuff,3",
                                                    "2023-01-04T00:00:00z,even more stuff,4",
                                                    "2023-01-05T00:00:00z,more otherer stuff,5", "_time,_raw,offset"
                                            ),
                                    listOfResult
                            );
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsLoadCustomCsvWithProvidedSchemaTest() {
        String dir = testResourcesPath.concat("/csv/hdfs.csv");
        if (!Files.exists(Paths.get(dir))) {
            Assertions.fail("Expected file does not exist: " + dir);
        }

        streamingTestUtil
                .performDPLTest(
                        "| teragrep exec hdfs load " + dir + " format=CSV header=TRUE schema=\"_time,_raw,offset\"",
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("offset")
                                    .orderBy("offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveLoadWildcardTest() {
        final String id = UUID.randomUUID().toString();

        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id + "/" + id
                                + " | regex _raw = \"\" | teragrep exec hdfs load /tmp/pth_10_hdfs/" + id + "/*",
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("offset")
                                    .orderBy("offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsListTest() {
        streamingTestUtil.performDPLTest("| teragrep exec hdfs list ./src/test/resources/hdfslist/*", testFile, ds -> {
            List<String> listOfResult = ds
                    .select("name")
                    .orderBy("name")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("another_dummy_file.txt", "dummy_file.txt"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsListWildcardTest() {
        streamingTestUtil
                .performDPLTest("| teragrep exec hdfs list ./src/test/resources/hdfslist/*.txt", testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("name")
                            .orderBy("name")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Arrays.asList("another_dummy_file.txt", "dummy_file.txt"), listOfResult);
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsListInvalidPathTest() {
        streamingTestUtil.performDPLTest("| teragrep exec hdfs list /tmp/this/path/does/not/exist", testFile, ds -> {
            List<String> listOfResult = ds
                    .select("name")
                    .orderBy("name")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.emptyList(), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveAfterBloomEstimateTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec tokenizer | teragrep exec bloom estimate | teragrep exec hdfs save /tmp/pth_10_hdfs/"
                                + id,
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("estimate(tokens)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Collections.singletonList("5"), listOfResult);
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil.performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/" + id, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("estimate(tokens)")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("5"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveAfterAggregateTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats avg(offset) AS avg_offset | teragrep exec hdfs save /tmp/pth_10_hdfs/"
                                + id,
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("avg_offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Collections.singletonList("3.0"), listOfResult);
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil.performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/" + id, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("avg_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("3.0"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveAfterTwoAggregationsTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats avg(offset) AS avg_offset | stats values(avg_offset) AS offset_values"
                                + " | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id,
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("offset_values")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Collections.singletonList("3.0"), listOfResult);
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil.performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/" + id, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("offset_values")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("3.0"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveAfterSequentialTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | sort num(offset) | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id, testFile,
                        ds -> {
                            List<String> listOfResult = ds
                                    .select("offset")
                                    .orderBy("offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil.performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/" + id, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest("index=index_A | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id, testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("offset")
                            .orderBy("offset")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                });
        this.streamingTestUtil.setUp();
        streamingTestUtil.performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/" + id, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveOverwriteTest() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec hdfs save /tmp/pth_10_hdfs/" + id + " overwrite=true", testFile,
                        ds -> {
                            List<String> listOfResult = ds
                                    .select("offset")
                                    .orderBy("offset")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil.performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/" + id, testFile, ds -> {
            List<String> listOfResult = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("1", "2", "3", "4", "5"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgGetArchiveSummaryTest() {
        streamingTestUtil.performDPLTest("| teragrep get archive summary index=* offset < 3", testFile, ds -> {
            List<String> listOfResult = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("1", "2"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractTokensTest() {
        // source: "127.4.4.4"
        String regex = "\\d+";
        Pattern pattern = Pattern.compile(regex);
        List<String> expected = Arrays.asList("127", "4", "4", "4");
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex " + regex + " input source output strTokens",
                        testFile, ds -> {
                            List<String> result = ds
                                    .select("strTokens")
                                    .first()
                                    .getList(0)
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(4, result.size());
                            Assertions.assertTrue(result.stream().allMatch(s -> pattern.matcher(s).matches()));
                            Assertions.assertTrue(result.stream().allMatch(s -> s.equals("127") || s.equals("4")));
                            Assertions.assertEquals(expected, result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractTokensTestWithoutQuotes() {
        // source: "127.4.4.4"
        List<String> expected = Arrays.asList("127", "4", "4", "4");
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex \\d+ input source output strTokens", testFile,
                        ds -> {
                            List<String> result = ds
                                    .select("strTokens")
                                    .first()
                                    .getList(0)
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(4, result.size());
                            Assertions.assertTrue(result.stream().allMatch(s -> s.equals("127") || s.equals("4")));
                            Assertions.assertEquals(expected, result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractTokensTestWithDoubleQuotes() {
        // source: "127.4.4.4"
        List<String> expected = Arrays.asList("127", "4", "4", "4");
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex \"\\d+\" input source output strTokens",
                        testFile, ds -> {
                            List<String> result = ds
                                    .select("strTokens")
                                    .first()
                                    .getList(0)
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(4, result.size());
                            Assertions.assertTrue(result.stream().allMatch(s -> s.equals("127") || s.equals("4")));
                            Assertions.assertEquals(expected, result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractTokensTestWithSingleQuotes() {
        // source: "127.4.4.4"
        List<String> expected = Arrays.asList("127", "4", "4", "4");
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex '\\d+' input source output strTokens",
                        testFile, ds -> {
                            List<String> result = ds
                                    .select("strTokens")
                                    .first()
                                    .getList(0)
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(4, result.size());
                            Assertions.assertTrue(result.stream().allMatch(s -> s.equals("127") || s.equals("4")));
                            Assertions.assertEquals(expected, result);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractDefaultInputTest() {
        // default "_raw": "5"
        String regex = "\\d+";
        Pattern pattern = Pattern.compile(regex);
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex " + regex + " output strTokens", testFile,
                        ds -> {
                            List<String> result = ds
                                    .select("strTokens")
                                    .first()
                                    .getList(0)
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(1, result.size());
                            Assertions.assertTrue(result.stream().allMatch(s -> pattern.matcher(s).matches()));
                            Assertions.assertTrue(result.stream().allMatch(s -> s.equals("5")));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractDefaultOutputTest() {
        // source: "127.4.4.4"
        String regex = "\\d+";
        Pattern pattern = Pattern.compile(regex);
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex " + regex + " input source", testFile, ds -> {
                            List<String> result = ds
                                    .select("tokens")
                                    .first()
                                    .getList(0)
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(4, result.size());
                            Assertions.assertTrue(result.stream().allMatch(s -> pattern.matcher(s).matches()));
                            Assertions.assertTrue(result.stream().allMatch(s -> s.equals("127") || s.equals("4")));
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgRegexExtractionMissingRegexExceptionTest() {
        AssertionFailedError exception = Assertions.assertThrows(AssertionFailedError.class, () -> {
            streamingTestUtil
                    .performDPLTest(
                            "index=index_A | teragrep exec regexextract input source output strTokens", testFile,
                            ds -> {
                            }
                    );
        });
        Assertions.assertTrue(exception.getMessage().contains("Missing regex parameter"));
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgHdfsSaveAfterBloomEstimateTestUsingRegexExtract() {
        final String id = UUID.randomUUID().toString();
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec regexextract regex \\d+ | teragrep exec bloom estimate | teragrep exec hdfs save overwrite=true /tmp/pth_10_hdfs/regexextract/"
                                + id,
                        testFile, ds -> {
                            List<String> listOfResult = ds
                                    .select("estimate(tokens)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Collections.singletonList("5"), listOfResult);
                        }
                );
        this.streamingTestUtil.setUp();
        streamingTestUtil
                .performDPLTest("| teragrep exec hdfs load /tmp/pth_10_hdfs/regexextract/" + id, testFile, ds -> {
                    List<String> listOfResult = ds
                            .select("estimate(tokens)")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Collections.singletonList("5"), listOfResult);
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgForEachBatchWithStatsTest() {
        streamingTestUtil.performDPLTest("index=index_A | teragrep exec foreachbatch | stats count", testFile, ds -> {
            List<String> listOfResult = ds
                    .select("count")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("5"), listOfResult);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgForEachBatchTest() {
        streamingTestUtil.performDPLTest("index=index_A | teragrep exec foreachbatch", testFile, ds -> {
            Assertions.assertEquals(5, ds.count());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgSetConfigStringTest() {
        Config fakeConfig = ConfigFactory
                .defaultApplication()
                .withValue("dpl.pth_00.dummy.value", ConfigValueFactory.fromAnyRef("oldValue"));
        streamingTestUtil.getCtx().setConfig(fakeConfig);
        Assertions.assertEquals("oldValue", streamingTestUtil.getCtx().getConfig().getString("dpl.pth_00.dummy.value"));
        streamingTestUtil
                .performDPLTest("index=index_A | teragrep set config dpl.pth_00.dummy.value newValue", testFile, ds -> {
                    Assertions
                            .assertEquals(
                                    "newValue",
                                    streamingTestUtil.getCtx().getConfig().getString("dpl.pth_00.dummy.value")
                            );
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgSetConfigLongWithNoLogicalStatementTest() {
        Config fakeConfig = ConfigFactory
                .defaultApplication()
                .withValue("dpl.pth_00.dummy.value", ConfigValueFactory.fromAnyRef(12345));
        streamingTestUtil.getCtx().setConfig(fakeConfig);
        Assertions.assertEquals(12345L, streamingTestUtil.getCtx().getConfig().getLong("dpl.pth_00.dummy.value"));
        streamingTestUtil.performDPLTest(" | teragrep set config dpl.pth_00.dummy.value 99999", testFile, ds -> {
            Assertions.assertEquals(99999L, streamingTestUtil.getCtx().getConfig().getLong("dpl.pth_00.dummy.value"));
            Assertions.assertEquals(0, ds.count());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgSetConfigLongTest() {
        Config fakeConfig = ConfigFactory
                .defaultApplication()
                .withValue("dpl.pth_00.dummy.value", ConfigValueFactory.fromAnyRef(12345));
        streamingTestUtil.getCtx().setConfig(fakeConfig);
        Assertions.assertEquals(12345L, streamingTestUtil.getCtx().getConfig().getLong("dpl.pth_00.dummy.value"));
        streamingTestUtil
                .performDPLTest("index=index_A | teragrep set config dpl.pth_00.dummy.value 99999", testFile, ds -> {
                    Assertions
                            .assertEquals(
                                    99999L, streamingTestUtil.getCtx().getConfig().getLong("dpl.pth_00.dummy.value")
                            );
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgSetConfigMismatchedTypesTest() {
        Config fakeConfig = ConfigFactory
                .defaultApplication()
                .withValue("dpl.pth_00.dummy.value", ConfigValueFactory.fromAnyRef(12345));
        streamingTestUtil.getCtx().setConfig(fakeConfig);
        Assertions.assertEquals(12345L, streamingTestUtil.getCtx().getConfig().getLong("dpl.pth_00.dummy.value"));
        Throwable t = streamingTestUtil
                .performThrowingDPLTest(
                        NumberFormatException.class,
                        "index=index_A | teragrep set config dpl.pth_00.dummy.value stringValue", testFile, ds -> {
                        }
                );

        Assertions.assertEquals("For input string: \"stringValue\"", t.getMessage());
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgGetConfigTest() {
        Config fakeConfig = ConfigFactory
                .defaultApplication()
                .withValue("dpl.pth_00.dummy.value", ConfigValueFactory.fromAnyRef(12345))
                .withValue("dpl.pth_00.another.dummy.value", ConfigValueFactory.fromAnyRef("string_here"));
        streamingTestUtil.getCtx().setConfig(fakeConfig);

        streamingTestUtil.performDPLTest("index=index_A | teragrep get config", testFile, ds -> {
            List<String> configs = ds
                    .select("_raw")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, configs.size());
            Assertions.assertTrue(configs.contains("dpl.pth_00.another.dummy.value = string_here"));
            Assertions.assertTrue(configs.contains("dpl.pth_00.dummy.value = 12345"));
        });
    }

}
