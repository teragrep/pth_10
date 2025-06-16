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
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConvertTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertTransformationTest.class);

    // Use this file for dataset initialization
    String testFile = "src/test/resources/convertTfData*.jsonl"; // * to make the path into a directory path

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

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertCtimeAs() {
        // "%m/%d/%Y %H:%M:%S";
        streamingTestUtil.performDPLTest("index=index_A | convert ctime(offset) AS new", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("new", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, ds.schema());

            List<String> listOfResults = ds
                    .select("new")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            for (String s : listOfResults) {
                // match 00/00/0000 00:00:00
                Matcher m = Pattern.compile("\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}").matcher(s);

                Assertions.assertTrue(m.find());
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertCtime() {
        streamingTestUtil.performDPLTest("index=index_A | convert ctime(offset)", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, ds.schema());

            List<String> listOfResults = ds
                    .select("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> expectedResults = Arrays
                    .asList(
                            "01/01/1970 00:00:11", "01/01/1970 00:00:11", "01/01/1970 00:00:10", "01/01/1970 00:00:09",
                            "01/01/1970 00:00:08", "01/01/1970 00:00:07", "01/01/1970 00:00:06", "01/01/1970 00:00:05",
                            "01/01/1970 00:00:04", "01/01/1970 00:00:03", "01/01/1970 00:00:02", "01/01/1970 00:00:01"
                    );

            for (int i = 0; i < listOfResults.size(); i++) {
                Assertions.assertEquals(expectedResults.get(i), listOfResults.get(i));
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "runSparkTest",
            matches = "true"
    )
    void testConvertMktime() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | convert timeformat=\"%Y-%m-%d'T'%H:%M:%S.%f%z\" mktime(_time) as epochTime",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("epochTime", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("epochTime")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            // rows get sorted by timestamp, so the order differs from original test data
                            List<String> expectedResults = Arrays
                                    .asList(
                                            "1286698810", "1286698210", "1286694610", "1252476549", "1218172088",
                                            "1183781227", "1149563166", "1115258705", "1081040644",
                                            // Below epochs are winter months, but still +0300 (differs from local finnish time)
                                            "1046649783", "1012604522", "978300061"
                                    );

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertMktimeWithDefaultTimezone() { // Use the system default timezone when timezone is not specified
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval a=\"2001-01-01T01:01:01.010\" | convert timeformat=\"%Y-%m-%d'T'%H:%M:%S.%f\" mktime(a) as epochTime",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("a", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("epochTime", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("epochTime")
                                    .distinct()
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections
                                    .singletonList(
                                            "978303661" // +0300 timezone
                                    );

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );

    }

    @Test
    @DisabledIfSystemProperty(
            named = "runSparkTest",
            matches = "true"
    )
    void testConvertDur2sec() {
        streamingTestUtil.performDPLTest("index=index_A | convert dur2sec(dur) as dur_sec", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("dur_sec", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, ds.schema());

            List<String> listOfResults = ds
                    .select("dur_sec")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            // rows get sorted by timestamp, so the order differs from original test data
            List<String> expectedResults = Arrays
                    .asList(
                            "5430", "0", "195792", "3600", "3661", "2400", "3723", "22", "7432", "1403", "24202",
                            "45296"
                    );

            Assertions.assertEquals(expectedResults, listOfResults);
        });

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertMemk() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat offset \"m\" offsetM | strcat offset \"k\" offsetK | strcat offset \"g\" offsetG | convert memk(offsetM) as memk_M memk(offsetK) as memk_K memk(offsetG) as memk_G memk(offset) as memk_def",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("offsetM", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("offsetK", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("offsetG", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("memk_M", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("memk_K", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("memk_G", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("memk_def", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> resDef = ds
                                    .select("memk_def")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> resK = ds
                                    .select("memk_K")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> resM = ds
                                    .select("memk_M")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> resG = ds
                                    .select("memk_G")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            // rows get sorted by timestamp, so the order differs from original test data
                            List<String> expDef = Arrays
                                    .asList(
                                            "11.0", "11.0", "10.0", "9.0", "8.0", "7.0", "6.0", "5.0", "4.0", "3.0",
                                            "2.0", "1.0"
                                    );
                            List<String> expM = Arrays
                                    .asList(
                                            "11264.0", "11264.0", "10240.0", "9216.0", "8192.0", "7168.0", "6144.0",
                                            "5120.0", "4096.0", "3072.0", "2048.0", "1024.0"
                                    );
                            List<String> expG = Arrays
                                    .asList(
                                            "1.1534336E7", "1.1534336E7", "1.048576E7", "9437184.0", "8388608.0",
                                            "7340032.0", "6291456.0", "5242880.0", "4194304.0", "3145728.0",
                                            "2097152.0", "1048576.0"
                                    );

                            Assertions.assertEquals(expDef, resDef);
                            Assertions.assertEquals(expDef, resK); // def is same as K
                            Assertions.assertEquals(expM, resM);
                            Assertions.assertEquals(expG, resG);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertMstime() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"\" \"47.\" \"329\" mst | strcat \"32:\" \"47.\" \"329\" mst2 | convert mstime(mst) as res mstime(mst2) as res2",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("mst2", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("res2", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> listOfResults2 = ds
                                    .select("res2")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections.singletonList("47329");
                            List<String> expectedResults2 = Collections.singletonList("1967329");

                            Assertions.assertEquals(expectedResults, listOfResults);
                            Assertions.assertEquals(expectedResults2, listOfResults2);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmcomma() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"\" \"47,\" \"329\" mst | strcat \"32,\" \"47,\" \"329\" mst2 | convert rmcomma(mst) as res rmcomma(mst2) as res2",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("mst2", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res2", DataTypes.StringType, false, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> listOfResults2 = ds
                                    .select("res2")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections.singletonList("47329");
                            List<String> expectedResults2 = Collections.singletonList("3247329");

                            Assertions.assertEquals(expectedResults, listOfResults);
                            Assertions.assertEquals(expectedResults2, listOfResults2);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunit() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"329\" \"abc\" as mst | convert rmunit(mst) as res", testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            List<String> expectedResults = Collections.singletonList("329");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitWithFloat() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"329.45\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            List<String> expectedResults = Collections.singletonList("329.45");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitExponentValues() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \".54e2\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            List<String> expectedResults = Collections.singletonList(".54E2");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitExponentValuesWithPrecedingMinus() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"-0.54e2\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections.singletonList("-0.54E2");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitWithInvalidNumbers() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"-0.21.54e2\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections.singletonList("");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitExponentValuesWithPrecedingPlus() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"+21.54e23\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections.singletonList("+21.54E23");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitNegExponentValuesWithPrecedingPlus() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"+21.54e-23\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            List<String> expectedResults = Collections.singletonList("+21.54E-23");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertRmunitPositiveExponentValuesWithPrecedingPlus() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"+21.54e+23\" \"abc\" as mst | convert rmunit(mst) as res", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("mst", DataTypes.StringType, false, new MetadataBuilder().build()), new StructField("res", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("res")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> expectedResults = Collections.singletonList("+21.54E+23");

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertAuto() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"329\" \"\" with_results |strcat \"329\" \"aa\" no_results | convert auto(with_results) | convert auto(no_results)",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("with_results", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("no_results", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("with_results")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> listOfResults2 = ds
                                    .select("no_results")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            List<String> expectedResults = Collections.singletonList("329.0");

                            List<String> expectedResults2 = Collections.singletonList("329aa");

                            Assertions.assertEquals(expectedResults, listOfResults);
                            Assertions.assertEquals(expectedResults2, listOfResults2);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertNum() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | strcat \"329\" \"\" with_results |strcat \"329\" \"aa\" no_results | convert num(with_results) | convert num(no_results)",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("with_results", DataTypes.DoubleType, true, new MetadataBuilder().build()), new StructField("no_results", DataTypes.DoubleType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("with_results")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> listOfResults2 = ds
                                    .select("no_results")
                                    .limit(1)
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0) == null ? "null" : r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            List<String> expectedResults = Collections.singletonList("329.0");

                            List<String> expectedResults2 = Collections.singletonList("null");

                            Assertions.assertEquals(expectedResults, listOfResults);
                            Assertions.assertEquals(expectedResults2, listOfResults2);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testConvertNone() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | convert dur2sec(\"dur|offset\") AS dur_sec none(offset)", testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("dur", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "partition",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("dur_sec", DataTypes.StringType, true, new MetadataBuilder().build())
                            });
                            Assertions.assertEquals(expectedSchema, ds.schema());

                            List<String> listOfResults = ds
                                    .select("dur_sec")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            // rows get sorted by timestamp, so the order differs from original test data
                            List<String> expectedResults = Arrays
                                    .asList(
                                            "5430", "0", "195792", "3600", "3661", "2400", "3723", "22", "7432", "1403",
                                            "24202", "45296"
                                    );

                            Assertions.assertEquals(expectedResults, listOfResults);
                        }
                );
    }
}
