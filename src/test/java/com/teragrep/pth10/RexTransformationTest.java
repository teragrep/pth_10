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

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RexTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RexTransformationTest.class);

    private final String testFile = "src/test/resources/rexTransformationTest_data*.json"; // * to make the path into a directory path
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

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_ExtractionModeSingleCaptureGroup() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex \".*rainfall_rate\\\":\\s(?<rainFALL>\\d+.\\d+)\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rainfallRate = ds.select("rainFALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rainfallRate.size());

                // check values
                assertEquals("139.875", rainfallRate.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_ExtractionModeSingleCaptureGroup_NoMatch() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex \".*rainfall_rate\\\":\\s(?<rainFALL>(noMatch))\"",
            testFile,
            ds -> {
                // get extracted column data
                List<Object> rainfallRate = ds.select("rainFALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0)).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rainfallRate.size());

                // check values: should be nullValue
                assertEquals(streamingTestUtil.getCtx().nullValue.value(), rainfallRate.get(0));
            });
    }

    // underscore in the capture group name
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_ExtractionModeSingleCaptureGroupWithUnderscore() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex \".*rainfall_rate\\\":\\s(?<rain_FALL>\\d+.\\d+)\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rainfallRate = ds.select("rain_FALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rainfallRate.size());

                // check values
                assertEquals("139.875", rainfallRate.get(0));
            });
    }

    // underscore in the capture group name, rex in sequential mode
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_ExtractionModeSingleCaptureGroupWithUnderscore_sequentialMode() {
        // sort command is sequential only
        streamingTestUtil.performDPLTest(
            "index=index_A | sort offset | rex \".*rainfall_rate\\\":\\s(?<rain_FALL>\\d+.\\d+)\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rainfallRate = ds.select("rain_FALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rainfallRate.size());

                // check values
                assertEquals("139.875", rainfallRate.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_ExtractionModeMultipleCaptureGroups() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex \".*rainfall_rate\\\":\\s(?<rainFALL>\\d+.\\d+).*wind_speed\\\":\\s(?<windSPEDE>\\d+.\\d+).*latitude\\\":\\s(?<latiTUDE>-?\\d+.\\d+)\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rainfallRate = ds.select("rainFALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> windSpeed = ds.select("windSPEDE").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> latitude = ds.select("latiTUDE").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, latitude.size());
                assertEquals(1, windSpeed.size());
                assertEquals(1, rainfallRate.size());

                // check values
                assertEquals("25.5", rainfallRate.get(0));
                assertEquals("51.0", windSpeed.get(0));
                assertEquals("-89.625", latitude.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_SedMode() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex mode=sed \"s/rainfall_rate/meltdown_rate/1\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rawData = ds.select("_raw").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rawData.size());

                // check values
                assertEquals("{\"meltdown_rate\": 25.5, \"wind_speed\": 51.0, \"atmosphere_water_vapor_content\": 76.5, \"atmosphere_cloud_liquid_water_content\": 2.5, \"latitude\": -89.625, \"rainfall_rate\": 139.875}",
                        rawData.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_SedMode2() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex mode=sed \"s/rainfall_rate/meltdown_rate/g\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rawData = ds.select("_raw").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rawData.size());

                // check values
                assertEquals("{\"meltdown_rate\": 25.5, \"wind_speed\": 51.0, \"atmosphere_water_vapor_content\": 76.5, \"atmosphere_cloud_liquid_water_content\": 2.5, \"latitude\": -89.625, \"meltdown_rate\": 139.875}",
                        rawData.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_SedMode_CustomDelimiter() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex mode=sed \"s;rainfall_rate;meltdown_rate;g\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rawData = ds.select("_raw").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rawData.size());

                // check values
                assertEquals("{\"meltdown_rate\": 25.5, \"wind_speed\": 51.0, \"atmosphere_water_vapor_content\": 76.5, \"atmosphere_cloud_liquid_water_content\": 2.5, \"latitude\": -89.625, \"meltdown_rate\": 139.875}",
                        rawData.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_SedMode_MissingSubstituteFlag() {
        StreamingQueryException sqe = streamingTestUtil.performThrowingDPLTest(StreamingQueryException.class,
                /* DPL Query = */		   "index=index_A | rex mode=sed \";rainfall_rate;meltdown_rate;g\"", testFile,
                ds -> {
                    // get extracted column data
                    List<String> rawData = ds.select("_raw").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    // every value should be unique
                    assertEquals(1, rawData.size());

                    // check values
                    assertEquals("{\"meltdown_rate\": 25.5, \"wind_speed\": 51.0, \"atmosphere_water_vapor_content\": 76.5, \"atmosphere_cloud_liquid_water_content\": 2.5, \"latitude\": -89.625, \"meltdown_rate\": 139.875}",
                            rawData.get(0));
                });

        assertEquals("Caused by: java.lang.IllegalStateException: Expected sed mode to be substitute! Other modes are not supported.",
                streamingTestUtil.getInternalCauseString(sqe.cause(), IllegalStateException.class));
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rexTest_SedMode3() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex mode=sed \"s/rainfall_rate/meltdown_rate/2g\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rawData = ds.select("_raw").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rawData.size());

                // check values
                assertEquals("{\"rainfall_rate\": 25.5, \"wind_speed\": 51.0, \"atmosphere_water_vapor_content\": 76.5, \"atmosphere_cloud_liquid_water_content\": 2.5, \"latitude\": -89.625, \"meltdown_rate\": 139.875}",
                        rawData.get(0));
            });
    }
}


