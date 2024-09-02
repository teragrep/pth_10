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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Rex4jTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(Rex4jTransformationTest.class);

    private final String testFile = "src/test/resources/rex4jTransformationTest_data*.json"; // * to make the path into a directory path
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
    public void rex4jTest_ExtractionModeMultipleCaptureGroups() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex4j \".*rainfall_rate\\\":\\s(?<rainFALL>\\d+.\\d+).*wind_speed\\\":\\s(?<windSPEDE>\\d+.\\d+).*latitude\\\":\\s(?<latiTUDE>-?\\d+.\\d+)\"",
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
    public void rex4jTest_ExtractionModeSingleCaptureGroup() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex4j \".*rainfall_rate\\\":\\s(?<rainFALL>\\d+.\\d+)\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rainfallRate = ds.select("rainFALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rainfallRate.size());

                // check values
                assertEquals("25.5", rainfallRate.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rex4jTest_ExtractionMode_EmptyResults() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex4j \"(?<rainFALL>)\"",
            testFile,
            ds -> {
                // get extracted column data
                List<Object> rainfallRate = ds.select("rainFALL").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0)).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rainfallRate.size());

                // check values
                assertEquals(streamingTestUtil.getCtx().nullValue.value(), rainfallRate.get(0));
            });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rex4jTest_SedMode() {
        streamingTestUtil.performDPLTest(
            "index=index_A | rex4j mode=sed \"s/rainfall_rate/meltdown_rate/g\"",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rawData = ds.select("_raw").dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rawData.size());

                // check values
                assertEquals("{\"meltdown_rate\": 25.5, \"wind_speed\": 51.0, \"atmosphere_water_vapor_content\": 76.5, \"atmosphere_cloud_liquid_water_content\": 2.5, \"latitude\": -89.625, \"longitude\": 139.875}",
                        rawData.get(0));
            });
    }

    @Test
    @Disabled(value = "Needs Spark 3.x to work")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void rex4jTest_ExtractNestedCaptureGroup_Issue391() {
        // FIXME: Rex4j needs regexp_extract_all support introduced in Spark 3.x
        //  Workaround is to use rex command instead
        streamingTestUtil.performDPLTest(
            "| makeresults count=1 |" +
                " eval _raw=\"RandomStuff name\\=john [INFO]notification(abc): " +
                "start\\=\\\"2022-01-01 11:11:11\\\" length\\=22 id\\=123 service\\=tcp/port:9999 " +
                "type\\=1 res zone\\=main spw zone\\=lan action\\=ok transmit\\=10 pswd\\=0 " +
                "src\\=192.168.4.2 dst\\=192.168.0.15 res_port\\=9999 spw_port\\=9998 res-xl ip\\=192.168.4.2 " +
                "port\\=99999 spw-xl ip\\=192.168.0.15 port\\=99998 sess_id\\=12345 reason\\=stop - TIME OUT\" " +
                "| rex4j \"id=(?<id>\\d+) service=(?<service>(tcp|udp)\\/port:\\d+) type=\\d+ res zone=(?<resZone>.*?) spw zone=(?<spwZone>.*?) action=(?<action>.*?) transmit=(?<transmit>\\d+) pswd=(?<pswd>\\d+) src=(?<sourceIP>\\d+\\.\\d+\\.\\d+\\.\\d+) dst=(?<destinationIP>\\d+\\.\\d+\\.\\d+\\.\\d+)( res_port=\\d+ res_port=(?<resultPort>\\d+)(.*?))? sess_id=(?<convId>\\d+) reason=(?<endReason>.*?)$\" | fields - _raw",
            testFile,
            ds -> {
                // get extracted column data
                List<String> rawData = ds.select("resZone")
                        .dropDuplicates().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                // every value should be unique
                assertEquals(1, rawData.size());

                // check values
                assertEquals("main",
                        rawData.get(0));
            });
    }
}


