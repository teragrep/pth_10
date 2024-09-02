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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for xmlkv command
 * Uses streaming datasets
 *
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class XmlkvTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlkvTransformationTest.class);

    private final StructType testSchema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
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

    // * to make the path into a directory path
    final String XML_DATA_1 = "src/test/resources/xmlkv/xmlkv_0*.json";
    final String XML_DATA_2 = "src/test/resources/xmlkv/xmlkv_1*.json";
    final String INVALID_DATA = "src/test/resources/xmlkv/xmlkv_inv*.json";

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void xmlkvTest0() {
        streamingTestUtil.performDPLTest(
                "index=index_A | xmlkv _raw",
                XML_DATA_2,
                ds -> {
                    assertEquals("[_time, id, _raw, index, sourcetype, host, source, partition, offset, item, something]",
                            Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");

                    String result = ds
                            .select("item", "something")
                            .dropDuplicates()
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString()
                                    .concat(";")
                                    .concat(r.getAs(1).toString())
                            )
                            .collect(Collectors.toList()).get(0);
                    assertEquals("b;123", result);
                }
        );
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void xmlkvTest1() {
        streamingTestUtil.performDPLTest(
                "index=index_A | xmlkv _raw",
                XML_DATA_1,
                ds -> {
                    assertEquals("[_time, id, _raw, index, sourcetype, host, source, partition, offset, item]",
                            Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");

                    String result = ds
                            .select("item")
                            .dropDuplicates()
                            .collectAsList()
                            .stream()
                            .map(
                                    r -> r.getAs(0).toString()
                            )
                            .collect(Collectors.toList()).get(0);
                    assertEquals("Hello world", result);
                }
        );
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void xmlkvTest2() {
        streamingTestUtil.performDPLTest(
                "index=index_A | xmlkv _raw",
                INVALID_DATA,
                ds -> {
                    // invalid data does not generate a result; only checking column arrangement
                    // to be the same as the input data.
                    assertEquals(Arrays.toString(testSchema.fieldNames()),
                            Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");
                }
        );
    }
}