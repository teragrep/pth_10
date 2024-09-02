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

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WhereTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WhereTransformationTest.class);

    private final String testFile = "src/test/resources/rex4jTransformationTest_data*.json"; // * to make the path into a directory path
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

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void WhereLikeWildcardTest() {
        streamingTestUtil.performDPLTest(
                "index=index_A | where _raw like \"%rainfall_rate%\"",
                testFile,
                ds -> {
                    // get extracted column data
                    List<String> rawColumn =
                            ds.select("_raw").dropDuplicates()
                            .collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals(1, rawColumn.size());
                });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void WhereLikeSameTextTest() {
        streamingTestUtil.performDPLTest(
                "index=index_A | where index like \"index_A\"",
                testFile,
                ds -> {
                    // get extracted column data
                    List<String> indexColumn = ds.select("index").dropDuplicates()
                            .collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals(1, indexColumn.size());
                });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void WhereLikeNoMatchTest() {
        streamingTestUtil.performDPLTest(
                "index=index_A | where index like \"index_A_\"",
                testFile,
                ds -> {
                    // get extracted column data
                    List<String> indexColumn = ds.select("index").dropDuplicates()
                            .collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals(0, indexColumn.size());
                });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void WhereLikeUnderscoreTest() {
        streamingTestUtil.performDPLTest(
                "index=index_A | where host like \"_ost\"",
                testFile,
                ds -> {
                    // get extracted column data
                    List<String> hostColumn = ds.select("host").dropDuplicates()
                            .collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals(1, hostColumn.size());
                });
    }
}



