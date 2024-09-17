/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Tests for the new ProcessingStack implementation Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StackTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StackTest.class);

    private final String testFile = "src/test/resources/predictTransformationTest_data*.json"; // * to make the path into a directory path
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

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* chart -> chart */
    public void stackTest_Streaming_ChartChart() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | chart count(offset) as c_offset by partition | chart count(c_offset) as final",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals(Arrays.toString(ds.columns()), "[final]", "Batch handler dataset contained an unexpected column arrangement !");
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void stackTest_Streaming_1() {
        streamingTestUtil.performDPLTest("index=index_A", testFile, ds -> {
            Assertions
                    .assertEquals(Arrays.toString(ds.columns()), "[_time, id, _raw, index, sourcetype, host, source, partition, offset]", "Batch handler dataset contained an unexpected column arrangement !");
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* eval */
    public void stackTest_Streaming_Eval() {
        streamingTestUtil.performDPLTest("index=index_A | eval newField = offset * 5", testFile, ds -> {
            Assertions
                    .assertEquals(Arrays.toString(ds.columns()), "[_time, id, _raw, index, sourcetype, host, source, partition, offset, newField]", "Batch handler dataset contained an unexpected column arrangement !");
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* stats -> chart */
    public void stackTest_Streaming_StatsChart() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats count(_raw) as raw_count | chart count(raw_count) as count", testFile,
                        ds -> {
                            Assertions
                                    .assertEquals(Arrays.toString(ds.columns()), "[count]", "Batch handler dataset contained an unexpected column arrangement !");
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* stats -> stats */
    public void stackTest_Streaming_StatsStats() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats avg(offset) as avg1 count(offset) as c_offset dc(offset) as dc | stats count(avg1) as c_avg count(c_offset) as c_count count(dc) as c_dc",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals(Arrays.toString(ds.columns()), "[c_avg, c_count, c_dc]", "Batch handler dataset contained an unexpected column arrangement !");
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* stats -> chart -> eval */
    public void stackTest_Streaming_StatsChartEval() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats avg(offset) as avg_offset | chart count(avg_offset) as c_avg_offset | eval final=c_avg_offset * 5",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals(Arrays.toString(ds.columns()), "[c_avg_offset, final]", "Batch handler dataset contained an unexpected column arrangement !");
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* eval -> eval -> eval -> stats -> chart */
    public void stackTest_Streaming_EvalEvalEvalStatsChart() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval a=exp(offset) | eval b=pow(a, 2) | eval x = a + b | stats var(x) as field | chart count(field) as final",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals(Arrays.toString(ds.columns()), "[final]", "Batch handler dataset contained an unexpected column arrangement !");
                        }
                );
    }

    // TODO: remove disabled annotation when pth-03 issue #125 is closed
    // Fails because c is parsed as count() command, not a column
    @Test
    @Disabled(value = "requires parser fixes")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) /* eval -> eval -> eval -> stats -> chart */
    public void stackTest_Streaming_EvalEvalEvalStatsChart_with_c() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval a=exp(offset) | eval b=pow(a, 2) | eval c = a + b | stats var(c) as field | chart count(field) as final",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals(Arrays.toString(ds.columns()), "[final]", "Batch handler dataset contained an unexpected column arrangement !");
                        }
                );
    }
}
