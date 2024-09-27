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

import com.teragrep.pth10.steps.tokenizer.AbstractTokenizerStep;
import com.teragrep.pth10.steps.tokenizer.TokenizerStep;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TokenizerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RexTransformationTest.class);

    private final String testFile = "src/test/resources/rexTransformationTest_data*.json"; // * to make the path into a directory path
    private final String regexTestFile = "src/test/resources/bloomAggregationData*.json";
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
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tokenize() {
        streamingTestUtil.performDPLTest("index=index_A | teragrep exec tokenizer", testFile, ds -> {
            assertEquals("tokens", ds.columns()[ds.columns().length - 1]);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tokenize2() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec tokenizer format string input _raw output strtokens", testFile,
                        ds -> {
                            String row = ds.select("strtokens").first().getList(0).toString();
                            assertTrue(row.startsWith("[{, \", rainfall"));
                            assertEquals("strtokens", ds.columns()[ds.columns().length - 1]);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tokenize3() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec tokenizer format bytes input _raw output bytetokens", testFile,
                        ds -> {
                            String row = ds.select("bytetokens").first().getList(0).toString();
                            assertTrue(row.startsWith("[[B")); // bytes start with '[[B'
                            assertEquals("bytetokens", ds.columns()[ds.columns().length - 1]);
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testRegexTokenizerNotSupportedFormat() {
        Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.pattern", "testRegex");
        Config config = ConfigFactory.parseProperties(properties);

        TokenizerStep step = new TokenizerStep(config, AbstractTokenizerStep.TokenizerFormat.STRING, "_raw", "result");
        SparkSession spark = streamingTestUtil.getCtx().getSparkSession();
        Assertions
                .assertThrows(UnsupportedOperationException.class, () -> step.get(spark.read().schema(testSchema).csv(spark.emptyDataset(Encoders.STRING()))));
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testRegexTokenizerSelection() {
        String regex = "^\\d+";
        Pattern pattern = Pattern.compile(regex);
        Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.pattern", regex);
        Config config = ConfigFactory.parseProperties(properties);

        TokenizerStep step = new TokenizerStep(
                config,
                AbstractTokenizerStep.TokenizerFormat.BYTES,
                "source",
                "bytetokens"
        );
        SparkSession spark = streamingTestUtil.getCtx().getSparkSession();
        Dataset<Row> result = step.get(spark.read().schema(testSchema).json(regexTestFile));
        // first row value: 127.0.0.0
        List<Object> list = result.select("bytetokens").first().getList(0);
        List<String> tokens = new ArrayList<>();
        for (Object o : list) {
            tokens.add(new String((byte[]) o, StandardCharsets.UTF_8));
        }
        Assertions.assertEquals(4, list.size());
        Assertions.assertTrue(tokens.contains("127"));
        Assertions.assertTrue(tokens.contains("0"));
        Assertions.assertTrue(tokens.stream().allMatch(s -> pattern.matcher(s).matches()));
    }
}
