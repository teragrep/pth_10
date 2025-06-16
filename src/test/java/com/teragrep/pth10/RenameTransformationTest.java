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

/**
 * Tests for the RenameTransformationTest implementation Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RenameTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RenameTransformationTest.class);

    private final String testFile = "src/test/resources/regexTransformationTest_data*.jsonl"; // * to make the path into a directory path
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
    )
    public void testRenameMultipleFields() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | rename _raw AS DATA , offset AS number, sourcetype AS typeOfSource, INVALID_FIELD AS fieldOfInvalid",
                        testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField("DATA", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField(
                                            "typeOfSource",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()), new StructField("number", DataTypes.LongType, true, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void rename_DoubleQuotes_test() {
        streamingTestUtil
                .performDPLTest(
                        "| makeresults count=1 | eval \"a\" = \"something\" | rename \"a\" as \"b\"", testFile, ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ), new StructField("b", DataTypes.StringType, false, new MetadataBuilder().build())
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void rename_SingleQuotes_test() {
        streamingTestUtil
                .performDPLTest(
                        "| makeresults count=1 | eval 'abc(def)' = \"xyz\" | rename 'abc(def)' as '(foo)bar'", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "_time",
                                            DataTypes.TimestampType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField(
                                            "(foo)bar",
                                            DataTypes.StringType,
                                            false,
                                            new MetadataBuilder().build()
                                    )
                            });
                            Assertions
                                    .assertEquals(
                                            expectedSchema, ds.schema(),
                                            "Batch handler dataset contained an unexpected column arrangement !"
                                    );
                        }
                );
    }
}
