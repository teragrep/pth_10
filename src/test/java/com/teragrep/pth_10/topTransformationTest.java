/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
package com.teragrep.pth_10;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class topTransformationTest {

    String testFile = "src/test/resources/xmlWalkerTestDataStreaming/xmlWalkerTestDataStreaming*";
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
    @Disabled(value = "Enable after fixing issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopLimitParameter() {
        streamingTestUtil.performDPLTest("index=index_A | top limit=3 sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.IntegerType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.LongType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(3, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "Enable after fixing issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopIntegerLimit() {
        streamingTestUtil.performDPLTest("index=index_A | top 3 sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.IntegerType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.LongType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(3, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "Enable after fixing issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopDefaultLimit() {
        streamingTestUtil.performDPLTest("index=index_A | top offset", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.DoubleType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(10, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "Enable after fixing issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopByclauseWithIntegerLimit() {
        streamingTestUtil.performDPLTest("index=index_A | top 5 _raw by sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.DoubleType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopByclauseWithLimitParameter() {
        streamingTestUtil.performDPLTest("index=index_A | top limit=5 _raw by sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.DoubleType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopBYclauseWithDefaultLimit() {
        streamingTestUtil.performDPLTest("index=index_A | top _raw by sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.DoubleType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopMultipleFields() {
        streamingTestUtil.performDPLTest("index=index_A | top 5 _raw, host, offset", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.DoubleType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #666 - https://github.com/teragrep/pth_10/issues/666")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopMultipleFieldsByclause() {
        streamingTestUtil.performDPLTest("index=index_A | top 3 _raw, host, offset by sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.DoubleType, true, new MetadataBuilder().build())
            });

            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #758 - https://github.com/teragrep/pth_10/issues/758")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopCountField() {
        streamingTestUtil
                .performDPLTest("index=index_A | top 3 countfield=sourcetypeCounts sourcetype", testFile, ds -> {
                    final StructType expectedSchema = new StructType(new StructField[] {
                            new StructField("sourcetype", DataTypes.LongType, true, new MetadataBuilder().build()),
                            new StructField(
                                    "sourcetypeCounts",
                                    DataTypes.IntegerType,
                                    true,
                                    new MetadataBuilder().build()
                            ),
                            new StructField("percent", DataTypes.LongType, true, new MetadataBuilder().build())
                    });
                    Assertions.assertEquals(3, ds.count());
                    Assertions.assertEquals(expectedSchema, ds.schema());
                });
    }

    @Test
    @Disabled(value = "issue #759 - https://github.com/teragrep/pth_10/issues/759")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopPercentField() {
        streamingTestUtil
                .performDPLTest("index=index_A | top percentfield=percentage_of_events offset", testFile, ds -> {
                    final StructType expectedSchema = new StructType(new StructField[] {
                            new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                            new StructField("count", DataTypes.IntegerType, true, new MetadataBuilder().build()),
                            new StructField(
                                    "percentage_of_events",
                                    DataTypes.LongType,
                                    true,
                                    new MetadataBuilder().build()
                            )
                    });
                    Assertions.assertEquals(10, ds.count());
                    Assertions.assertEquals(expectedSchema, ds.schema());
                });
    }

    @Test
    @Disabled(value = "issue #760 - https://github.com/teragrep/pth_10/issues/760")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopShowCount() {
        streamingTestUtil.performDPLTest("index=index_A | top 5 showcount=false _raw", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.LongType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #761 - https://github.com/teragrep/pth_10/issues/761")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopShowPerc() {
        streamingTestUtil.performDPLTest("index=index_A | top limit=5 showperc=f _raw", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #762 - https://github.com/teragrep/pth_10/issues/762")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopUseother() {
        streamingTestUtil.performDPLTest("index=index_A | top 2 useother=true sourcetype", testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("percent", DataTypes.LongType, true, new MetadataBuilder().build())
            });

            List<String> sourcetypeList = ds
                    .select("sourcetype")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertTrue(sourcetypeList.contains("OTHER"));
            Assertions.assertEquals(3, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @Disabled(value = "issue #762 - https://github.com/teragrep/pth_10/issues/762")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopOtherstr() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | top limit=2 useother=true otherstr=otherSourcetypes sourcetype", testFile,
                        ds -> {
                            final StructType expectedSchema = new StructType(new StructField[] {
                                    new StructField(
                                            "sourcetype",
                                            DataTypes.StringType,
                                            true,
                                            new MetadataBuilder().build()
                                    ),
                                    new StructField("count", DataTypes.LongType, true, new MetadataBuilder().build()),
                                    new StructField("percent", DataTypes.LongType, true, new MetadataBuilder().build())
                            });

                            List<String> sourcetypeList = ds
                                    .select("sourcetype")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            Assertions.assertTrue(sourcetypeList.contains("otherSourcetypes"));
                            Assertions.assertEquals(3, ds.count());
                            Assertions.assertEquals(expectedSchema, ds.schema());
                        }
                );
    }

    @Test
    @Disabled(value = "issue #884 - https://github.com/teragrep/pth_10/issues/884")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopInvalidStringLimitParameterValue() {
        final String query = "index=index_A | top limit=string _raw";
        IllegalArgumentException invalidArgument = this.streamingTestUtil
                .performThrowingDPLTest(IllegalArgumentException.class, query, testFile, ds -> {
                });
        Assertions
                .assertEquals(
                        "Invalid value for limit parameter, it only takes positive integers.",
                        invalidArgument.getMessage()
                );
    }

    @Test
    @Disabled(value = "issue #884 - https://github.com/teragrep/pth_10/issues/884")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testTopInvalidDecimalLimitParameterValue() {
        final String query = "index=index_A | top limit=0.5 _raw";
        IllegalArgumentException invalidArgument = this.streamingTestUtil
                .performThrowingDPLTest(IllegalArgumentException.class, query, testFile, ds -> {
                });
        Assertions
                .assertEquals(
                        "Invalid value for limit parameter, it only takes positive integers.",
                        invalidArgument.getMessage()
                );
    }

}
