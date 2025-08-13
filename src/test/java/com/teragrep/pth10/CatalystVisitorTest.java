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
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLTimeFormat;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalystVisitorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalystVisitorTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestDataStreaming/xmlWalkerTestDataStreaming*";
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
    void searchQueryWithNotTest() {
        String q = "index = \"cpu\" AND sourcetype = \"log:cpu:0\" NOT src";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            //        e = "((`index` LIKE 'cpu' AND `sourcetype` LIKE 'log:cpu:0') AND (NOT `_raw` LIKE '%src%'))";
            String e = "(RLIKE(index, (?i)^cpu$) AND (RLIKE(sourcetype, (?i)^log:cpu:0) AND (NOT RLIKE(_raw, (?i)^.*\\Qsrc\\E.*))))";

            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithOrTest() {
        final String query = "(index!=strawberry sourcetype=example:strawberry:strawberry host=loadbalancer.example.com) OR (index=* host=firewall.example.com earliest=2021-01-26T00:00:00Z latest=2021-04-26T00:00:00Z \"Denied\")";
        final String expected = "(((NOT RLIKE(index, (?i)^strawberry$)) AND (RLIKE(sourcetype, (?i)^example:strawberry:strawberry) AND RLIKE(host, (?i)^loadbalancer.example.com))) OR (RLIKE(index, (?i)^.*$) AND (((RLIKE(host, (?i)^firewall.example.com) AND (_time >= from_unixtime(1611619200, yyyy-MM-dd HH:mm:ss))) AND (_time < from_unixtime(1619395200, yyyy-MM-dd HH:mm:ss))) AND RLIKE(_raw, (?i)^.*\\QDenied\\E.*))))";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();
            Assertions.assertEquals(expected, ctx.getSparkQuery());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithTimestampTest() {
        // Add time ranges
        String q = "((( index =\"cpu\" AND host = \"sc-99-99-14-25\" ) AND sourcetype = \"log:cpu:0\" ) AND ( earliest= \"01/01/1970:02:00:00\"  AND latest= \"01/01/2030:00:00:00\" ))";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            try {
                long earliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss")
                        .instantOf("01/01/1970:02:00:00")
                        .getEpochSecond();
                long latestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss")
                        .instantOf("01/01/2030:00:00:00")
                        .getEpochSecond();
                String e = "(((RLIKE(index, (?i)^cpu$) AND RLIKE(host, (?i)^sc-99-99-14-25)) AND RLIKE(sourcetype, (?i)^log:cpu:0)) AND ((_time >= from_unixtime("
                        + earliestEpoch + ", yyyy-MM-dd HH:mm:ss)) AND (_time < from_unixtime(" + latestEpoch
                        + ", yyyy-MM-dd HH:mm:ss))))";
                DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

                String result = ctx.getSparkQuery();
                Assertions.assertEquals(e, result);
            }
            catch (ParseException e) {
                Assertions.fail(e.getMessage());
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithAndTest() {
        //LOGGER.info("------ AND ---------");
        String q = "index =\"strawberry\" AND sourcetype =\"example:strawberry:strawberry\"";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "(RLIKE(index, (?i)^strawberry$) AND RLIKE(sourcetype, (?i)^example:strawberry:strawberry))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            String result = ctx.getSparkQuery();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithOrTest2() {
        //LOGGER.info("------ OR ---------");
        String q = "index != \"strawberry\" OR sourcetype =\"example:strawberry:strawberry\"";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "((NOT RLIKE(index, (?i)^strawberry$)) OR RLIKE(sourcetype, (?i)^example:strawberry:strawberry))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            String result = ctx.getSparkQuery();
            Assertions.assertEquals(e, result);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithAggrTest() {
        String q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
            long earliest = Assertions.assertDoesNotThrow(() -> tf.instantOf("04/16/2020:10:25:40")).getEpochSecond();
            String e = "(RLIKE(index, (?i)^cinnamon$) AND (_time >= from_unixtime(" + earliest
                    + ", yyyy-MM-dd HH:mm:ss)))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            String result = ctx.getSparkQuery();
            // Check logical part
            Assertions.assertEquals(e, result);
        });
    }

    // index = cinnamon _index_earliest="04/16/2020:10:25:40"
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithIndexEarliestTest() {
        this.streamingTestUtil
                .performDPLTest("index = cinnamon _index_earliest=\"04/16/2020:10:25:40\"", this.testFile, res -> {
                    final StructType expectedSchema = new StructType(new StructField[] {
                            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                            new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                            new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                            new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                            new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                            new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                            new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                            new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    });

                    // check schema
                    Assertions.assertEquals(expectedSchema, res.schema());

                    String logicalPart = this.streamingTestUtil.getCtx().getSparkQuery();
                    // check column for archive query i.e. only logical part'
                    DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
                    long indexEarliestEpoch = Assertions
                            .assertDoesNotThrow(() -> tf.instantOf("04/16/2020:10:25:40"))
                            .getEpochSecond();
                    String e = "(RLIKE(index, (?i)^cinnamon$) AND (_time >= from_unixtime(" + indexEarliestEpoch
                            + ", yyyy-MM-dd HH:mm:ss)))";
                    Assertions.assertEquals(e, logicalPart);
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQueryWithStringTest() {
        // Use this file as the test data
        String testFile = "src/test/resources/subsearchData*.jsonl";

        this.streamingTestUtil.performDPLTest("index=index_A \"(1)(enTIty)\"", testFile, res -> {

            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("origin", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
            });

            Assertions.assertEquals(expectedSchema, res.schema());
            // Check result count
            List<Row> lst = res.collectAsList();
            // check result count
            Assertions.assertEquals(1, lst.size());

            // get logical part
            String logicalPart = this.streamingTestUtil.getCtx().getSparkQuery();
            String e = "(RLIKE(index, (?i)^index_A$) AND RLIKE(_raw, (?i)^.*\\Q(1)(enTIty)\\E.*))";
            Assertions.assertEquals(e, logicalPart);
        });
    }

    // Check that is AggregatesUsed returns false
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void aggregatesUsedTest() {
        this.streamingTestUtil.performDPLTest("index = jla02logger ", this.testFile, res -> {
            boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
            Assertions.assertFalse(aggregates);
        });
    }

    // Check that issue#179 returns user friendly error message
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void searchQualifierMissingRightSideTest() {
        // assert user-friendly exception
        RuntimeException thrown = this.streamingTestUtil
                .performThrowingDPLTest(RuntimeException.class, "index = ", this.testFile, res -> {
                });

        Assertions
                .assertEquals(
                        "The right side of the search qualifier was empty! Check that the index has a valid value, like 'index = cinnamon'.",
                        thrown.getMessage()
                );
    }
}
