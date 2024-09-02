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

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.DPLTimeFormat;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalystVisitorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalystVisitorTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestDataStreaming/xmlWalkerTestDataStreaming*";
    private StreamingTestUtil streamingTestUtil;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
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

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void fromStringNot2Test() {
        String q = "index = \"cpu\" AND sourcetype = \"log:cpu:0\" NOT src";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            //        e = "((`index` LIKE 'cpu' AND `sourcetype` LIKE 'log:cpu:0') AND (NOT `_raw` LIKE '%src%'))";
            String e = "(RLIKE(index, (?i)^cpu$) AND (RLIKE(sourcetype, (?i)^log:cpu:0) AND (NOT RLIKE(_raw, (?i)^.*\\Qsrc\\E.*))))";

            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);
        });
    }

    @Disabled
    @Test
    void columnFromStringTest() {
        String q, e;
        Column result;
        // Add time ranges
        q = "<OR><AND><AND><index value=\"strawberry\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:strawberry:strawberry\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        e = "((((NOT (`index` = 'strawberry')) AND (`sourcetype` = 'example:strawberry:strawberry')) AND (`host` = 'loadbalancer.example.com')) OR ((((`index` = '*') AND (`host` = 'firewall.example.com')) AND (`_time` >= DATE '2021-01-26')) AND (`_time` <= DATE '2021-04-26')))";
        DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        LOGGER.debug(tree.toStringTree(parser));
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        result = visitor.getLogicalPartAsColumn();
        assertEquals(e, result.expr().sql());
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void columnFromStringDateTest() {
        // Add time ranges
        String q = "((( index =\"cpu\" AND host = \"sc-99-99-14-25\" ) AND sourcetype = \"log:cpu:0\" ) AND ( earliest= \"01/01/1970:02:00:00\"  AND latest= \"01/01/2030:00:00:00\" ))";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            try {
                long earliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("01/01/1970:02:00:00");
                long latestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("01/01/2030:00:00:00");
                String e = "(((RLIKE(index, (?i)^cpu$) AND RLIKE(host, (?i)^sc-99-99-14-25)) AND RLIKE(sourcetype, (?i)^log:cpu:0)) AND ((_time >= from_unixtime(" + earliestEpoch + ", yyyy-MM-dd HH:mm:ss)) AND (_time < from_unixtime("+ latestEpoch + ", yyyy-MM-dd HH:mm:ss))))";
                DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

                String result = ctx.getSparkQuery();
                LOGGER.info("Query=" + q);
                LOGGER.info("Expected=" + e);
                LOGGER.info("Result=" + result);
                assertEquals(e, result);
            } catch (ParseException e) {
                fail(e.getMessage());
            }
        });
    }

    @Disabled
        // FIXME * to %
    void columnFromStringDate1Test() {
        String q, e;
        Column result;
        q = "((index !=\"strawberry\" AND sourcetype =\"example:strawberry:strawberry\") AND host =\"loadbalancer.example.com\" ) OR  (((index = \"*\"  AND host =\"firewall.example.com\") AND earliest= \"01/01/1970:02:00:00\" ) AND latest= \"01/01/2030:00:00:00\" ) AND Denied";

        try {
            long zero = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("01/01/1970:02:00:00");
            long epoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("01/01/2030:00:00:00");

            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            e = "((((NOT `index` LIKE 'strawberry') AND `sourcetype` LIKE 'example:strawberry:strawberry') AND `host` LIKE 'loadbalancer.example.com') OR ((((`index` LIKE '*' AND `host` LIKE 'firewall.example.com') AND (`_time` >= from_unixtime("+zero+", 'yyyy-MM-dd HH:mm:ss'))) AND (`_time` <= from_unixtime("+epoch+", 'yyyy-MM-dd HH:mm:ss'))) AND `_raw` RLIKE '(?i)^.*\\\\QDenied\\\\E.*'))";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        Object n = visitor.visit(tree);
        result = visitor.getLogicalPartAsColumn();
            assertEquals(e, result.expr().sql());
        } catch (ParseException exception) {
            fail(exception.getMessage());
        }
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void columnFromStringAndTest() {
        //LOGGER.info("------ AND ---------");
        String q = "index =\"strawberry\" AND sourcetype =\"example:strawberry:strawberry\"";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "(RLIKE(index, (?i)^strawberry$) AND RLIKE(sourcetype, (?i)^example:strawberry:strawberry))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            String result = ctx.getSparkQuery();
            assertEquals(e, result);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void columnFromStringOrTest() {
        //LOGGER.info("------ OR ---------");
        String q = "index != \"strawberry\" OR sourcetype =\"example:strawberry:strawberry\"";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "((NOT RLIKE(index, (?i)^strawberry$)) OR RLIKE(sourcetype, (?i)^example:strawberry:strawberry))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            String result = ctx.getSparkQuery();
            assertEquals(e, result);
        });
    }

    //earliest="05/12/2021:00:00:00" latest="05/12/2021:23:59:59"
    // _time >= from_unixtime(1619395200) AND _time <= from_unixtime(1619481599)>
    @Disabled
    //FIXME * to %
    void filterTest() {
        String q, e;
        Column result;
        q = "((index !=\"strawberry\" AND sourcetype =\"example:strawberry:strawberry\") AND host =\"loadbalancer.example.com\" )  OR ( index =\"*\" AND host = \"firewall.example.com\" ) AND  earliest= \"04/26/2021:07:00:00\"  AND latest= \"04/26/2021:08:00:00\" AND \"Denied\"";

        try {
            long earliest = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/26/2021:07:00:00");
            long latest = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/26/2021:08:00:00");
//        e = "((((NOT `index` LIKE 'strawberry') AND `sourcetype` LIKE 'example:strawberry:strawberry') AND `host` LIKE 'loadbalancer.example.com') OR ((((`index` LIKE '*' AND `host` LIKE 'firewall.example.com') AND (`_time` >= from_unixtime("+earliest+", 'yyyy-MM-dd HH:mm:ss'))) AND (`_time` <= from_unixtime("+latest+", 'yyyy-MM-dd HH:mm:ss'))) AND `_raw` LIKE '%Denied%'))";
            e = "((((NOT `index` LIKE 'strawberry') AND `sourcetype` LIKE 'example:strawberry:strawberry') AND `host` LIKE 'loadbalancer.example.com') OR ((((`index` LIKE '*' AND `host` LIKE 'firewall.example.com') AND (`_time` >= from_unixtime("+earliest+", 'yyyy-MM-dd HH:mm:ss'))) AND (`_time` <= from_unixtime("+latest+", 'yyyy-MM-dd HH:mm:ss'))) AND `_raw` RLIKE '(?i)^.*\\\\QDenied\\\\E.*'))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            ctx.setEarliest("-1Y");
            DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
            CharStream inputStream = CharStreams.fromString(q);
            DPLLexer lexer = new DPLLexer(inputStream);
            DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
            ParseTree tree = parser.root();
            LOGGER.debug(tree.toStringTree(parser));
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPartAsColumn();
            assertEquals(e, result.expr().sql());} catch (ParseException exception) {
            fail(exception.getMessage());
        }
    }


    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void fromStringFullTest() {
        String q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
            long earliest = Assertions.assertDoesNotThrow(() -> tf.getEpoch("04/16/2020:10:25:40"));
            String e = "(RLIKE(index, (?i)^cinnamon$) AND (_time >= from_unixtime(" + earliest + ", yyyy-MM-dd HH:mm:ss)))";
            DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();

            String result = ctx.getSparkQuery();
            // Check logical part
            assertEquals(e, result);
        });
    }


    // index = cinnamon _index_earliest="04/16/2020:10:25:40"
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void endToEndTest() {
        this.streamingTestUtil.performDPLTest("index = cinnamon _index_earliest=\"04/16/2020:10:25:40\"", this.testFile, res -> {
            String e = "[_raw: string, _time: string ... 6 more fields]";
            // check schema
            assertEquals(e, res.toString());

            String logicalPart = this.streamingTestUtil.getCtx().getSparkQuery();
            // check column for archive query i.e. only logical part'
            DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
            long indexEarliestEpoch = Assertions.assertDoesNotThrow(() -> tf.getEpoch("04/16/2020:10:25:40"));
            e = "(RLIKE(index, (?i)^cinnamon$) AND (_time >= from_unixtime(" + indexEarliestEpoch + ", yyyy-MM-dd HH:mm:ss)))";
            assertEquals(e, logicalPart);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void endToEnd2Test() {
        // Use this file as the test data
        String testFile = "src/test/resources/subsearchData*.json";

        this.streamingTestUtil.performDPLTest("index=index_A \"(1)(enTIty)\"", testFile, res -> {
            String e = "StructType(StructField(_raw,StringType,true),StructField(_time,StringType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true))";
            String resSchema=res.schema().toString();
            assertEquals(e, resSchema);
            // Check result count
            List<Row> lst = res.collectAsList();
            // check result count
            assertEquals(1,lst.size());

            // get logical part
            String logicalPart = this.streamingTestUtil.getCtx().getSparkQuery();
            e="(RLIKE(index, (?i)^index_A$) AND RLIKE(_raw, (?i)^.*\\Q(1)(enTIty)\\E.*))";
            assertEquals(e, logicalPart);
        });
    }

    // Check that is AggregatesUsed returns false
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void endToEnd6Test() {
        this.streamingTestUtil.performDPLTest("index = jla02logger ", this.testFile, res -> {
            boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
            assertFalse(aggregates);
        });
    }


    // Check that issue#179 returns user friendly error message
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void searchQualifierMissingRightSide_Issue179_Test() {
        // assert user-friendly exception
        RuntimeException thrown =
            this.streamingTestUtil.performThrowingDPLTest(RuntimeException.class, "index = ", this.testFile, res -> {});

        Assertions.assertEquals("The right side of the search qualifier was empty! Check that the index has a valid value, like 'index = cinnamon'.", thrown.getMessage());
    }

    @Disabled(value="Disabled because the test needs to be converted to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseWhereXmlTest() {
        String q, e;
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70";

        DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
        long earliest = Assertions.assertDoesNotThrow(() -> tf.getEpoch("04/16/2020:10:25:40"));

        e = "<root><!--index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"cinnamon\"/><index_earliest operation=\"GE\" value=\"" + earliest + "\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"count\" function=\"count\" type=\"aggregate\"><transform><where><evalCompareStatement field=\"count\" operation=\"GT\" value=\"70\"/></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        String xml = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        assertEquals(e, xml);
    }

    @Disabled(value="Disabled because the test needs to be converted to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseWhereXml1Test() {
        String q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75";

        DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
        long earliest = Assertions.assertDoesNotThrow(() -> tf.getEpoch("04/16/2020:10:25:40"));
        String e = "<root><!--index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"cinnamon\"/><index_earliest operation=\"GE\" value=\"" + earliest + "\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"count\" function=\"count\" type=\"aggregate\"><transform><where><AND><evalCompareStatement field=\"count\" operation=\"GT\" value=\"70\"/><evalCompareStatement field=\"count\" operation=\"LT\" value=\"75\"/></AND></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        String xml = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        assertEquals(e, xml);
    }

    @Disabled(value="Disabled because the test needs to be converted to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseWhereXml2Test() {
        String q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75 | where count = 72";

        DPLTimeFormat tf = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss");
        long earliest = Assertions.assertDoesNotThrow(() -> tf.getEpoch("04/16/2020:10:25:40"));
        String e = "<root><!--index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75 | where count = 72--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"cinnamon\"/><index_earliest operation=\"GE\" value=\"" + earliest + "\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"count\" function=\"count\" type=\"aggregate\"><transform><where><AND><evalCompareStatement field=\"count\" operation=\"GT\" value=\"70\"/><evalCompareStatement field=\"count\" operation=\"LT\" value=\"75\"/><transform><where><evalCompareStatement field=\"count\" operation=\"EQUALS\" value=\"72\"/></where></transform></AND></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        String xml = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        assertEquals(e, xml);
    }
}

