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
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.teragrep.pth10.ast.TimestampToEpochConversion.unixEpochFromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalystVisitorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalystVisitorTest.class);

    DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor catalystVisitor;
    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestData.json";
    SparkSession spark = null;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
//        spark.sparkContext().setLogLevel("ERROR");
        ctx = new DPLParserCatalystContext(spark);
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
//        ctx = new DPLParserCatalystContext(spark);
        // initialize test dataset
        SparkSession curSession = spark.newSession();
        Dataset<Row> df = curSession.read().json(testFile);
        ctx.setDs(df);
    }


    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        catalystVisitor = null;
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void fromStringNot2Test() {
        String q, e;
        Column result;
        q = "index = \"cpu\" AND sourcetype = \"log:cpu:0\" NOT src";
//        e = "((`index` LIKE 'cpu' AND `sourcetype` LIKE 'log:cpu:0') AND (NOT `_raw` LIKE '%src%'))";
        e = "(`index` RLIKE '(?i)^cpu' AND (`sourcetype` RLIKE '(?i)^log:cpu:0' AND (NOT `_raw` RLIKE '(?i)^.*\\\\Qsrc\\\\E.*')))";

        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        visitor.visit(tree);
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());
    }

    @Disabled
	@Test
    void columnFromStringTest() {
        String q, e;
        Column result;
        // Add time ranges
        q = "<OR><AND><AND><index value=\"haproxy\" operation=\"NOT_EQUALS\"/><sourcetype value=\"example:haproxy:haproxy\" operation=\"EQUALS\"/></AND><host value=\"loadbalancer.example.com\" operation=\"EQUALS\"/></AND><AND><AND><AND><AND><index value=\"*\" operation=\"EQUALS\"/><host value=\"firewall.example.com\" operation=\"EQUALS\"/></AND><earliest value=\"1611657303\" operation=\"GE\"/></AND><latest value=\"1619437701\" operation=\"LE\"/></AND><indexstring value=\"Denied\" /></AND></OR>";
        e = "((((NOT (`index` = 'haproxy')) AND (`sourcetype` = 'example:haproxy:haproxy')) AND (`host` = 'loadbalancer.example.com')) OR ((((`index` = '*') AND (`host` = 'firewall.example.com')) AND (`_time` >= DATE '2021-01-26')) AND (`_time` <= DATE '2021-04-26')))";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void columnFromStringDateTest() {
        String q, e;
        Column result;
        // Add time ranges
        q = "((( index =\"cpu\" AND host = \"sc-99-99-14-25\" ) AND sourcetype = \"log:cpu:0\" ) AND ( earliest= \"01/01/1970:02:00:00\"  AND latest= \"01/01/2030:00:00:00\" ))";
        long earliestEpoch = unixEpochFromString("01/01/1970:02:00:00", "MM/dd/yyyy:HH:mm:ss");
        long latestEpoch = unixEpochFromString("01/01/2030:00:00:00", "MM/dd/yyyy:HH:mm:ss");

        e = "(((`index` RLIKE '(?i)^cpu' AND `host` RLIKE '(?i)^sc-99-99-14-25') AND `sourcetype` RLIKE '(?i)^log:cpu:0') AND ((`_time` >= from_unixtime(" + earliestEpoch + ", 'yyyy-MM-dd HH:mm:ss')) AND (`_time` <= from_unixtime("+ latestEpoch + ", 'yyyy-MM-dd HH:mm:ss'))))";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
	    LOGGER.debug(tree.toStringTree(parser));
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());
    }

    @Disabled
    // FIXME * to %
    void columnFromStringDate1Test() {
        String q, e;
        Column result;
        q = "((index !=\"haproxy\" AND sourcetype =\"example:haproxy:haproxy\") AND host =\"loadbalancer.example.com\" ) OR  (((index = \"*\"  AND host =\"firewall.example.com\") AND earliest= \"01/01/1970:02:00:00\" ) AND latest= \"01/01/2030:00:00:00\" ) AND Denied";
        long zero = unixEpochFromString("01/01/1970:02:00:00", "MM/dd/yyyy:HH:mm:ss");
        long epoch = unixEpochFromString("01/01/2030:00:00:00", "MM/dd/yyyy:HH:mm:ss");

        e = "((((NOT `index` LIKE 'haproxy') AND `sourcetype` LIKE 'example:haproxy:haproxy') AND `host` LIKE 'loadbalancer.example.com') OR ((((`index` LIKE '*' AND `host` LIKE 'firewall.example.com') AND (`_time` >= from_unixtime("+zero+", 'yyyy-MM-dd HH:mm:ss'))) AND (`_time` <= from_unixtime("+epoch+", 'yyyy-MM-dd HH:mm:ss'))) AND `_raw` RLIKE '(?i)^.*\\\\QDenied\\\\E.*'))";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        Object n = visitor.visit(tree);
        LOGGER.info("String node=" + n.getClass().getName());
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void columnFromStringAndTest() {
        String q, e;
        Column result;
        //LOGGER.info("------ AND ---------");
        q = "index =\"haproxy\" AND sourcetype =\"example:haproxy:haproxy\"";
        e = "(`index` RLIKE '(?i)^haproxy' AND `sourcetype` RLIKE '(?i)^example:haproxy:haproxy')";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        CatalystNode n = (CatalystNode) visitor.visit(tree);

        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void columnFromStringOrTest() {
        String q, e;
        Column result;
        //LOGGER.info("------ OR ---------");
        q = "index != \"haproxy\" OR sourcetype =\"example:haproxy:haproxy\"";
        e = "((NOT `index` RLIKE '(?i)^haproxy') OR `sourcetype` RLIKE '(?i)^example:haproxy:haproxy')";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        ctx.setRuleNames(parser.getRuleNames());
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CatalystNode n = (CatalystNode) visitor.visit(tree);

        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());

    }

    //earliest="05/12/2021:00:00:00" latest="05/12/2021:23:59:59"
    // _time >= from_unixtime(1619395200) AND _time <= from_unixtime(1619481599)>
    @Disabled
    //FIXME * to %
    void filterTest() {
        String q, e;
        Column result;
        q = "((index !=\"haproxy\" AND sourcetype =\"example:haproxy:haproxy\") AND host =\"loadbalancer.example.com\" )  OR ( index =\"*\" AND host = \"firewall.example.com\" ) AND  earliest= \"04/26/2021:07:00:00\"  AND latest= \"04/26/2021:08:00:00\" AND \"Denied\"";
        long earliest = unixEpochFromString("04/26/2021:07:00:00", "MM/dd/yyyy:HH:mm:ss");
        long latest = unixEpochFromString("04/26/2021:08:00:00", "MM/dd/yyyy:HH:mm:ss");
//        e = "((((NOT `index` LIKE 'haproxy') AND `sourcetype` LIKE 'example:haproxy:haproxy') AND `host` LIKE 'loadbalancer.example.com') OR ((((`index` LIKE '*' AND `host` LIKE 'firewall.example.com') AND (`_time` >= from_unixtime("+earliest+", 'yyyy-MM-dd HH:mm:ss'))) AND (`_time` <= from_unixtime("+latest+", 'yyyy-MM-dd HH:mm:ss'))) AND `_raw` LIKE '%Denied%'))";
        e = "((((NOT `index` LIKE 'haproxy') AND `sourcetype` LIKE 'example:haproxy:haproxy') AND `host` LIKE 'loadbalancer.example.com') OR ((((`index` LIKE '*' AND `host` LIKE 'firewall.example.com') AND (`_time` >= from_unixtime("+earliest+", 'yyyy-MM-dd HH:mm:ss'))) AND (`_time` <= from_unixtime("+latest+", 'yyyy-MM-dd HH:mm:ss'))) AND `_raw` RLIKE '(?i)^.*\\\\QDenied\\\\E.*'))";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql());
    }


    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void fromStringFullTest() {
        String q, e;
        Column result;
        q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70";
        long earliest = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
        e = "(`index` RLIKE '(?i)^voyager' AND (`_time` >= from_unixtime(" + earliest + ", 'yyyy-MM-dd HH:mm:ss')))";
        ctx.setEarliest("-1Y");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));
        visitor.visit(tree);

        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        // Check logical part
        assertEquals(e, result.expr().sql());
    }


    // index = voyager _index_earliest="04/16/2020:10:25:40"
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEndTest() {
        String q, e;
        String logicalPart;
        // First parse incoming DPL
        q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\"";
        e = "[_raw: string, _time: string ... 6 more fields]";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/xmlWalkerTestData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            LOGGER.info("Results after query" + res.toString());
            LOGGER.info("-------------------");
            // check schema
            res.show();
            assertEquals(e, res.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

        // get logical part which is used for archive queries
        logicalPart = visitor.getLogicalPart();
        // check column for archive query i.e. only logical part
        long indexEarliestEpoch = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
        e = "(`index` RLIKE '(?i)^voyager' AND (`_time` >= from_unixtime(" + indexEarliestEpoch + ", 'yyyy-MM-dd HH:mm:ss')))";
        LOGGER.info("Query=" + logicalPart);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + logicalPart);
        assertEquals(e, logicalPart);
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEnd2Test() {
        String q, e;
        String logicalPart;
        // First parse incoming DPL
        q = "index = index_A \"(1)(enTIty)\"";
        e = "StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true))";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            // check schema
            //res.show();
            String resSchema=res.schema().toString();
            assertEquals(e, resSchema);
            // Check result count
            List<Row> lst = res.collectAsList();
            // check result count
            assertEquals(1,lst.size());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

        // get logical part
        logicalPart = visitor.getLogicalPart();
        e="(`index` RLIKE '(?i)^index_A' AND `_raw` RLIKE '(?i)^.*\\\\Q(1)(enTIty)\\\\E.*')";
        assertEquals(e, logicalPart);
    }

    // Check that is AggregatesUsed returns false
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEnd6Test() {
        String q;
        // First parse incoming DPL
        q = "index = jla02logger ";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/xmlWalkerTestData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            res.show();
            boolean aggregates = visitor.getAggregatesUsed();
            assertFalse(aggregates);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    
    // Check that issue#179 returns user friendly error message
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void searchQualifierMissingRightSide_Issue179_Test() {
        String q;
        // First parse incoming DPL
        q = "index = ";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
		LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/xmlWalkerTestData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        try {
        	// assert user-friendly exception
            RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () -> {
            	visitor.visit(tree);
            });
            
            Assertions.assertEquals("The right side of the search qualifier was empty! Check that the index has a valid value, like 'index = voyager'.", thrown.getMessage());
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseWhereXmlTest() throws AnalysisException {
        String q, e, result;
        q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70";
        long earliest = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");

        e = "<root><!--index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"voyager\"/><index_earliest operation=\"GE\" value=\""+earliest+"\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"count\" function=\"count\" type=\"aggregate\"><transform><where><evalCompareStatement field=\"count\" operation=\"GT\" value=\"70\"/></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        String xml = utils.getQueryAnalysis(q);
        LOGGER.info("XML =" + xml);
        LOGGER.info("EXP =" + e);
        assertEquals(e, xml);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseWhereXml1Test() throws AnalysisException {
        String q, e, result;
        q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75";
        long indexEarliestEpoch = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
        e = "<root><!--index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"voyager\"/><index_earliest operation=\"GE\" value=\"" + indexEarliestEpoch + "\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"count\" function=\"count\" type=\"aggregate\"><transform><where><AND><evalCompareStatement field=\"count\" operation=\"GT\" value=\"70\"/><evalCompareStatement field=\"count\" operation=\"LT\" value=\"75\"/></AND></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        String xml = utils.getQueryAnalysis(q);
        LOGGER.info("XML =" + xml);
        LOGGER.info("EXP =" + e);
        assertEquals(e, xml);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseWhereXml2Test() throws AnalysisException {
        String q, e, result;
        q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75 | where count = 72";
        long indexEarliestEpoch = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
        e = "<root><!--index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where  count > 70 AND count < 75 | where count = 72--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"voyager\"/><index_earliest operation=\"GE\" value=\"" + indexEarliestEpoch + "\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"count\" function=\"count\" type=\"aggregate\"><transform><where><AND><evalCompareStatement field=\"count\" operation=\"GT\" value=\"70\"/><evalCompareStatement field=\"count\" operation=\"LT\" value=\"75\"/><transform><where><evalCompareStatement field=\"count\" operation=\"EQUALS\" value=\"72\"/></where></transform></AND></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        String xml = utils.getQueryAnalysis(q);
        LOGGER.info("XML =" + xml);
        LOGGER.info("EXP =" + e);
        assertEquals(e, xml);
    }

}

