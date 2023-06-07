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
import com.teragrep.pth10.ast.TimestampToEpochConversion;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.teragrep.pth10.ast.TimestampToEpochConversion.unixEpochFromString;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class logicalOperationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(logicalOperationTest.class);

	SparkSession spark = null;
	DPLParserCatalystContext ctx = null;
	// Use this file for  dataset initialization
	String testFile = "src/test/resources/xmlWalkerTestData.json";

	@org.junit.jupiter.api.BeforeAll
	void setEnv() {
		spark = SparkSession
				.builder()
				.appName("Teragrep")
				.master("local[2]")
				.config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
				.config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
				.config("spark.sql.session.timeZone", "UTC")
				.getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
		ctx = new DPLParserCatalystContext(spark);
		// initialize test dataset
		SparkSession curSession = spark.newSession();
		Dataset<Row> df = curSession.read().json(testFile);
		ctx.setDs(df);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseDPLTest() throws AnalysisException {
		String q = "index=kafka_topic conn error eka OR toka kolmas";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%conn%' AND _raw LIKE '%error%' AND _raw LIKE '%eka%' OR _raw LIKE '%toka%' AND _raw LIKE '%kolmas%'";
		LOGGER.info("Complex query<" + q + ">");
		String result = utils.getQueryAnalysis(q);
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseDPLCatalystTest() {
		String q = "index=kafka_topic *conn* *error* *eka* OR *toka* *kolmas*";
		String e = "(`index` RLIKE '(?i)^kafka_topic' AND (((`_raw` RLIKE '(?i)^.*\\\\Q*conn*\\\\E.*' AND `_raw` RLIKE '(?i)^.*\\\\Q*error*\\\\E.*') AND (`_raw` RLIKE '(?i)^.*\\\\Q*eka*\\\\E.*' OR `_raw` RLIKE '(?i)^.*\\\\Q*toka*\\\\E.*')) AND `_raw` RLIKE '(?i)^.*\\\\Q*kolmas*\\\\E.*'))";
		Column result;
		try {
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();
			visitor.visit(tree);

			result = visitor.getLogicalPartAsColumn();
			LOGGER.info("Complex query<" + q + ">");
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseOrTest() throws AnalysisException {
		String q = "index=kafka_topic a1 OR a2";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%a1%' OR _raw LIKE '%a2%'";
		LOGGER.info("Test OR query<" + q + ">");
		String result = utils.getQueryAnalysis(q);
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseOrCatalystTest() {
		String q = "index=kafka_topic a1 OR a2";
		String e = "(`index` RLIKE '(?i)^kafka_topic' AND (`_raw` RLIKE '(?i)^.*\\\\Qa1\\\\E.*' OR `_raw` RLIKE '(?i)^.*\\\\Qa2\\\\E.*'))";
		Column result;
		try {
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();
			visitor.visit(tree);

			result = visitor.getLogicalPartAsColumn();
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseIndexInCatalystTest() {
		String q = "index IN ( index_A index_B )";
		String e = "(`index` RLIKE '(?i)^index_a' OR `index` RLIKE '(?i)^index_b')";
		Column result;
		try {
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();
			visitor.visit(tree);

			result = visitor.getLogicalPartAsColumn();
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseAndTest() throws AnalysisException {
		String q = "index=kafka_topic a1 AND a2";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%a1%' AND _raw LIKE '%a2%'";
		String result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseAndCatalystTest() {
		String q = "index=kafka_topic a1 AND a2";
		String e = "(`index` RLIKE '(?i)^kafka_topic' AND (`_raw` RLIKE '(?i)^.*\\\\Qa1\\\\E.*' AND `_raw` RLIKE '(?i)^.*\\\\Qa2\\\\E.*'))";
		Column result;
		try {
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();
			visitor.visit(tree);

			result = visitor.getLogicalPartAsColumn();
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseRawUUIDCatalystTest() {
		String q = "index=abc sourcetype=\"cd:ef:gh:0\"  \"1848c85bfe2c4323955dd5469f18baf6\"";
		String e = "(`index` RLIKE '(?i)^abc' AND (`sourcetype` RLIKE '(?i)^cd:ef:gh:0' AND `_raw` RLIKE '(?i)^.*\\\\Q1848c85bfe2c4323955dd5469f18baf6\\\\E.*'))";
		Column result;
		try {
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();
			LOGGER.debug(tree.toStringTree(parser));
			// Use this file for  dataset initialization
			String testFile = "src/test/resources/uuidTestData.json";
			Dataset<Row> inDs = spark.read().json(testFile);
			ctx.setDs(inDs);
			ctx.setEarliest("-1Y");
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
			CatalystNode n = (CatalystNode) visitor.visit(tree);
			result = visitor.getLogicalPartAsColumn();
			LOGGER.info("logicalStatement="+result.expr().sql());
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
			Dataset<Row> res = n.getDataset();
			// Get raw field and check results. Should be only 1 match
			Dataset<Row> selected = res.select("_raw");
			//selected.show(false);
			List<Row> lst = selected.collectAsList();
			// check result count
			assertEquals(3,lst.size(),visitor.getTraceBuffer().toString());
			// Compare values
			assertEquals("uuid=1848c85bfe2c4323955dd5469f18baf6  computer01.example.com",lst.get(0).getString(0),visitor.getTraceBuffer().toString());
			assertEquals("uuid=1848c85bfe2c4323955dd5469f18baf6666  computer01.example.com",lst.get(1).getString(0),visitor.getTraceBuffer().toString());
			assertEquals("uuid=*!<1848c85bFE2c4323955dd5469f18baf6<  computer01.example.com",lst.get(2).getString(0),visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseWithQuotesInsideQuotesCatalystTest() {
		String q = "index=abc \"\\\"latitude\\\": -89.875, \\\"longitude\\\": 24.125\"";
		String e = "(`index` RLIKE '(?i)^abc' AND `_raw` RLIKE '(?i)^.*\\\\Q\"latitude\": -89.875, \"longitude\": 24.125\\\\E.*')";
		Column result;
		try {
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();

			//LOGGER.debug(TreeUtils.toPrettyTree(tree, Arrays.asList(parser.getRuleNames())));
			//LOGGER.debug(tree.toStringTree(parser));

			// Use this file for  dataset initialization
			String testFile = "src/test/resources/latitudeTestData.json";
			Dataset<Row> inDs = spark.read().json(testFile);
			//inDs.show(false);
			ctx.setDs(inDs);
			ctx.setEarliest("-5Y");
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
 			CatalystNode n = (CatalystNode) visitor.visit(tree);
			result = visitor.getLogicalPartAsColumn();
			LOGGER.info("logicalStatement="+result.expr().sql());
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
            Dataset<Row> res = n.getDataset();
			// Get raw field and check results. Should be only 1 match
			Dataset<Row> selected = res.select("_raw");
            //selected.show(false);
			List<Row> lst = selected.collectAsList();
			// check result count
			assertEquals(2, lst.size(), visitor.getTraceBuffer().toString());
			// Compare values
			assertEquals("\"latitude\": -89.875, \"longitude\": 24.125", lst.get(0).getString(0), visitor.getTraceBuffer().toString());
			assertEquals("\"latitude\": -89.875, \"longitude\": 24.125", lst.get(1).getString(0), visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseAnd1Test() throws AnalysisException {
		String q,e,result;
		q = "index=kafka_topic a1 a2";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%a1%' AND _raw LIKE '%a2%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseMultipleParenthesisTest() throws AnalysisException {
		String q = "index=kafka_topic conn ( ( error AND toka) OR kolmas )";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%conn%' AND ((_raw LIKE '%error%' AND _raw LIKE '%toka%') OR _raw LIKE '%kolmas%')";
		String result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseParenthesisWithOrTest() throws AnalysisException {
		String q,e,result;
		q = "index=kafka_topic conn AND ( ( error AND toka ) OR ( kolmas AND n4 ))";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%conn%' AND ((_raw LIKE '%error%' AND _raw LIKE '%toka%') OR (_raw LIKE '%kolmas%' AND _raw LIKE '%n4%'))";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseSimpleParenthesisTest() throws AnalysisException {
		String q,e,result;
		q = "index=kafka_topic ( conn )";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND (_raw LIKE '%conn%')";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}
   
	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseHostTest() throws AnalysisException {
		String q = "index = archive_memory host = \"localhost\" Deny";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND _raw LIKE '%Deny%'";
		String result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}
	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseHost1Test() throws AnalysisException {
        String q,e,result;
		q = "index = archive_memory ( host = \"localhost\" OR host = \"test\" ) AND sourcetype = \"memory\" Deny";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND (host LIKE \"localhost\" OR host LIKE \"test\") AND sourcetype LIKE \"memory\" AND _raw LIKE '%Deny%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseHost2Test() throws AnalysisException {
        String q,e,result;
		q = "index = archive_memory host = \"localhost\" host = \"test\" host = \"test1\" Deny";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND host LIKE \"test\" AND host LIKE \"test1\" AND _raw LIKE '%Deny%'";
		result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseHost3Test() throws AnalysisException {
        String q,e,result;
		// missing AND in query
		q = "index = archive_memory host = \"localhost\" host = \"test\" Deny";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND host LIKE \"test\" AND _raw LIKE '%Deny%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseHost4Test() throws AnalysisException {
        String q,e,result;
		q = "index = archive_memory host = \"localhost\" host = \"test\" host = \"test1\" Deny";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND host LIKE \"test\" AND host LIKE \"test1\" AND _raw LIKE '%Deny%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseHost5Test() throws AnalysisException {
        String q,e,result;
		// Same but missing AND in query		
		q = "index = archive_memory host = \"one\" host = \"two\" host = \"tree\" number";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"one\" AND host LIKE \"two\" AND host LIKE \"tree\" AND _raw LIKE '%number%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);		
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void streamListWithoutQuotesTest() throws AnalysisException  {
		String q,e,result;
		q = "index = memory-test latest=\"05/10/2022:09:11:40\" host= sc-99-99-14-25 sourcetype= log:f17:0 Latitude";
		long latestEpoch = unixEpochFromString("05/10/2022:09:11:40", null);
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory-test\" AND _time <= from_unixtime("+latestEpoch+") AND host LIKE \"sc-99-99-14-25\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void streamListWithoutQuotes1Test() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index = MEMORY-test latest=\"05/10/2022:09:11:40\" host= SC-99-99-14-20 sourcetype= LOG:F17:0 Latitude";
		long latestEpoch2 = unixEpochFromString("05/10/2022:09:11:40", null);
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory-test\" AND _time <= from_unixtime("+latestEpoch2+") AND host LIKE \"sc-99-99-14-20\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalNotTest() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=f17 sourcetype=log:f17:0 _index_earliest=\"12/31/1970:10:15:30\" _index_latest=\"12/31/2022:10:15:30\" NOT rainfall_rate";
		long earliestEpoch = unixEpochFromString("12/31/1970:10:15:30", null);
		long latestEpoch = unixEpochFromString("12/31/2022:10:15:30", null);
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND sourcetype LIKE \"log:f17:0\" AND _time >= from_unixtime("+earliestEpoch+") AND _time <= from_unixtime("+latestEpoch+") AND NOT _raw LIKE '%rainfall_rate%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}



	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalNot1Test() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=cpu sourcetype=log:cpu:0 NOT src";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cpu\" AND sourcetype LIKE \"log:cpu:0\" AND NOT _raw LIKE '%src%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}



	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalQuotedCompoundTest() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=f17 \"ei yhdys sana\"";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%ei yhdys sana%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalUnQuotedCompoundTest() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=f17 ei yhdys sana";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%ei%' AND _raw LIKE '%yhdys%' AND _raw LIKE '%sana%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalUnQuotedCompound2Test() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=f17 ei AND yhdys sana";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%ei%' AND _raw LIKE '%yhdys%' AND _raw LIKE '%sana%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalQuotedIntTest() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=f17 \"1.2\"";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%1.2%'";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalUnQuotedIntTest() throws AnalysisException {
		String q,e,result;
		// Test to_lower() for inex,host,sourcetyype
		q = "index=f17 1.2";
		e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%1.2%'";
		result = utils.getQueryAnalysis(q);
		//LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void parseLikeWithParenthesisCatalystTest() {
		String q = "index=access_log earliest=\"01/21/2022:10:00:00\" latest=\"01/21/2022:11:59:59\" \"*(3)www(9)microsoft(3)com(0)*\" OR \"*(4)mail(6)google(3)com(0)*\"";
		//long indexEarliestEpoch = TimestampToEpochConversion.unixEpochFromString("01/21/2022:10:00:00", null);
		//long indexLatestEpoch = TimestampToEpochConversion.unixEpochFromString("01/21/2022:11:59:59", null);
		String e = "(`index` RLIKE '(?i)^access_log' AND (((`_time` >= from_unixtime(1642752000, 'yyyy-MM-dd HH:mm:ss')) AND (`_time` <= from_unixtime(1642759199, 'yyyy-MM-dd HH:mm:ss'))) AND (`_raw` RLIKE '(?i)^.*\\\\Q*(3)www(9)microsoft(3)com(0)*\\\\E.*' OR `_raw` RLIKE '(?i)^.*\\\\Q*(4)mail(6)google(3)com(0)*\\\\E.*')))";
		Column result;
		try {
			DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
			CharStream inputStream = CharStreams.fromString(q);
			DPLLexer lexer = new DPLLexer(inputStream);
			DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
			ParseTree tree = parser.root();
			visitor.visit(tree);

			result = visitor.getLogicalPartAsColumn();
			assertEquals(e, result.expr().sql(),visitor.getTraceBuffer().toString());
		} catch (Exception ex){
			ex.printStackTrace();
		}
	}
}
