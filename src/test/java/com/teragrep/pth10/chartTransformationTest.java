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
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static com.teragrep.pth10.ast.TimestampToEpochConversion.unixEpochFromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class chartTransformationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(chartTransformationTest.class);

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
        spark.sparkContext().setLogLevel("ERROR");
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

	@Disabled
	@Test
	public void parseTest() throws Exception {

		File testFile = new File("src/test/resources/antlr4/mytest.txt");

		String[] args = new String[] { "com.teragrep.pth_03.antlr.DPL", "root", "-tree",
				testFile.getAbsoluteFile().toString() };

		LOGGER.info("Show parse-tree test");

		//TestRig chartRig = new TestRig(args);
		//chartRig.process();
	}

	@Disabled
	@Test
	public void parseTimeformatTreeTest() throws Exception {

		File testFile = new File("src/test/resources/antlr4/timeformat.txt");

		String[] args = new String[] { "com.teragrep.pth_03.antlr.DPL", "root", "-tree",
				testFile.getAbsoluteFile().toString() };

		LOGGER.info("Show parse-timeformat tree");

		//TestRig chartRig = new TestRig(args);
		//chartRig.process();
	}


	// transformation operation test for count
	// index =* |chart count(_raw) by host
	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartcountTest() throws AnalysisException {
		String q = "index = voyager | chart count(_raw) as count";
		String e = "SELECT count(_raw) AS count FROM `temporaryDPLView` WHERE index LIKE \"voyager\"";
		String result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountColumNameTest() throws AnalysisException {
		String q,e,result;
		// Define column name for count
		q = "index=voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw_) as cnt";
		long indexEarliestEpoch = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT count(_raw_) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch+")";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountWithLogicalOperationAndColumNameTest() throws AnalysisException {
		String q,e,result;
		// logical AND-part and named column
		q = "index=voyager _index_earliest=\"04/16/2020:10:25:40\" _index_latest=\"04/16/2020:10:25:42\" | chart count(_raw) as count by timestamp";
		long indexEarliestEpoch2 = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		long indexLatestEpoch2 = unixEpochFromString("04/16/2020:10:25:42", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT timestamp,count(_raw) AS count FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch2+") AND _time <= from_unixtime("+indexLatestEpoch2+") GROUP BY timestamp";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountDefaultNameTest() throws AnalysisException {
		String q,e,result;
		// Test autogenerated column names
		q = "index = voyager | chart count(_raw) by host";
		e = "SELECT host,count(_raw) AS `count(_raw)` FROM `temporaryDPLView` WHERE index LIKE \"voyager\" GROUP BY host";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountDefaultName1Test() throws AnalysisException {
		String q,e,result;
		q = "index=voyager _index_earliest=\"04/16/2020:10:25:40\" _index_latest=\"04/16/2020:10:25:42\" | chart count(_raw)";
		long earliestEpoch = unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		long latestEpoch = unixEpochFromString("04/16/2020:10:25:42", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT count(_raw) AS `count(_raw)` FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+earliestEpoch+") AND _time <= from_unixtime("+latestEpoch+")";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled
	@Test
	public void parseChainedTransformationTest() {
		String q = "index=fs_mon host=\"$form.host$\" sourcetype=\"fs:mon-01:pid-cpu:0\" earliest=\"09/24/2018:00:00:00\" latest=\"9/24/2018:04:00:00\"  | where 'usr-ms'!=\"184467440737095516160\" | where 'system-ms'!=\"184467440737095516160\" | eval ProcessWithPID=Command+\"@\"+PID | timechart useother=f sum(usr-ms) by ProcessWithPID";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND GROUP BY ProcessWithPID";
		String result = null;
		assertEquals(e,result,q);
	}


	@Disabled
	@Test
	public void streamListWithTimeLimitsTest() {
		String q = "earliest=-24h AND ( index = haproxy AND (sourcetype = \"example:haproxy:haproxy\" ) AND ( host = \"loadbalancer.example.com\" ) OR ( index = * AND host = \"firewall.example.com\" AND earliest = -90d Denied))";
		String e = "SELECT * FROM `haproxy` WHERE index LIKE \"haproxy\" AND _time >= from_unixtime(1618144982) AND ( AND (sourcetype LIKE \"example:haproxy:haproxy\") AND (host LIKE \"loadbalancer.example.com\") OR ( AND host LIKE \"firewall.example.com\" AND timestamp >= from_unixtime(1610455382) AND _raw LIKE '%Denied%'))";
		String result = null;
		assertEquals(e,result,q);
	}


	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void countParsingTest() throws AnalysisException {
		String q,e,result;
		q = "index=cpu sourcetype=log:cpu:0 (host=sc-99-99-11-48 OR host=sc-99-99-13-164) | chart count(_raw) as cnt by host";
	    e = "SELECT host,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cpu\" AND sourcetype LIKE \"log:cpu:0\" AND (host LIKE \"sc-99-99-11-48\" OR host LIKE \"sc-99-99-13-164\") GROUP BY host";
		result = utils.getQueryAnalysis(q);
		assertEquals(e, result,q);
	}

//	@Test
	public void countXmlParsingTest() throws AnalysisException {
		String q,e,result;
		q = "index=cpu sourcetype=log:cpu:0 (host=sc-99-99-11-48 OR host=sc-99-99-13-164) | chart count(_raw) as cnt by host";
	    e = "<root><!--index=cpu sourcetype=log:cpu:0 (host=sc-99-99-11-48 OR host=sc-99-99-13-164) | chart count(_raw) as cnt by host--><search root=\"true\"><logicalStatement><AND><AND><index operation=\"EQUALS\" value=\"cpu\"/><sourcetype operation=\"EQUALS\" value=\"log:cpu:0\"/></AND><OR><host operation=\"EQUALS\" value=\"sc-99-99-11-48\"/><host operation=\"EQUALS\" value=\"sc-99-99-13-164\"/></OR></AND><transformStatements><transform><divideBy field=\"host\"><chart field=\"_raw\" fieldRename=\"cnt\" function=\"count\"/></divideBy></transform></transformStatements></logicalStatement></search></root>";
		result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e, result);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void endToEnd2Test() {
		String q, e;
		// First parse incoming DPL
		q = "( index = index_A OR index = index_B ) _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count by _time";
		e = "[_time: string, count: bigint]"; // At least schema is correct
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

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
			assertEquals(e, res.toString());
			// Check full result
			e = "+---------------------------------------------------+\n" +
					"|value                                              |\n" +
					"+---------------------------------------------------+\n" +
					"|{\"_time\":\"2009-09-09T09:09:09.090+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2010-10-10T10:10:10.100+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2008-08-08T08:08:08.080+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2007-07-07T07:07:07.070+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2005-05-05T05:05:05.050+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2004-04-04T04:04:04.040+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2006-06-06T06:06:06.060+03:00\",\"count\":1}|\n" +
					"+---------------------------------------------------+\n";

			String jsonStr = res.toJSON().showString(7, 0, false);
			assertEquals(e, jsonStr);
			res.printSchema();
			boolean aggregates = visitor.getAggregatesUsed();
			assertTrue(aggregates);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void endToEnd3Test() {
		String q, e;
		// First parse incoming DPL
		q = "index = index_A _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count by _time | where count > 0";
		e = "StructType(StructField(_time,StringType,true), StructField(count,LongType,true))"; // At least schema is correct
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

		DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
		// Use this file for  dataset initialization
		String testFile = "src/test/resources/xmlWalkerTestData.json";
		Dataset<Row> inDs = spark.read().json(testFile);
		ctx.setDs(inDs);
		ctx.setEarliest("-1Y");
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		try {
			CatalystNode n = (CatalystNode) visitor.visit(tree);
			LOGGER.info("logicalPart="+visitor.getLogicalPart());
			Dataset<Row> res = n.getDataset();
//			res.show();
			String schema=res.schema().toString();
			// check that incoming stream schema is right
			assertEquals(e, schema);
			// Check full result
			e = "+---------------------------------------------------+\n" +
					"|value                                              |\n" +
					"+---------------------------------------------------+\n" +
					"|{\"_time\":\"2005-05-05T05:05:05.050+03:00\",\"count\":1}|\n" +
					"|{\"_time\":\"2004-04-04T04:04:04.040+03:00\",\"count\":1}|\n" +
					"+---------------------------------------------------+\n";
			String jsonStr = res.toJSON().showString(2, 0, false);
			assertEquals(e, jsonStr);

		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void endToEnd4Test() {
		String q, e;
		// First parse incoming DPL
		q = "index = index_A _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw)";
//		q = "index = index_A _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count";
		e = "[count(_raw): bigint]"; // At least schema is correct
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

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
			LOGGER.info("Results after query");
			LOGGER.info("-------------------");
			res.show();
			res.printSchema();
			res.orderBy("count(_raw)").show();
			LOGGER.info(" Parsed logicalPart:" + visitor.getLogicalPart());
			assertEquals(e, res.toString());
			// Check full result
			e = "+-----------------+\n"
					+ "|value            |\n"
					+ "+-----------------+\n"
					+ "|{\"count(_raw)\":2}|\n"
					+ "+-----------------+\n";
			String jsonStr = res.toJSON().showString(7, 0, false);
			assertEquals(e, jsonStr);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
	// multiple chart aggregations
	// specifically for issue #184
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void chart_multipleAggs_issue184_Test() {
		String q, e;
		// First parse incoming DPL
		q = "index = index_A | chart count(_raw), min(offset), max(offset) by sourcetype";
		
		final StructType expectedSchema = new StructType(
				new StructField[] {
						new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
						new StructField("count(_raw)", DataTypes.LongType, true, new MetadataBuilder().build()),
						new StructField("min(offset)", DataTypes.StringType, true, new MetadataBuilder().build()),
						new StructField("max(offset)", DataTypes.StringType, true, new MetadataBuilder().build())
				}
		);

		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

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
			LOGGER.info("Results after query");
			LOGGER.info("-------------------");
			res.show();
			LOGGER.info(" Parsed logicalPart:" + visitor.getLogicalPart());
			
			// assert schema
			assertEquals(expectedSchema, res.schema());
			
			// assert contents
			List<Row> count = res.select("count(_raw)").collectAsList();
			List<Row> min = res.select("min(offset)").collectAsList();
			List<Row> max = res.select("max(offset)").collectAsList();
			
			Row cr = count.get(0);
			Row minr = min.get(0);
			Row maxr = max.get(0);

			Row cr2 = count.get(1);
			Row minr2 = min.get(1);
			Row maxr2 = max.get(1);

			assertEquals("3",cr.getAs(0).toString());
			assertEquals("3",minr.getAs(0).toString());
			assertEquals("5",maxr.getAs(0).toString());
			
			assertEquals("2",cr2.getAs(0).toString());
			assertEquals("1",minr2.getAs(0).toString());
			assertEquals("2",maxr2.getAs(0).toString());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	
	// Check that is AggregatesUsed returns true
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void endToEnd5Test() {
		String q;
		// First parse incoming DPL
		// timechart span=1m count(_raw) by host
		q = "index = jla02logger | chart count(_raw)";
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

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
			assertTrue(aggregates);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
	@Disabled
	@Test
	void endToEnd7Test() {
		String q;
		// First parse incoming DPL
		// check that timechart returns also aggregatesUsed=true
		q = "index = jla02logger | timechart span=1m count(_raw) by host";
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

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
			assertTrue(aggregates,visitor.getTraceBuffer().toString());
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

}