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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class fieldTransformationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(fieldTransformationTest.class);

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
				.getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
		ctx = new DPLParserCatalystContext(spark);
	}

	@org.junit.jupiter.api.BeforeEach
	void setUp() {
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
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseFieldsTransformTest() throws Exception {		
		String q = "index=voyager | fields meta.*";
		String e = "SELECT meta.* FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" )";
		String result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void parseFieldsTransformCatTest() {
		String q = "index=index_B | fields _time";
		ctx.setEarliest("-1Y");
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		CatalystNode n = (CatalystNode) visitor.visit(tree);
		Dataset<Row> res = n.getDataset();
		LOGGER.info("Results after query" + res.toString());
		LOGGER.info("-------------------");
		// check that we ger only _time-column
		res.show();
		assertEquals("[_time: string]", res.toString(), visitor.getTraceBuffer().toString());
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void parseFieldsTransformCat2Test() {
		String q = "index=index_B | fields _time host";
		ctx.setEarliest("-1Y");
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		CatalystNode n = (CatalystNode) visitor.visit(tree);
		Dataset<Row> res = n.getDataset();
		LOGGER.info("Results after query" + res.toString());
		LOGGER.info("-------------------");
		// check that we ger only _time-column
		res.show();
		assertEquals("[_time: string, host: string]", res.toString(), visitor.getTraceBuffer().toString());
	}

	/*
	  _raw, _time, host, index, offset, partition, source, sourcetype
	 */
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void parseFieldsTransformCatDropTest() {
		String q = "index=index_B | fields - host";
		ctx.setEarliest("-1Y");
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		CatalystNode n = (CatalystNode) visitor.visit(tree);
		Dataset<Row> res = n.getDataset();
		LOGGER.info("Results after query" + res.toString());
		LOGGER.info("-------------------");
		// check that we drop only host-column
		res.show();
		String schema = res.schema().toString();
		assertEquals("StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true))", schema, visitor.getTraceBuffer().toString());
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void parseFieldsTransformCatDropSeveralTest() {
		String q = "index=index_B | fields - host index partition";
		ctx.setEarliest("-1Y");
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		CatalystNode n = (CatalystNode) visitor.visit(tree);
		Dataset<Row> res = n.getDataset();
		LOGGER.info("Results after query" + res.toString());
		LOGGER.info("-------------------");
		// check that we get _raw,_time, offset,source, sourcetype
		res.show();
		String schema = res.schema().toString();
		assertEquals("StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(offset,LongType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true))", schema, visitor.getTraceBuffer().toString());
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseFieldsTransform1Test() throws Exception {		
		String q,e,result;
		q = "index=voyager Denied | fields meta.*,_raw";
		e = "SELECT meta.*,_raw FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _raw LIKE '%Denied%' )";
		result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseFieldsTransform2Test() throws Exception {		
		String q,e,result;

		q = "index=voyager Denied Port | fields meta.*,_raw";
		e = "SELECT meta.*,_raw FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _raw LIKE '%Denied%' AND _raw LIKE '%Port%' )";
		result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseFieldsTransformAddTest() throws Exception {		
		String q,e,result;

		q = "index=voyager Denied Port | fields + meta.*,_raw";
		e = "SELECT meta.*,_raw FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _raw LIKE '%Denied%' AND _raw LIKE '%Port%' )";
		result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseFieldsTransformDropTest() throws Exception {		
		String q,e,result;
		q = "index=voyager Denied Port | fields - meta.*, _raw";
		e = "SELECT DROPFIELDS(meta.*,_raw) FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _raw LIKE '%Denied%' AND _raw LIKE '%Port%' )";
		result = utils.getQueryAnalysis(q);
		LOGGER.info("DPL      =<" + q + ">");
		utils.printDebug(e,result);
		assertEquals(e,result);
	}

}
