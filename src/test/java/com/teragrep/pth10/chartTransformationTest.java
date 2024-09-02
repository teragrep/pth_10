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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class chartTransformationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(chartTransformationTest.class);

	String testFile = "src/test/resources/xmlWalkerTestDataStreaming/xmlWalkerTestDataStreaming*";
	SparkSession spark = null;
	StreamingTestUtil streamingTestUtil;

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


	// transformation operation test for count
	// index =* |chart count(_raw) by host
	@Disabled(value="Should be converted to a dataframe test")
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartcountTest() throws AnalysisException {
		String q = "index = cinnamon | chart count(_raw) as count";
		String e = "SELECT count(_raw) AS count FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\"";
		String result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled(value="Should be converted to a dataframe test")
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountColumNameTest() throws AnalysisException {
		String q,e,result;
		// Define column name for count
		q = "index=cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw_) as cnt";

		try {
			long indexEarliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
			e = "SELECT count(_raw_) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("+indexEarliestEpoch+")";
			result = utils.getQueryAnalysis(q);
			assertEquals(e,result);
		} catch (ParseException exception) {
			fail(exception.getMessage());
		}
	}

	@Disabled(value="Should be converted to a dataframe test")
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountWithLogicalOperationAndColumNameTest() throws AnalysisException {
		String q,e,result;
		// logical AND-part and named column
		q = "index=cinnamon _index_earliest=\"04/16/2020:10:25:40\" _index_latest=\"04/16/2020:10:25:42\" | chart count(_raw) as count by timestamp";

		try {
			long indexEarliestEpoch2 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
			long indexLatestEpoch2 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
			e = "SELECT timestamp,count(_raw) AS count FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("+indexEarliestEpoch2+") AND _time <= from_unixtime("+indexLatestEpoch2+") GROUP BY timestamp";
			result = utils.getQueryAnalysis(q);
			assertEquals(e,result);
		} catch (ParseException exception) {
			fail(exception.getMessage());
		}
	}

	@Disabled(value="Should be converted to a dataframe test")
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountDefaultNameTest() throws AnalysisException {
		String q,e,result;
		// Test autogenerated column names
		q = "index = cinnamon | chart count(_raw) by host";
		e = "SELECT host,count(_raw) AS `count(_raw)` FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" GROUP BY host";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result);
	}

	@Disabled(value="Should be converted to a dataframe test")
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void parseChartCountDefaultName1Test() throws AnalysisException {
		String q,e,result;
		q = "index=cinnamon _index_earliest=\"04/16/2020:10:25:40\" _index_latest=\"04/16/2020:10:25:42\" | chart count(_raw)";

		try {
			long earliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
			long latestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:42");
			e = "SELECT count(_raw) AS `count(_raw)` FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("+earliestEpoch+") AND _time <= from_unixtime("+latestEpoch+")";
			result = utils.getQueryAnalysis(q);
			assertEquals(e,result);
		} catch (ParseException exception) {
			fail(exception.getMessage());
		}
	}

	@Disabled(value="Should be converted to a dataframe test")
	@Test
	public void parseChainedTransformationTest() {
		String q = "index=fs_mon host=\"$form.host$\" sourcetype=\"fs:mon-01:pid-cpu:0\" earliest=\"09/24/2018:00:00:00\" latest=\"9/24/2018:04:00:00\"  | where 'usr-ms'!=\"184467440737095516160\" | where 'system-ms'!=\"184467440737095516160\" | eval ProcessWithPID=Command+\"@\"+PID | timechart useother=f sum(usr-ms) by ProcessWithPID";
		String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND GROUP BY ProcessWithPID";
		String result = null;
		assertEquals(e,result,q);
	}


	@Disabled(value="Should be converted to a dataframe test")
	@Test
	public void streamListWithTimeLimitsTest() {
		String q = "earliest=-24h AND ( index = strawberry AND (sourcetype = \"example:strawberry:strawberry\" ) AND ( host = \"loadbalancer.example.com\" ) OR ( index = * AND host = \"firewall.example.com\" AND earliest = -90d Denied))";
		String e = "SELECT * FROM `strawberry` WHERE index LIKE \"strawberry\" AND _time >= from_unixtime(1618144982) AND ( AND (sourcetype LIKE \"example:strawberry:strawberry\") AND (host LIKE \"loadbalancer.example.com\") OR ( AND host LIKE \"firewall.example.com\" AND timestamp >= from_unixtime(1610455382) AND _raw LIKE '%Denied%'))";
		String result = null;
		assertEquals(e,result,q);
	}


	@Disabled(value="Should be converted to a dataframe test")
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
	public void countParsingTest() throws AnalysisException {
		String q,e,result;
		q = "index=cpu sourcetype=log:cpu:0 (host=sc-99-99-11-48 OR host=sc-99-99-13-164) | chart count(_raw) as cnt by host";
	    e = "SELECT host,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cpu\" AND sourcetype LIKE \"log:cpu:0\" AND (host LIKE \"sc-99-99-11-48\" OR host LIKE \"sc-99-99-13-164\") GROUP BY host";
		result = utils.getQueryAnalysis(q);
		assertEquals(e, result,q);
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void endToEnd2Test() {
		String q = "( index = index_A OR index = index_B ) _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count by offset";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {

			String e = "[offset: bigint, count: bigint]"; // At least schema is correct
			assertEquals(e, res.toString());

			// 3 first rows are earlier than where _index_earliest is set to
			List<String> expectedValues = new ArrayList<>();
			for (int i = 4; i < 11; i++) {
				expectedValues.add(i + ",1");
			}

			List<String> resultList = res.collectAsList().stream().map(r -> r.mkString(",")).collect(Collectors.toList());

			// sort the lists, as the order of rows doesn't matter with this aggregation
			Collections.sort(expectedValues);
			Collections.sort(resultList);

			assertEquals(expectedValues, resultList);
			boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
			assertTrue(aggregates);
		});
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void endToEnd3Test() {
		String q = "index = index_A _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw) as count by offset | where count > 0";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {

			String e = "[offset: bigint, count: bigint]"; // At least schema is correct
			assertEquals(e, res.toString());

			List<String> expectedValues = new ArrayList<>();
			// Only first 5 rows have index: index_A
			// and only the latter 2 have _time after index_earliest
			expectedValues.add(4 + ",1");
			expectedValues.add(5 + ",1");

			List<String> resultList = res.collectAsList().stream().map(r -> r.mkString(",")).collect(Collectors.toList());

			// sort the lists, as the order of rows doesn't matter with this aggregation
			Collections.sort(expectedValues);
			Collections.sort(resultList);

			assertEquals(expectedValues, resultList);
			res.printSchema();
			boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
			assertTrue(aggregates);
		});
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void endToEnd4Test() {
		String q = "index = index_B _index_earliest=\"04/16/2003:10:25:40\" | chart count(_raw)";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {

			String e = "[count(_raw): bigint]"; // At least schema is correct
			assertEquals(e, res.toString());

			List<String> expectedValues = new ArrayList<>();
			expectedValues.add("5"); // only last 5 rows have index: index_B

			List<String> resultList = res.collectAsList().stream().map(r -> r.mkString(",")).collect(Collectors.toList());

			// sort the lists, as the order of rows doesn't matter with this aggregation
			Collections.sort(expectedValues);
			Collections.sort(resultList);

			assertEquals(expectedValues, resultList);
			res.printSchema();
			boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
			assertTrue(aggregates);
		});
	}
	
	// multiple chart aggregations
	// specifically for issue #184
	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void chart_multipleAggs_issue184_Test() {
		String q = "index=* | chart count(_raw), min(offset), max(offset) by index";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
			final StructType expectedSchema = new StructType(
					new StructField[] {
							new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
							new StructField("count(_raw)", DataTypes.LongType, true, new MetadataBuilder().build()),
							new StructField("min(offset)", DataTypes.StringType, true, new MetadataBuilder().build()),
							new StructField("max(offset)", DataTypes.StringType, true, new MetadataBuilder().build())
					}
			);

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

			assertEquals("5",cr.getAs(0).toString());
			assertEquals("1",minr.getAs(0).toString());
			assertEquals("5",maxr.getAs(0).toString());

			assertEquals("5",cr2.getAs(0).toString());
			assertEquals("6",minr2.getAs(0).toString());
			assertEquals("10",maxr2.getAs(0).toString());
		});
	}
	
	// Check that is AggregatesUsed returns true
	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void endToEnd5Test() {
		String q = "index = jla02logger | chart count(_raw)";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
			boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
			assertTrue(aggregates);
		});
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void testSplittingByTime() {
		String q = "index=* | chart avg(offset) by _time";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
			final StructType expectedSchema = new StructType(
					new StructField[] {
							new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
							new StructField("avg(offset)", DataTypes.DoubleType, true, new MetadataBuilder().build()),
					}
			);

			assertEquals(expectedSchema, res.schema());

			List<Row> time = res.select("_time").collectAsList();
			List<Row> offset = res.select("avg(offset)").collectAsList();

			System.out.println(time.stream().map(r -> r.getAs(0).toString()).toArray());

			// assert correct ordering, old to new
			String[] expectedTime = new String[]{
				"2001-01-01T01:01:01.010+03:00", "2002-02-02T02:02:02.020+03:00",
				"2003-03-03T03:03:03.030+03:00", "2004-04-04T04:04:04.040+03:00",
				"2005-05-05T05:05:05.050+03:00", "2006-06-06T06:06:06.060+03:00",
				"2007-07-07T07:07:07.070+03:00", "2008-08-08T08:08:08.080+03:00",
				"2009-09-09T09:09:09.090+03:00", "2010-10-10T10:10:10.100+03:00"
			};
			String[] expectedOffset = new String[]{ "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0", "10.0" };

			assertArrayEquals(expectedTime, time.stream().map(r -> r.getAs(0).toString()).toArray());
			assertArrayEquals(expectedOffset, offset.stream().map(r -> r.getAs(0).toString()).toArray());
		});
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void testSplittingByString() {
		String q = "index=* | chart avg(offset) by sourcetype";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
			final StructType expectedSchema = new StructType(
					new StructField[] {
							new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
							new StructField("avg(offset)", DataTypes.DoubleType, true, new MetadataBuilder().build()),
					}
			);

			assertEquals(expectedSchema, res.schema());

			List<Row> sourcetype = res.select("sourcetype").collectAsList();
			List<Row> offset = res.select("avg(offset)").collectAsList();

			// ascending ordering for strings
			String[] expectedSourcetype = new String[]{"A:X:0", "A:Y:0", "B:X:0", "B:Y:0"};
			String[] expectedOffset = new String[]{"1.5", "4.0", "7.0", "9.5"};

			assertArrayEquals(expectedSourcetype, sourcetype.stream().map(r -> r.getAs(0).toString()).toArray());
			assertArrayEquals(expectedOffset, offset.stream().map(r -> r.getAs(0).toString()).toArray());
		});
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void testSplittingByNumber() {
		String q = "index=* | chart count(offset) by offset";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
			final StructType expectedSchema = new StructType(
					new StructField[] {
							new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
							new StructField("count(offset)", DataTypes.LongType, true, new MetadataBuilder().build()),
					}
			);

			assertEquals(expectedSchema, res.schema());

			List<Row> offset = res.select("offset").collectAsList();
			List<Row> count = res.select("count(offset)").collectAsList();

			// assert correct ascending ordering
			String[] expectedOffset = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
			String[] expectedCount = new String[]{"1", "1", "1", "1", "1", "1", "1", "1", "1", "1"};

			assertArrayEquals(expectedOffset, offset.stream().map(r -> r.getAs(0).toString()).toArray());
			assertArrayEquals(expectedCount, count.stream().map(r -> r.getAs(0).toString()).toArray());
		});
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	void testSplittingByNumericalString() {
		String q = "index=* | eval a = offset + 0 | chart count(offset) by a";

		this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
			final StructType expectedSchema = new StructType(
					new StructField[] {
							new StructField("a", DataTypes.StringType, true, new MetadataBuilder().build()),
							new StructField("count(offset)", DataTypes.LongType, true, new MetadataBuilder().build()),
					}
			);

			assertEquals(expectedSchema, res.schema());

			List<Row> a = res.select("a").collectAsList();
			List<Row> count = res.select("count(offset)").collectAsList();

			// assert correct ascending ordering
			String[] expectedA = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
			String[] expectedCount = new String[]{"1", "1", "1", "1", "1", "1", "1", "1", "1", "1"};

			assertArrayEquals(expectedA, a.stream().map(r -> r.getAs(0).toString()).toArray());
			assertArrayEquals(expectedCount, count.stream().map(r -> r.getAs(0).toString()).toArray());
		});
	}

    @Disabled(value="Should be converted to a dataframe test")
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

		CatalystNode n = (CatalystNode) visitor.visit(tree);
		boolean aggregates = visitor.getAggregatesUsed();
		assertTrue(aggregates,visitor.getTraceBuffer().toString());
	}
}