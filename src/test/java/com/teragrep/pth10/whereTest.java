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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
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
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class whereTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(whereTest.class);


	// proper tests -v ----------------------------------------

	DPLParserCatalystContext ctx = null;
	DPLParserCatalystVisitor visitor = null;

	SparkSession sparkSession = null;
	SQLContext sqlContext = null;

	ExpressionEncoder<Row> encoder = null;
	MemoryStream<Row> rowMemoryStream = null;
	Dataset<Row> rowDataset = null;

	private static final StructType testSchema = new StructType(
			new StructField[] {
					new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
					new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
					new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
			}
	);

	@org.junit.jupiter.api.BeforeAll
	void setEnv() {

	}

	@org.junit.jupiter.api.BeforeEach
	void setUp() {
		sparkSession = SparkSession.builder()
				.master("local[*]")
				.config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
				.config("checkpointLocation","/tmp/pth_10/test/where" +
						"/checkpoints/" + UUID.randomUUID() + "/")
				.getOrCreate();

		sqlContext = sparkSession.sqlContext();
		ctx = new DPLParserCatalystContext(sparkSession);

		sparkSession.sparkContext().setLogLevel("ERROR");

		encoder = RowEncoder.apply(testSchema);
		rowMemoryStream =
				new MemoryStream<>(1, sqlContext, encoder);

		// Create a spark structured streaming dataset and start writing the stream
		rowDataset = rowMemoryStream.toDS();
		ctx.setDs(rowDataset);	// for stream ds
	}

	@org.junit.jupiter.api.AfterEach
	void tearDown() {
		visitor = null;
	}


	// ----------------------------------------
	// Tests
	// ----------------------------------------


	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void parseWhereTest() throws Exception {
		String q, e, result, uuid;
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where count > 70";
		long indexEarliestEpoch = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", null);
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS count FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch+") GROUP BY _time ) WHERE count > 70";
		result = utils.getQueryAnalysis(q);
		/*
		LOGGER.info("SQL ="+result);
		LOGGER.info("EXP ="+e);
		 */
		assertEquals(e,result, q);
	}

	/**
	 * <!-- index = voyager _index_earliest="04/16/2020:10:25:40" | chart count(_raw) as count by _time | where  count > 70 -->
	 * <root>
	 *  <search root=true>
	 *    <logicalStatement>
	 *       <AND>
	 *          <index operation="EQUALS" value="voyager" />
	 *          <index_earliest operation="GE" value="1587021940" />
	 *       </AND>
	 *    <transformStatements>
	 *       <transform>
	 *           <divideBy field="_time">
	 *              <chart field="_raw" fieldRename="count" function="count" />
	 *                  <transform>
	 *                     <where>
	 *                         <evalCompareStatement field="count" operation="GT" value="70" />
	 *                     </where>
	 *                  </transform>
	 *              </chart>
	 *          </divideBy>
	 *       </transform>
	 *    </transformStatements>
	 *    </logicalStatement>
	 *  </search>
	 * </root>
	 *
	 * ---------------------------
	 * <root>
	 *    <transformStatements>
	 *       <where>
	 *           <evalCompareStatement field="count" operation="GT" value="70" />
	 *           <transformStatement>
	 * 		         <divideBy field="_time">
	 *                   <chart field="_raw" fieldRename="count" function="count">
	 *                       <transformStatement>
	 *                           <search root="true">
	 *                               <logicalStatement>
	 *     			  		 	         <AND>
	 *          					         <index operation="EQUALS" value="voyager" />
	 *          					         <index_earliest operation="GE" value="1587021940" />
	 *       				 	         </AND>
	 *    					         </logicalStatement>
	 *    					     </search>
	 *    					  </transformStatement>
	 *                   </chart>
	 *  	          </divideBy>
	 *           </transformStatement>
	 *       </where>
	 *    </transformStatements>
	 * </root>
	 *
	 * scala-sample
	 * create dataframe (Result of search-transform when root=true)
	 * val df = spark.readStream.load().option("query","<AND><index operation=\"EQUALS\" value=\"voyager\" /><index_earliest operation=\"GE\" value=\"1587021940\" /></AND>")
	 * process that ( processing resulting dataframe)
	 * val resultingDataSet =  df.groupBy(col("`_time`")).agg(functions.count(col("`_raw`")).as("`count`")).where(col("`_raw`").gt(70));
	 *
	 * Same using single
	 * spark.readStream.load().option("query","<AND><index operation=\"EQUALS\" value=\"voyager\" /><index_earliest operation=\"GE\" value=\"1587021940\" /></AND>").groupBy(col("`_time`")).agg(functions.count(col("`_raw`")).as("`count`")).where(col("`_raw`").gt(70));
	 *  when using treewalker, add "`"-around column names count -> `count`
	 *
	 */

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalOrWhereTest() throws Exception {
		String q, e, result;
		// test where-clause with logical operation OR
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 OR cnt < 75";
		long indexEarliestEpoch5 = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch5+") GROUP BY _time ) WHERE cnt > 70 OR cnt < 75";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalAndWhereTest() throws Exception {
		String q, e, result;
		// test where-clause with logical operation AND
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 AND cnt < 75";
		long indexEarliestEpoch4 = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch4+") GROUP BY _time ) WHERE cnt > 70 AND cnt < 75";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalAndOrWhereTest() throws Exception {
		String q, e, result;
		// test where-clause with logical operation AND, OR
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 AND cnt < 75 OR cnt != 72";
		long indexEarliestEpoch6 = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch6+") GROUP BY _time ) WHERE cnt > 70 AND cnt < 75 OR cnt != 72";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void chainedTransformWhereTest() throws Exception {
		String q, e, result;
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 | where cnt < 75";
		long indexEarliestEpoch2 = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch2+") GROUP BY _time ) WHERE cnt > 70 ) WHERE cnt < 75";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void logicalWhereTest() throws Exception {
		String q, e, result;
		// test where-clause with logical operation AND, OR with parents
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 )";
		long indexEarliestEpoch8 = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch8+") GROUP BY _time ) WHERE ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 )";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void complexWhereTest() throws Exception {
		String q, e, result;
		// test where-clause with logical operation AND, OR with parents and several
		// logical operation
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt !=35 OR cnt = 65)";
		long indexEarliestEpoch = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", null);
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch+") GROUP BY _time ) WHERE ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt != 35 OR cnt = 65 )";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void complexWhereXmlTest() throws Exception {
		String q, e, result;
		// test where-clause with logical operation AND, OR with parents and several
		// logical operation
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt !=35 OR cnt = 65)";
		long indexEarliestEpoch = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e ="<root><!--index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt !=35 OR cnt = 65)--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"voyager\"/><index_earliest operation=\"GE\" value=\""+indexEarliestEpoch+"\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"cnt\" function=\"count\" type=\"aggregate\"><transform><where><OR><AND><evalCompareStatement field=\"cnt\" operation=\"GT\" value=\"70\"/><evalCompareStatement field=\"cnt\" operation=\"LT\" value=\"75\"/></AND><OR><AND><AND><evalCompareStatement field=\"cnt\" operation=\"GT\" value=\"30\"/><evalCompareStatement field=\"cnt\" operation=\"LT\" value=\"40\"/></AND><evalCompareStatement field=\"cnt\" operation=\"NOT_EQUALS\" value=\"35\"/></AND><evalCompareStatement field=\"cnt\" operation=\"EQUALS\" value=\"65\"/></OR></OR></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
		//e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index = \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch+") GROUP BY _time ) WHERE ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt != 35 OR cnt = 65 )";
		result = utils.getQueryAnalysis(q);
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void symbolTableTest() throws Exception {
		String q, e, result;
		// test symbol-table, count(raw)->replaced with generated row in form
		// __count_UUID
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) by _time | where  \"count(_raw)\" > 70 | where \"count(_raw)\" < 75";
		long indexEarliestEpoch = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT * FROM ( SELECT _time,count(_raw) AS __count_UUID FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch+") GROUP BY _time ) WHERE __count_UUID > 70 ) WHERE __count_UUID < 75";
		result = utils.getQueryAnalysis(q);
		// find generated fieldname from result and check that it is like __count_UUID
		String r[] = result.split("__count");
		String uuid = r[1].substring(1, 37);
		if (utils.isUUID(uuid)) {
			// Was generated row-name so accept that as expected one
			e = e.replace("__count_UUID", "__count_" + uuid);
		}
		assertEquals(e,result, q);
	}

	@Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
	public void defaultCountWithLogicalOperationsTest() throws Exception {
		String q, e, result,uuid;
		// test where-clause with logical operation AND, OR and testing symbol-table
		q = "index = voyager _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) by _time | where 'count(_raw)' > 71 AND 'count(_raw)' < 75 OR 'count(_raw)' != 72";
		long indexEarliestEpoch = TimestampToEpochConversion.unixEpochFromString("04/16/2020:10:25:40", "MM/dd/yyyy:HH:mm:ss");
		e = "SELECT * FROM ( SELECT _time,count(_raw) AS `count(_raw)` FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime("+indexEarliestEpoch+") GROUP BY _time ) WHERE 'count(_raw)' > 71 AND 'count(_raw)' < 75 OR 'count(_raw)' != 72";
		result = utils.getQueryAnalysis(q);
		//utils.printDebug(e, result);
		assertEquals(e,result, q);
	}


	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void whereTestIntegerColumnLessThan() throws StreamingQueryException,
			InterruptedException {
		performStreamingDPLTest(
				"index=index_A | where offset < 3",
				"[_time, id, _raw, index, sourcetype, host, source, " +
						"partition, offset]",
				ds -> {
					assertEquals(2, ds.collectAsList().size());
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void whereTestIntegerColumnLessThanAfterChart() throws StreamingQueryException,
			InterruptedException {
		performStreamingDPLTest(
				"index=index_A " +
						"| chart avg(offset) as aoffset" +
						"| chart values(aoffset) as voffset" +
						"| chart sum(voffset) as soffset" +
						"| where soffset > 3",
				"[soffset]",
				ds -> {
					assertEquals(1, ds.collectAsList().size());
				}
		);
	}


	// ----------------------------------------
	// Helper methods
	// ----------------------------------------


	private void performStreamingDPLTest(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) throws StreamingQueryException, InterruptedException {

		// the names of the queries for source and result
		final String nameOfSourceStream = "pth10_where_test_src";

		// start streams for rowDataset (source)
		StreamingQuery sourceStreamingQuery = startStream(rowDataset, "append", nameOfSourceStream);
		sourceStreamingQuery.processAllAvailable();


		// listener for source stream, this allows stopping the stream when all processing is done
		// without the use of thread.sleep()
		sparkSession.streams().addListener(new StreamingQueryListener() {
			int noProgress = 0;
			@Override
			public void onQueryStarted(QueryStartedEvent queryStarted) {
				LOGGER.info("Query started: " + queryStarted.id());
			}
			@Override
			public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
				LOGGER.info("Query terminated: " + queryTerminated.id());
			}
			@Override
			public void onQueryProgress(QueryProgressEvent queryProgress) {
				Double progress = queryProgress.progress().processedRowsPerSecond();
				String nameOfStream = queryProgress.progress().name();

				LOGGER.info("Processed rows/sec: " + progress);
				// Check if progress has stopped (progress becomes NaN at the end)
				// and end the query if that is the case
				if (Double.isNaN(progress) && nameOfStream == nameOfSourceStream) {
					noProgress++;
					if (noProgress > 1) {
						LOGGER.info("No progress on source stream");

						sourceStreamingQuery.processAllAvailable();
						sourceStreamingQuery.stop();
						sparkSession.streams().removeListener(this);
					}

				}

			}
		}); // end listener


		// Keep adding data to stream until enough runs are done
		long run = 0L, counter = 0L, id = 0L;
		long maxCounter = 10L, maxRuns = 1L; /* You can use these two variables to customize the amount of data */

		while (sourceStreamingQuery.isActive()) {
			Timestamp time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));

			if (run < maxRuns) {
				org.apache.spark.sql.execution.streaming.Offset rmsOffset = rowMemoryStream.addData(
						makeRows(
								time, 					// _time
								++id, 					// id
								"data data", 			// _raw
								"index_A", 				// index
								"stream" + (counter % 2 == 0 ? 1 : 2), 				// sourcetype
								"host", 				// host
								counter + "." + counter + "." + counter + "." + counter,				// source
								String.valueOf(run), 	// partition
								++counter, 				// offset
								1 //500 					// make n amount of rows
						)
				);
				//rowMemoryStream.commit((LongOffset)rmsOffset);
			}

			// Run $run times, each with $counter makeRows()
			if (counter == maxCounter) {
				run++;
				counter = 0;
			}

		}

		sourceStreamingQuery.awaitTermination(); // wait for query to be over

		// Got streamed data
		boolean truncateFields = false;
		int numRowsToPreview = 25;

		// Print previews of dataframes
		LOGGER.info(" -- Source data -- ");
		Dataset<Row> df = sqlContext.sql("SELECT * FROM " + nameOfSourceStream);
		df.show(numRowsToPreview, truncateFields);

		// Start performing the dpl query
		performDPLQuery(query, expectedColumns, assertConsumer);

	}

	// Starts the stream for streaming dataframe rowDataset in outputMode and sets the queryName
	private StreamingQuery startStream(Dataset<Row> rowDataset, String outputMode, String queryName) {
		return rowDataset
				.writeStream()
				.outputMode(outputMode)
				.format("memory")
				.queryName(queryName)
				.start();
	}

	// Performs given DPL query and returns result dataset<row>
	private Dataset<Row> performDPLQuery(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) {
		LOGGER.info("-> Got DPL query: " + query);

		ctx.setEarliest("-1Y");

		// Visit the parse tree
		visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(query);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();

		LOGGER.debug(tree.toStringTree(parser));

		// set path for join cmd
		visitor.setHdfsPath("/tmp/pth_10/" + UUID.randomUUID());

		// Set consumer for testing
		visitor.setConsumer(ds -> {
			LOGGER.info("Batch handler consumer called for ds with schema: " + ds.schema());
			ds.show(50, false);
			if (expectedColumns != null) assertEquals(expectedColumns, Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");
			if (assertConsumer != null) assertConsumer.accept(ds); // more assertions, if any
		});

		assertTrue(visitor.getConsumer() != null, "Consumer was not properly registered to visitor !");
		CatalystNode n = (CatalystNode) visitor.visit(tree);

		DataStreamWriter<Row> dsw = n.getDataStreamWriter();

		assertTrue(dsw != null || !n.getDataset().isStreaming(), "DataStreamWriter was not returned from visitor !");
		if (dsw != null) {
			// process forEachBatch
			StreamingQuery sq = dsw.start();
			sq.processAllAvailable();
		}

		return n.getDataset();
	}

	// Make rows of given amount
	private Seq<Row> makeRows(Timestamp _time, Long id, String _raw, String index, String sourcetype, String host, String source, String partition, Long offset, long amount) {
		ArrayList<Row> rowArrayList = new ArrayList<>();
		Row row = RowFactory.create(_time, id, _raw, index, sourcetype, host, source, partition, offset);

		while (amount > 0) {
			rowArrayList.add(row);
			amount--;
		}

		Seq<Row> rowSeq = JavaConverters.asScalaIteratorConverter(rowArrayList.iterator()).asScala().toSeq();
		return rowSeq;
	}


}
