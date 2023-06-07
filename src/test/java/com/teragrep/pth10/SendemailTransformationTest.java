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


import com.icegreen.greenmail.junit5.GreenMailExtension;
import com.icegreen.greenmail.util.ServerSetup;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * @author p000043u
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SendemailTransformationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(SendemailTransformationTest.class);

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
	void setEnv() throws IOException {

	}
	
	@org.junit.jupiter.api.BeforeEach
	void setUp() {
		sparkSession = SparkSession.builder()
				.master("local[*]")
				.config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
				.config("checkpointLocation","/tmp/pth_10/test/sendemail/checkpoints/" + UUID.randomUUID() + "/")
				.getOrCreate();
		
		sqlContext = sparkSession.sqlContext();
		ctx = new DPLParserCatalystContext(sparkSession);
		
		//sparkSession.sparkContext().setLogLevel("ERROR");
		
		encoder = RowEncoder.apply(testSchema);
		rowMemoryStream =
				new MemoryStream<>(1, sqlContext, encoder);
		
		// Create a spark structured streaming dataset and start writing the stream
		rowDataset = rowMemoryStream.toDS(); 
		ctx.setDs(rowDataset);	// for stream ds

		greenMail.start();
	}
	
	@org.junit.jupiter.api.AfterEach
	void tearDown() {
		greenMail.stop();
		visitor = null;
	}
	
	@org.junit.jupiter.api.AfterAll
	void disassembleEnv() throws IOException {
	}

	@RegisterExtension
	static GreenMailExtension greenMail = new GreenMailExtension(new ServerSetup(2525, "localhost", "smtp"));
	
	
	// ----------------------------------------
	// Tests
	// ----------------------------------------
		
	// basic email without results, no aggregations
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void sendemail_test_1() throws StreamingQueryException, InterruptedException, IOException, MessagingException {
		// Perform DPL query with streaming data
		performStreamingDPLTest(
				"index=index_A | sendemail to=exa@mple.test from=from@example.test cc=cc@example.test server=localhost:2525",
				"[_time, id, _raw, index, sourcetype, host, source, partition, offset]",
				ds -> {

				}
				);

		// Get message
		MimeMessage msg = greenMail.getReceivedMessages()[0];
		String msgStr = msgToString(msg);

		// Get toEmails and subject.
		String[] toEmails = msg.getHeader("to");
		String subject = msg.getHeader("subject")[0];
		String cc = msg.getHeader("cc")[0];
		String from = msg.getHeader("from")[0];

		//LOGGER.info(msgStr);

		// Assertions
		assertTrue(msgStr.contains("Search complete."));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("cc@example.test", cc);
		assertEquals("from@example.test", from);
		assertEquals("Teragrep Results", subject);
	}
	
	// basic email with two preceding eval commands
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void sendemail_test_2() throws StreamingQueryException, InterruptedException, IOException, MessagingException {
		// Perform DPL query with streaming data
		performStreamingDPLTest(
				"index=index_A | eval extraField=null() | eval oneMoreField=true() | sendemail to=\"exa@mple.test\" server=localhost:2525",
				"[_time, id, _raw, index, sourcetype, host, source, partition, offset, extraField, oneMoreField]",
				ds -> {
					
				}
				);
		
		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = msgToString(msg);
		
		// Get toEmails and subject.
		String[] toEmails = msg.getHeader("to");
		String subject = msg.getHeader("subject")[0];
		
		// Assertions
		assertTrue(msgStr.contains("Search complete."));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Teragrep Results", subject);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void sendemail_test_3() throws StreamingQueryException, InterruptedException, IOException, MessagingException {
		// Perform DPL query with streaming data
		performStreamingDPLTest(
				"index=index_A | chart avg(offset) as avgo | chart avg(avgo) as resultssss | sendemail to=\"exa@mple.test\" sendresults=true inline=true sendpdf=true format=csv server=localhost:2525 ",
				null,//"[result]",
				ds -> {
					
				}
				);
		
		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = msgToString(msg);
		
		// Get toEmails and subject.
		String[] toEmails = msg.getHeader("to");
		String subject = msg.getHeader("subject")[0];
		
		// Assertions
		assertTrue(msgStr.contains("Search results."));
		
		// if message contains the column headers like this it will contain the csv too
		assertTrue(msgStr.contains("result"));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Teragrep Results", subject);
	}
	
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void sendemail_test_4() throws StreamingQueryException, InterruptedException, IOException, MessagingException {
		// Perform DPL query with streaming data
		performStreamingDPLTest(
				"index=index_A | sendemail to=\"exa@mple.test\" subject=\"Custom subject\" sendresults=true inline=true format=csv server=localhost:2525",
				"[_time, id, _raw, index, sourcetype, host, source, partition, offset]",
				ds -> {
					
				}
				);
		
		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = msgToString(msg);
		
		// Get toEmails and subject.;
		String[] toEmails = msg.getHeader("to");
		String subject = msg.getHeader("subject")[0];

		// Assertions
		assertTrue(msgStr.contains("Search results."));
		
		// if message contains the column headers like this it will contain the csv too
		assertTrue(msgStr.contains("_time,id,_raw,index,sourcetype,host,source,partition,offset"));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Custom subject", subject);
	}
	
	// pipe where after stats, then send email
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void sendemail_test_5() throws StreamingQueryException, InterruptedException, IOException, MessagingException {
		// Perform DPL query with streaming data
		performStreamingDPLTest(
				"index=index_A | stats avg(offset) as avgo count(offset) as co | where co > 1 | sendemail to=\"exa@mple.test\" server=localhost:2525",
				"[avgo, co]",
				ds -> {
					
				}
				);
		
		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = msgToString(msg);
		
		// Get toEmails and subject.
		String[] toEmails = msg.getHeader("to");
		String subject = msg.getHeader("subject")[0];
		
		// Assertions
		assertTrue(msgStr.contains("Search complete."));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Teragrep Results", subject);
	}

	// empty resultset must not send email
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void sendemailTestEmptyResultset() throws StreamingQueryException,
			InterruptedException, IOException {
		// Perform DPL query with streaming data
		performStreamingDPLTest(
				"index=index_A" +
						"|chart count(_raw) as craw" +
						"|where craw < 0 " + // filter out all
						"|sendemail to=\"1@example.com\" server=localhost:2525",
				"[craw]", // returns empty dataframe, but has column names present
				ds -> {

				}
		);

		// must not send any message
		assertEquals(0, greenMail.getReceivedMessagesForDomain("1@example.com").length);
	}
	
	// ----------------------------------------
	// Helper methods
	// ----------------------------------------
	
	private void performStreamingDPLTest(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) throws StreamingQueryException, InterruptedException {	
		
		// the names of the queries for source and result
		final String nameOfSourceStream = "pth10_email_test_src";
				
		// start streams for rowDataset (source)
		StreamingQuery sourceStreamingQuery = startStream(rowDataset, "append", nameOfSourceStream);
		sourceStreamingQuery.processAllAvailable();
			
		
		// listener for source stream, this allows stopping the stream when all processing is done
		// without the use of thread.sleep()
		sparkSession.streams().addListener(new StreamingQueryListener() {
			int noProgress = 0;
		    @Override
		    public void onQueryStarted(QueryStartedEvent queryStarted) {
		        //LOGGER.info("Query started: " + queryStarted.id());
		    }
		    @Override
		    public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
		        //LOGGER.info("Query terminated: " + queryTerminated.id());
		    }
		    @Override
		    public void onQueryProgress(QueryProgressEvent queryProgress) {
		    	Double progress = queryProgress.progress().processedRowsPerSecond();
		    	String nameOfStream = queryProgress.progress().name();
		    	
		    	//LOGGER.info("Processed rows/sec: " + progress);
		    	// Check if progress has stopped (progress becomes NaN at the end)
		    	// and end the query if that is the case
		        if (Double.isNaN(progress) && nameOfStream == nameOfSourceStream) {
		        	noProgress++;
		        	if (noProgress > 1) {
		        		//LOGGER.info("No progress on source stream");
			        	
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
								" \"something\",\"nothing\" ", 			// _raw
								"index_A", 				// index
								"stream" + (counter % 2 == 0 ? 1 : 2), 				// sourcetype
								"host", 				// host
								"input", 				// source
								String.valueOf(run), 	// partition
								++counter, 				// offset
								1 //500 				// make n amount of rows
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
		
		sourceStreamingQuery.awaitTermination();
				
		// Got streamed data
		boolean truncateFields = false;
		int numRowsToPreview = 25;
		
		// Print previews of dataframes
		//LOGGER.info(" -- Source data -- ");
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
		//LOGGER.info("-> Got DPL query: " + query);
		
		ctx.setEarliest("-1Y");
		
		// Visit the parse tree
		visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(query);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		
		// set path for join cmd
		visitor.setHdfsPath("/tmp/pth_10/" + UUID.randomUUID());

		// set paragraph url
		ctx.setBaseUrl("http://teragrep.test");
		ctx.setNotebookUrl("NoteBookID");
		ctx.setParagraphUrl("ParaGraphID");
		
		// Set consumer for testing
		visitor.setConsumer(ds -> {
			//LOGGER.info("Batch handler consumer called for ds with schema: " + ds.schema());

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
			
			try {
				ctx.flush();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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

	private String msgToString(MimeMessage mimeMsg) throws MessagingException {
		String text = new BufferedReader(
				new InputStreamReader(mimeMsg.getRawInputStream(), StandardCharsets.UTF_8))
				.lines()
				.collect(Collectors.joining("\n"));

		return text;
	}
	
}