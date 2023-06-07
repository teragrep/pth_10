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
import com.teragrep.pth10.ast.commands.transformstatement.accum.BatchCollector;
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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class accumTransformationStreamingTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(accumTransformationStreamingTest.class);

	DPLParserCatalystContext ctx = null;
	DPLParserCatalystVisitor visitor = null;
	
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
	
	// TODO Add assertions now that streaming dataframe is used
	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void accumTransformationMainStreamingTest() throws StreamingQueryException, InterruptedException {
		SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
		SQLContext sqlContext = sparkSession.sqlContext();
		ctx = new DPLParserCatalystContext(sparkSession);
		
		sparkSession.sparkContext().setLogLevel("ERROR");
		
		// Create a testing stream of data based on the test schema defined above
		ExpressionEncoder<Row> encoder = RowEncoder.apply(testSchema);
		MemoryStream<Row> rowMemoryStream =
				new MemoryStream<>(1, sqlContext, encoder);
		
		// Create a spark structured streaming dataset and start writing the stream
		Dataset<Row> rowDataset = rowMemoryStream.toDS(); 
		//ctx.setDs(rowDataset);	// for stream ds
		
		// Dataset<Row> result = performDPLQuery("index=index_A | accum offset AS accum_result");
		BatchCollector collector = new BatchCollector();
		StreamingQuery streamingQuery = startStream(rowDataset, "append", "accum_test_source_data", collector);
		
		// StreamingQuery results = startStream(result, "append", "accum_test_result_data");
		
		// Keep adding data to stream until enough runs are done
		long run = 0L, counter = 0L, id = 0L;
		while (streamingQuery.isActive()) {
			Timestamp time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
			
			rowMemoryStream.addData(
					makeRows(
							time, 					// _time
							++id, 					// id (for debug purposes)
							"data data", 			// _raw
							"index_A", 				// index
							"stream", 				// sourcetype
							"host", 				// host
							"input", 				// source
							String.valueOf(run), 	// partition
							++counter, 				// offset
							1 						// make n amount of rows
					)
			);
			
			// Run $run times, each with $counter makeRows()
			if (counter == 25) {
				run++;
				counter = 0;
			}

			if (run == 3) {
//				Thread.sleep(10000);
				streamingQuery.processAllAvailable();
				streamingQuery.stop();
				streamingQuery.awaitTermination();
			}
		}
	
		/*while (results.isActive()) {
			//Thread.sleep(6000);
			///while (results.status().isTriggerActive()) { Do nothing }
			Thread.sleep(10000);
			results.stop();
			results.awaitTermination();
		}*/
		Dataset<Row> sortedDF = sparkSession.createDataFrame(collector.toList(), rowDataset.schema());
		ctx.setDs(sortedDF);
		Dataset<Row> res = performDPLQuery("index=index_A | accum offset AS accum_result");
		
		// Got streamed data
		boolean truncateFields = true;
		int numRowsToPreview = 75;
		
		//LOGGER.info(" -- Source data on which accum will be performed on -- ");
		//Dataset<Row> df = sqlContext.sql("SELECT * FROM accum_test_source_data");
		//df.show(numRowsToPreview, truncateFields);
		
		res.show(numRowsToPreview, truncateFields);
		
		//LOGGER.info(" -- Result data for command 'accum offset AS accum_result' -- ");
		//Dataset<Row> res = sqlContext.sql("SELECT * FROM accum_test_result_data");
		//res.show(numRowsToPreview, truncateFields);
		
		// Assertions
		List<Long> listOfResultAccums = res.select("accum_result").collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList()); // accum offset AS accum_result
		// Test data should always result to this with current params
		List<Long> listOfManuallyCalculated = new ArrayList<Long>(Arrays.asList(
					// 1L, 3L, 6L, 10L, 15L, 16L, 18L, 21L, 25L, 30L
					1L, 3L, 6L, 10L, 15L, 21L, 28L, 36L, 45L, 55L, 66L, 78L,
                    91L, 105L, 120L, 136L, 153L, 171L, 190L, 210L, 231L, 253L, 276L, 300L,
                    325L, 326L, 328L, 331L, 335L, 340L, 346L, 353L, 361L, 370L, 380L, 391L,
                    403L, 416L, 430L, 445L, 461L, 478L, 496L, 515L, 535L, 556L, 578L, 601L,
                    625L, 650L, 651L, 653L, 656L, 660L, 665L, 671L, 678L, 686L, 695L, 705L,
                    716L, 728L, 741L, 755L, 770L, 786L, 803L, 821L, 840L, 860L, 881L, 903L,
                    926L, 950L, 975L
				));
		
		assertEquals(listOfManuallyCalculated, listOfResultAccums);
		
		
		/*
		Dataset<Row> df = sqlContext.sql("SELECT * FROM accum_query");
		ctx.setDs(df);
		Dataset<Row> res = performDPLQuery("index=index_A | accum offset AS offset_accum");
		LOGGER.info("Result of length" + res.count() + " row(s)");
		res.show(100, false);
		*/
	}
	
	//private List<Dataset<Row>> results = new ArrayList<>();
	//Long previousAccum = 0L;
	//@SuppressWarnings("serial")
	private StreamingQuery startStream(Dataset<Row> rowDataset, String outputMode, String queryName, BatchCollector collector) {
		return rowDataset
				.writeStream()
				.outputMode(outputMode)
				.format("memory")
				.queryName(queryName)
				.foreachBatch((batchDF, batchId) -> {
					LOGGER.info("New batch--");
					// TODO Collect batches, and perform accum operation on final sorted DataFrame
					collector.collect(batchDF, batchId);
					//collector.printCollected();
				})
				.start();
	}
	
	// Performs given DPL query and returns result dataset<row>
	private Dataset<Row> performDPLQuery(String query) {
		LOGGER.info("-> Got DPL query: " + query);
		ctx.setEarliest("-1Y");
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(query);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		CatalystNode n = (CatalystNode) visitor.visit(tree);
		
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
	
	/**
	 * rowDataset.writeStream() ...
	 * .foreachBatch(
						new VoidFunction2<Dataset<Row>, Long>() {
							@Override
							public void call(Dataset<Row> batchDF, Long batchId) throws Exception {
								// batchCollect.collect(batchDf, BatchId)
								// batchDF is the current batch of data that is going to get transformed 
								LOGGER.info("Performing DPL Query for a batch of data ...");
								ctx.setDs(batchDF);
								Dataset<Row> res = performDPLQuery("index=index_A | accum offset AS offset_accum");
								res = res.withColumn("offset_accum", functions.col("offset_accum").plus(previousAccum));
								List<Long> listOfAccums = res.select("offset_accum").collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
								previousAccum = listOfAccums.get(listOfAccums.size()-1);
								results.add(res);						
							}
						}
				)
	 */
}
 
