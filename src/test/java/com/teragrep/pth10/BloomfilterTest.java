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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.*;
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
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BloomfilterTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepTransformationTest.class);

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
	new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
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
		.config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.5")
		.config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
		.config("checkpointLocation","/tmp/pth_10/test/teragrep_cmd/checkpoints/" + UUID.randomUUID() + "/")
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

	@Test
	@Disabled
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	public void filterCreateTest() throws StreamingQueryException, InterruptedException {
		final String id = UUID.randomUUID().toString();
		performStreamingDPLTest(
		/* DPL Query = */		    "index=index_A | teragrep exec bloom create",
		/* Expected result = */    Arrays.asList("1","2"),
		/* Expected value col = */ "offset"
		);
	}

    @Test
    @Disabled
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void filterUpdateTest() throws StreamingQueryException, InterruptedException {
        final String id = UUID.randomUUID().toString();
        performStreamingDPLTest(
        /* DPL Query = */		    "index=index_A | teragrep exec bloom update",
        /* Expected result = */    Arrays.asList("1","2"),
        /* Expected value col = */ "offset"
        );
    }

	// ----------------------------------------
	// Helper methods
	// ----------------------------------------

	private long lastBatchId = -1;
	public boolean checkCompletionOfStreamingQuery(StreamingQuery sq) {
		if (sq.lastProgress() == null || sq.status().message().equals("Initializing sources")) {
			// Query has not started yet
			return false;
		}

		boolean shouldStop = false;

		if (sq.lastProgress().batchId() != lastBatchId) {
			if (sq.lastProgress().sources().length != 0) {
				shouldStop = isMemoryStreamDone(sq);
			}
		}
		lastBatchId = sq.lastProgress().batchId();

		return shouldStop;
	}

	private boolean isMemoryStreamDone(StreamingQuery sq) {
		boolean isMemoryStreamDone = true;
		for (int i = 0; i < sq.lastProgress().sources().length; i++) {
			SourceProgress progress = sq.lastProgress().sources()[i];

			if (progress.description() != null && !progress.description().startsWith("MemoryStream[")) {
				// ignore others than MemoryStream
				continue;
			}

			if (progress.startOffset() != null) {
				if (!progress.startOffset().equalsIgnoreCase(progress.endOffset())) {
					isMemoryStreamDone = false;
				}
			}
			else {
				isMemoryStreamDone = false;
			}
		}

		return isMemoryStreamDone;
	}

	private void performStreamingDPLTest(String query, List<String> expectedValues, String expectedValueCol) throws StreamingQueryException, InterruptedException {
		// the names of the queries for source
		final String nameOfSourceStream = "test_source_data";

		// start streams for rowDataset (source)
		StreamingQuery sourceStreamingQuery = startStream(rowDataset, "append", nameOfSourceStream);

		// listener for source stream, this allows stopping the stream when all processing is done
		// without the use of thread.sleep()
		sparkSession.streams().addListener(new StreamingQueryListener() {
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
				String nameOfStream = queryProgress.progress().name();

				if (nameOfSourceStream.equals(nameOfStream)) {
					if (checkCompletionOfStreamingQuery(sourceStreamingQuery)) {
						sourceStreamingQuery.stop();
						sparkSession.streams().removeListener(this);
					}
				}
			}
		}); // end listener


		// Keep adding data to stream until enough runs are done
		long run = 0L, counter = 0L, id = 0L;
		long maxCounter = 2L, maxRuns = 1L; /* You can use these two variables to customize the amount of data */
        long tokenCount = 10L; // Number of tokens in _raw data

		while (sourceStreamingQuery.isActive()) {
			Timestamp time = Timestamp.from(Instant.ofEpochSecond(rng(1300091969, 1665391969)));//Timestamp.from(Instant.ofEpochSecond(0L));//Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));

			if (run < maxRuns) {
				rowMemoryStream.addData(
				makeRows(
				time, 					// _time
				++id, 					// id
                (counter % 2 == 0 ? generateRawData(tokenCount) : generateRawData(tokenCount/2)),			// _raw
				"index_A", 				// index
				"stream", 				// sourcetype
				"host", 				// host
				counter + "." + counter + "." + counter + "." + counter, 				// source
				String.valueOf(++counter), 	// partition
				counter, 				// offset
				1 						// make n amount of rows
				)
				);
			}

			// Run $run times, each with $counter makeRows()
			if (counter == maxCounter) {
				run++;
				counter = 0;
			}

		}



		// Got streamed data
		boolean truncateFields = false;
		int numRowsToPreview = 50;

		// Print previews of dataframes
		LOGGER.info(" -- Source data -- ");
		Dataset<Row> df = sqlContext.sql("SELECT * FROM test_source_data");
		df.show(numRowsToPreview, truncateFields);

		// Start performing the dpl query
		performDPLQuery(query, expectedValueCol, expectedValues);

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

	private int rng(int a, int b) {
		return new Random().nextInt(b-a) + a;
	}

	// Performs given DPL query and returns result dataset<row>
	private void performDPLQuery(String query, String expectedValueCol, List<String> expectedValues) {
		LOGGER.info("-> Got DPL query: " + query);

		ctx.setEarliest("-1Y");

		visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(query);
		DPLLexer lexer = new DPLLexer(inputStream);

		CommonTokenStream tokenStream = new CommonTokenStream(lexer);
		DPLParser parser = new DPLParser(tokenStream);
		ParseTree tree = parser.root();

		LOGGER.debug(tree.toStringTree(parser));

		//LOGGER.debug(TreeUtils.toPrettyTree(tree, Arrays.asList(parser.getRuleNames())));

		visitor.setConsumer(ds -> {
			LOGGER.info("Consumer dataset : " + ds.schema());
			ds.show(false);
			List<String> listOfResult = ds.select(expectedValueCol).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

			for (String expValue : expectedValues) {
				if (!listOfResult.contains(expValue)) {
					fail("Result set did not contain an expected value of " + expValue);
				}
			}
		});

		CatalystNode n = (CatalystNode) visitor.visit(tree);

		DataStreamWriter<Row> dsw = n.getDataStreamWriter();

		if (dsw != null) {
			LOGGER.info("! Data stream writer was found !");

			StreamingQuery sq = dsw.start();
			sq.processAllAvailable();
		}
		else {
			fail("DataStreamWriter was null! Streaming dataset was not generated like it should be");
		}
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

    // Generate comma seperated String of random characters of desired length
    private String generateRawData(long numberOfTokens) {
        if (numberOfTokens < 1) { return ""; }

        StringBuilder builder = new StringBuilder((int) numberOfTokens + 1);
        Random random = new Random();
        int wordLength = 8;

        builder.append("one");
        for(long l = 1; l < numberOfTokens; l++) {
            char[] rndWord = new char[wordLength];
            for (int j = 0; j < wordLength; j++) {
                rndWord[j] = (char)('a' + random.nextInt(26));
            }
            builder.append(", ").append(new String(rndWord));
        }
        return builder.toString();
    }
}

