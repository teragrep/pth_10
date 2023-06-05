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
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author p000043u
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MakeresultsTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformationTest.class);

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
                .config("checkpointLocation","/tmp/pth_10/test/makeresults/checkpoints/" + UUID.randomUUID() + "/")
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
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        visitor = null;
    }


    // ----------------------------------------
    // Tests
    // ----------------------------------------


    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void makeresults_test_1() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "| makeresults",
                "[_time]",
                ds -> {
                    assertEquals(1, ds.count());
                }
        );
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void makeresults_test_2() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "| makeresults annotate=true",
                "[_time, _raw, host, source, sourcetype, struck_server, struck_server_group]",
                ds -> {
                    assertEquals(1, ds.count());
                }
        );
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void makeresults_test_3() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "| makeresults count=100",
                "[_time]",
                ds -> {
                    assertEquals(100, ds.count());
                }
        );
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void makeresults_test_4() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "| makeresults | eval a = 1",
                "[_time, a]",
                ds -> {
                    assertEquals(1, ds.count());
                }
        );
    }


    // ----------------------------------------
    // Helper methods
    // ----------------------------------------


    private void performStreamingDPLTest(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) throws StreamingQueryException, InterruptedException {
        // no input data

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
    private void performDPLQuery(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) {
        LOGGER.info("-> Got DPL query: " + query);

        ctx.setEarliest("-1Y");

        // Visit the parse tree
        visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        // set path for join cmd
        visitor.setHdfsPath("/tmp/pth_10/" + UUID.randomUUID());

        // Set consumer for testing
        visitor.setConsumer(ds -> {
            LOGGER.info("Batch handler consumer called for ds with schema: " + ds.schema());
            ds.show(50, false);
            if (expectedColumns != null) assertEquals(expectedColumns, Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");
            if (assertConsumer != null) assertConsumer.accept(ds); // more assertions, if any
        });

        assertNotNull(visitor.getConsumer(), "Consumer was not properly registered to visitor !");
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        DataStreamWriter<Row> dsw = n.getDataStreamWriter();

        assertNotNull(dsw, "DataStreamWriter was not returned from visitor !");

        // process forEachBatch
        StreamingQuery sq = dsw.start();
        sq.processAllAvailable();

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