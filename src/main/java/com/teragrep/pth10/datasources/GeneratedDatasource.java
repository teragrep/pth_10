/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2025 Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
package com.teragrep.pth10.datasources;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.typesafe.config.Config;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Datasource generator class
 */
public class GeneratedDatasource {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeneratedDatasource.class);

    private final Config config;
    private SparkSession sparkSession;
    private DPLParserCatalystContext catCtx;

    private final StructType schema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("origin", DataTypes.StringType, false, new MetadataBuilder().build()),
    });

    public GeneratedDatasource(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
        this.config = catCtx.getConfig();
        this.sparkSession = catCtx.getSparkSession();
    }

    public Dataset<Row> constructEmptyStream() throws StreamingQueryException {
        SQLContext sqlContext = sparkSession.sqlContext();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        final String queryName = "construct_empty_" + ((int) (Math.random() * 100000));

        DataStreamWriter<Row> writer = rowDataset.writeStream().format("memory").outputMode(OutputMode.Append());

        // generate one row with practically no data
        // TODO: Not generating any rows stops any progress. Figure out a way to generate a empty *streaming* dataset!
        rowMemoryStream
                .addData(makeRows(new java.sql.Timestamp(0L), Collections.singletonList(""), "", "", "", "", "", -1L, ""));

        StreamingQuery sq = this.catCtx.getInternalStreamingQueryListener().registerQuery(queryName, writer);
        sq.awaitTermination();

        // filter the one generated row out to have a truly empty dataset
        return rowDataset.where(functions.col("offset").geq(functions.lit(0)));
    }

    public Dataset<Row> constructStream(String status, String explainStr)
            throws StreamingQueryException, InterruptedException, UnknownHostException {
        List<String> lines = new ArrayList<>();
        lines.add(status);
        return constructStream(lines, explainStr);
    }

    public Dataset<Row> constructStream(List<String> strings, String commandStr)
            throws StreamingQueryException, InterruptedException, UnknownHostException {
        SQLContext sqlContext = sparkSession.sqlContext();

        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        if (commandStr == null) {
            commandStr = "Unspecified";
        }

        Dataset<Row> rowDataset = rowMemoryStream.toDF();
        final String queryName = "construct_" + ((int) (Math.random() * 100000));

        DataStreamWriter<Row> writer = rowDataset.writeStream().format("memory").outputMode("append");

        long offset = 0;
        String host = InetAddress.getLocalHost().getHostName();
        final String explainStr = commandStr;
        Timestamp time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));

        rowMemoryStream
                .addData(
                        // make rows containing counter as offset and run as partition
                        makeRows(
                                time, // 0 "_time", DataTypes.TimestampType
                                strings, // 1 "_raw", DataTypes.StringType
                                "_internal", // 2 "index", DataTypes.StringType
                                explainStr, // 3 "sourcetype", DataTypes.StringType
                                host, // 4 "host", DataTypes.StringType,
                                "teragrep", // 5 "input", DataTypes.StringType
                                sparkSession.sparkContext().applicationId(), // 6 "partition", DataTypes.StringType
                                offset, // 7 "offset", DataTypes.LongType
                                "original-host" // 8 "origin", DataTypes.StringType
                        )
                );

        StreamingQuery streamingQuery = this.catCtx
                .getInternalStreamingQueryListener()
                .registerQuery(queryName, writer);

        streamingQuery.awaitTermination();
        return rowDataset;
    }

    private Seq<Row> makeRows(
            Timestamp _time,
            List<String> _raw,
            String index,
            String sourcetype,
            String host,
            String source,
            String partition,
            Long offset,
            String origin
    ) {
        ArrayList<Row> rowArrayList = new ArrayList<>();

        _raw.forEach(s -> {
            Row row = RowFactory.create(_time, s, index, sourcetype, host, source, partition, offset, origin);
            rowArrayList.add(row);
        });
        Seq<Row> rowSeq = JavaConverters.asScalaIteratorConverter(rowArrayList.iterator()).asScala().toSeq();
        return rowSeq;
    }

}
