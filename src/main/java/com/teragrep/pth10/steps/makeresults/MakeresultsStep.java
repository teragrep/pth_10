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
package com.teragrep.pth10.steps.makeresults;

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
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public final class MakeresultsStep extends AbstractMakeresultsStep {

    public MakeresultsStep() {
        super();
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        /*if (dataset == null) {
            return null;
        }*/

        // change schema based on annotate parameter
        StructType schema;
        if (annotate) {
            schema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("struck_server", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("struck_server_group", DataTypes.StringType, true, new MetadataBuilder().build())
            });
        }
        else {
            schema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build())
            });
        }

        // make a streaming dataset
        SparkSession ss = SparkSession.builder().getOrCreate();
        SQLContext sqlCtx = ss.sqlContext();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlCtx, Option.apply(1), encoder);
        Dataset<Row> generated = rowMemoryStream.toDS();

        final String queryName = "makeresults_" + ((int) (Math.random() * 100000));

        DataStreamWriter<Row> makeResultsWriter = generated.writeStream().outputMode("append").format("memory");

        StreamingQuery makeResultsQuery = this.catCtx
                .getInternalStreamingQueryListener()
                .registerQuery(queryName, makeResultsWriter);

        // add row $count times
        rowMemoryStream.addData(makeRows(count, annotate));

        makeResultsQuery.awaitTermination();

        return generated;
    }

    /**
     * Make one row $amount times and return as {@literal Seq<Row>}<br>
     * Uses system default timezone
     * 
     * @param amount   How many times each row should be repeated?
     * @param annotate Add more columns in addition to '_time'?
     * @return scala sequence of Rows
     */
    private Seq<Row> makeRows(int amount, boolean annotate) {
        List<Row> rowsList = new ArrayList<>();
        Row row;

        if (annotate) {
            row = RowFactory
                    .create(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.systemDefault())), catCtx.nullValue.value(), catCtx.nullValue.value(), catCtx.nullValue.value(), catCtx.nullValue.value(), catCtx.nullValue.value(), catCtx.nullValue.value());
        }
        else {
            row = RowFactory
                    .create(Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.systemDefault())));
        }

        while (amount > 0) {
            rowsList.add(row);
            amount--;
        }

        return JavaConverters.asScalaIteratorConverter(rowsList.iterator()).asScala().toSeq();
    }
}
