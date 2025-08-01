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
package com.teragrep.pth10.steps.teragrep;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
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
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.Collections;

/**
 * Teragrep exec hdfs delete: Deletes the given path from HDFS.
 */
public final class TeragrepHdfsDeleteStep extends AbstractStep {

    private final DPLParserCatalystContext catCtx;
    public final String pathStr;

    public TeragrepHdfsDeleteStep(DPLParserCatalystContext catCtx, String pathStr) {
        this.catCtx = catCtx;
        this.pathStr = pathStr;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        boolean success = false;
        String reason = "Unknown failure";
        Dataset<Row> generated;
        try {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem
                    .get(catCtx.getSparkSession().sparkContext().hadoopConfiguration());
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathStr);

            if (fs.exists(path)) {
                success = fs.delete(path, true);
                if (success) {
                    reason = "File deleted successfully.";
                }
            }
            else {
                reason = "The specified path does not exist, cannot delete";
            }
        }
        catch (IOException e) {
            reason = "Hadoop FS Error: " + e.getMessage();
        }

        Row r = RowFactory.create(pathStr, "delete", String.valueOf(success), reason);

        final StructType schema = new StructType(new StructField[] {
                new StructField("path", DataTypes.StringType, true, new MetadataBuilder().build()),
                new StructField("operation", DataTypes.StringType, true, new MetadataBuilder().build()),
                new StructField("success", DataTypes.StringType, true, new MetadataBuilder().build()),
                new StructField("reason", DataTypes.StringType, true, new MetadataBuilder().build())
        });

        // make a streaming dataset
        SparkSession ss = catCtx.getSparkSession();
        SQLContext sqlCtx = ss.sqlContext();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlCtx, Option.apply(1), encoder);

        generated = rowMemoryStream.toDS();

        // create hdfs writer and query
        final String queryName = "delete_hdfs_file" + ((int) (Math.random() * 100000));
        DataStreamWriter<Row> deleteHdfsWriter = generated.writeStream().outputMode("append").format("memory");
        StreamingQuery deleteHdfsQuery = catCtx
                .getInternalStreamingQueryListener()
                .registerQuery(queryName, deleteHdfsWriter);

        // add all the generated data to the memory stream
        rowMemoryStream.addData(JavaConversions.asScalaBuffer(Collections.singletonList(r)));

        // wait for it to be done and then return it
        deleteHdfsQuery.awaitTermination();

        return generated;
    }
}
