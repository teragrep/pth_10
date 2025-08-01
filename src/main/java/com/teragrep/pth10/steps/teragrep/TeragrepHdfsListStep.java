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
import org.apache.hadoop.fs.FileStatus;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Teragrep exec hdfs list: Lists the files and directories in the given HDFS path non-recursively.
 */
public final class TeragrepHdfsListStep extends AbstractStep {

    private final DPLParserCatalystContext catCtx;
    private String pathStr;

    public TeragrepHdfsListStep(DPLParserCatalystContext catCtx, String pathStr) {
        this.catCtx = catCtx;
        this.pathStr = pathStr;
    }

    // for testing
    public String getPathStr() {
        return pathStr;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        Dataset<Row> generated = null;
        try {
            // get hdfs path and RemoteIterator of the files and directories
            if (pathStr == null) {
                // no path specified, get user's home directory
                pathStr = "/user/" + catCtx.getSparkSession().sparkContext().sparkUser();
            }
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem
                    .get(catCtx.getSparkSession().sparkContext().hadoopConfiguration());
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathStr);

            FileStatus[] fileStatuses = fs.globStatus(path);

            final List<Row> listOfRows = new ArrayList<>();
            final DecimalFormat twoDecimals = new DecimalFormat("#.##");
            if (fileStatuses != null) {
                // files found
                for (FileStatus fileStatus : fileStatuses) {
                    // get all files and their info
                    String fileName = fileStatus.getPath().getName();
                    String filePath = fileStatus.getPath().toUri().getPath();

                    // build 'type' column's string
                    String type = "";
                    type += fileStatus.isDirectory() ? "d" : "";
                    type += fileStatus.isFile() ? "f" : "";
                    type += fileStatus.isSymlink() ? "s" : "";
                    type += fileStatus.isEncrypted() ? "e" : "";

                    String fileOwner = fileStatus.getOwner();
                    String fileModDate = Instant.ofEpochSecond(fileStatus.getModificationTime() / 1000L).toString();
                    String fileAccDate = Instant.ofEpochSecond(fileStatus.getAccessTime() / 1000L).toString();

                    String filePerms = fileStatus.getPermission().toString();
                    String size = twoDecimals.format((fileStatus.getLen() / 1024d)) + "K";

                    // create row containing the file info and add it to the listOfRows
                    Row r = RowFactory
                            .create(filePerms, fileOwner, size, fileModDate, fileAccDate, fileName, filePath, type);
                    listOfRows.add(r);
                }
            }
            else {
                // no files found
                listOfRows.add(RowFactory.create(null, null, null, null, null, null, null, null));
            }

            // schema for the created rows
            final StructType schema = new StructType(new StructField[] {
                    new StructField("permissions", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("owner", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("size", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("modificationDate", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("accessDate", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("path", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("type", DataTypes.StringType, true, new MetadataBuilder().build())
            });

            // make a streaming dataset
            SparkSession ss = catCtx.getSparkSession();
            SQLContext sqlCtx = ss.sqlContext();
            ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
            MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlCtx, Option.apply(1), encoder);

            generated = rowMemoryStream.toDS();

            // create hdfs writer and query
            final String queryName = "list_hdfs_files_" + ((int) (Math.random() * 100000));
            DataStreamWriter<Row> listHdfsWriter = generated.writeStream().outputMode("append").format("memory");
            StreamingQuery listHdfsQuery = catCtx
                    .getInternalStreamingQueryListener()
                    .registerQuery(queryName, listHdfsWriter);

            // add all the generated data to the memory stream
            rowMemoryStream.addData(JavaConversions.asScalaBuffer(listOfRows));

            // wait for it to be done and then return it
            listHdfsQuery.awaitTermination();

        }
        catch (FileNotFoundException fnfe) {
            throw new RuntimeException(
                    "Specified path '" + pathStr + "' could not be found. Check that the path is written correctly."
            );
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        // filter null-name rows out
        return generated.where(functions.col("name").isNotNull());
    }
}
