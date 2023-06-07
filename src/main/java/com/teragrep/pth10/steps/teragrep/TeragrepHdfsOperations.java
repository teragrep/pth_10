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
package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.commands.transformstatement.teragrep.HdfsSaveMetadata;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.*;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class TeragrepHdfsOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepHdfsOperations.class);

    /**
     * Save dataset to disk in avro format
     * @param pathStr HDFS path for the data to save
     * @param retentionSpan the time how long the data will be retained on disk
     * @return input dataset (passthrough)
     */
    static Dataset<Row> saveDataToHdfs(final Dataset<Row> dataset, final DPLParserCatalystContext catCtx, final boolean overwrite,
                                        final String pathStr, final String retentionSpan, final String id) {
        
        org.apache.hadoop.fs.FileSystem fs;
        String applicationId = null;
        String paragraphId = null;
        boolean previousSaveWasStreaming = true;
        final StructType jsonifiedSchema =
                new StructType(new StructField[]{new StructField("value", DataTypes.StringType, true, new MetadataBuilder().build())});

        try {
            // Check for pre-existing data in the given path

            // Set filesystem object based on spark's hadoop configuration
            fs = org.apache.hadoop.fs.FileSystem.get(catCtx.getSparkSession().sparkContext().hadoopConfiguration());

            // get the path for specified folder
            org.apache.hadoop.fs.Path fsPath = new org.apache.hadoop.fs.Path(pathStr);

            // Check for existence of that folder; if exists and overwrite=true, delete it. Otherwise cancel operation.
            // If it doesn't exist, continue as-is.
            if (fs.exists(fsPath) && fs.isDirectory(fsPath)) {
                // read metadata first
                org.apache.hadoop.fs.Path metadataPath = new org.apache.hadoop.fs.Path(pathStr + "/metadata.dpl");

                // throw exception if metadata file not found
                if (fs.exists(metadataPath)) {
                    // read byte stream and deserialize into metadata object
                    FSDataInputStream in = fs.open(metadataPath);
                    byte[] metadataAsByteArray = new byte[in.available()];
                    in.readFully(metadataAsByteArray);
                    in.close();

                    HdfsSaveMetadata metadata = deserializeMetadata(metadataAsByteArray);
                    applicationId = metadata.getApplicationId();
                    paragraphId = metadata.getParagraphId();
                    previousSaveWasStreaming = metadata.getWasStreamingDataset();
                }

                if (overwrite) {
                    LOGGER.info("TG HDFS Save: Pre-existing data was found in specified path. Deleting pre-existing data.");

                    // path=fsPath, recursive=true
                    fs.delete(fsPath, true);
                }
                else {
                    if (applicationId != null && applicationId.equals(catCtx.getSparkSession().sparkContext().applicationId())
                            && paragraphId != null && paragraphId.equals(catCtx.getParagraphUrl()) && !previousSaveWasStreaming) {
                        // appId matches last save and was not streaming (=aggregated); allow overwrite
                        // this is due to sequential mode visiting this multiple times -> metadata will exist after first batch and overwrite=false would block rest of the batches!
                        LOGGER.info("Previous HDFS save to this path was not streaming and appId matches last save; allowing overwrite and bypassing overwrite=false parameter.");
                    } else {
                        throw new RuntimeException("The specified path '" + pathStr + "' already exists, please select another path.");
                    }

                }
            }

            // (Re)create directory needed for hdfs save and metadata
            fs.mkdirs(fsPath);
        }
        catch (IOException ioe) {
            throw new RuntimeException("Checking/deleting pre-existing data failed due to: \n" + ioe);
        }
        catch (IllegalArgumentException iae) {
            throw new RuntimeException("Path was not a directory or it did not exist: \n" + iae);
        }

        try {
            // Initialize metadata object
            HdfsSaveMetadata metadata = new HdfsSaveMetadata();
            metadata.setSchema(dataset.isStreaming() ? dataset.schema() : jsonifiedSchema);
            metadata.setOriginalSchema(dataset.isStreaming() ? null : dataset.schema());
            metadata.setTimestamp(Timestamp.from(Instant.now()));
            metadata.setRetentionSpan(retentionSpan);
            metadata.setWasStreamingDataset(dataset.isStreaming());
            metadata.setApplicationId(catCtx.getSparkSession().sparkContext().applicationId());
            metadata.setParagraphId(catCtx.getParagraphUrl());

            // serialize and write to hdfs
            byte[] mdataArray = serializeMetadata(metadata);

            org.apache.hadoop.fs.Path pathToMetadata = new org.apache.hadoop.fs.Path(pathStr + "/metadata.dpl");

            FSDataOutputStream out = fs.create(pathToMetadata);
            out.write(mdataArray);
            out.close();

        } catch (IOException e) {
            throw new RuntimeException("Saving metadata object failed due to: \n" + e);
        }

        if (!dataset.isStreaming()) {
            // Non-streaming dataset, e.g. inside forEachBatch (sequential stack mode)
            List<String> dsAsJsonList = dataset.toJSON().collectAsList();
            final String json = Arrays.toString(dsAsJsonList.toArray());

            Dataset<Row> jsonifiedDs = catCtx.getSparkSession().createDataFrame(Collections.singletonList(RowFactory.create(json)),jsonifiedSchema);

            final String cpPath = pathStr + "/checkpoint";
            DataFrameWriter<Row> hdfsSaveWriter =
                    jsonifiedDs
                            .write()
                            .format("avro")
                            .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                            .option("path", pathStr.concat("/data"))
                            .option("checkpointLocation", cpPath);

            catCtx.getObjectStore().add(id, hdfsSaveWriter);
        }
        else {
            // query name and checkpoint location path (required)
            final String randomID = UUID.randomUUID().toString();
            final String queryName = "tg-hdfs-save-" + randomID;
            final String cpPath = pathStr + "/checkpoint";

            DataStreamWriter<Row> hdfsSaveWriter =
                    dataset
                            .repartition(1)
                            .writeStream()
                            .format("avro")
                            .trigger(Trigger.ProcessingTime(0))
                            .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                            .option("path", pathStr.concat("/data"))
                            .option("checkpointLocation", cpPath)
                            .outputMode(OutputMode.Append());

            // check for query completion
            StreamingQuery hdfsSaveQuery = catCtx.getInternalStreamingQueryListener().registerQuery(queryName, hdfsSaveWriter);

            try {
                // await for listener to stop the hdfsSaveQuery
                hdfsSaveQuery.awaitTermination();
            } catch (StreamingQueryException e) {
                throw new RuntimeException("HDFS Save query failed while awaiting termination: \n" + e);
            }
        }
        return dataset;
    }

    /**
     * Load avro-formatted data from disk to memory
     * @param pathStr Path of data
     * @return read data from disk as dataset
     */
    static Dataset<Row> loadDataFromHdfs(final DPLParserCatalystContext catCtx, final String pathStr) {
        HdfsSaveMetadata metadata;
        StructType schema;
        StructType originalSchema;
        boolean wasStreaming;

        try {
            // read metadata first
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
                    catCtx.getSparkSession().sparkContext().hadoopConfiguration());
            org.apache.hadoop.fs.Path metadataPath = new org.apache.hadoop.fs.Path(pathStr + "/metadata.dpl");

            // throw exception if metadata file not found
            if (!fs.exists(metadataPath) || !fs.isFile(metadataPath)) {
                throw new RuntimeException("Could not find metadata in the specified path. Double-check the given path.");
            }

            // read byte stream and deserialize into metadata object
            FSDataInputStream in = fs.open(metadataPath);
            byte[] metadataAsByteArray = new byte[in.available()];
            in.readFully(metadataAsByteArray);
            in.close();

            metadata = deserializeMetadata(metadataAsByteArray);
            schema = metadata.getSchema();
            wasStreaming = metadata.getWasStreamingDataset();
            originalSchema = metadata.getOriginalSchema();


        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // return loaded data from disk using schema from metadata object
        final SparkSession ss = SparkSession.builder().getOrCreate();
        if (wasStreaming) {
            // Standard streaming dataset, e.g. no aggregations or forEachBatch mode when saved.
            return ss.readStream().format("avro").schema(schema).load(pathStr.concat("/data"));
        } else {
            // Non-streaming dataset, e.g. aggregations or forEachBatch mode when saved.

            // read json dataset
            Dataset<Row> jsonDataset = ss.readStream().format("avro").schema(schema).load(pathStr.concat("/data"));

            // explode into '$$dpl_internal_json_table$$' column
            Dataset<Row> explodedJsonDs = jsonDataset.withColumn("$$dpl_internal_json_table$$",
                    functions.explode(functions.from_json(functions.col("value"), DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)))));

            // get original column names and prefix with '$$dpl_internal_json_table$$.' to access them
            final List<String> jsonTableFields = new ArrayList<>();
            for (String field : originalSchema.fieldNames()) {
                jsonTableFields.add("$$dpl_internal_json_table$$.".concat(field));
            }

            // select only prefixed columns
            Seq<Column> selectCols = JavaConversions.asScalaBuffer(jsonTableFields.stream().map(functions::col).collect(Collectors.toList()));
            explodedJsonDs = explodedJsonDs.select(selectCols);
            return explodedJsonDs;
        }
    }

    /**
     * Lists the files and directories in the given HDFS path non-recursively.
     * @param pathStr HDFS or local file path
     * @return Dataset containing the file listing
     */
    static Dataset<Row> listDataFromHdfs(final DPLParserCatalystContext catCtx, final String pathStr) {
        Dataset<Row> generated;
        try {
            // get hdfs path and RemoteIterator of the files and directories
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
                    catCtx.getSparkSession().sparkContext().hadoopConfiguration());
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathStr);
            org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it = fs.listLocatedStatus(path);

            final List<Row> listOfRows = new ArrayList<>();
            final DecimalFormat twoDecimals = new DecimalFormat("#.##");
            while (it.hasNext()) {
                // get all files and their info
                org.apache.hadoop.fs.LocatedFileStatus lfs = it.next();

                String fileName = lfs.getPath().getName();
                String filePath = lfs.getPath().toUri().getPath();

                // build 'type' column's string
                String type = "";
                type += lfs.isDirectory() ? "d" : "";
                type += lfs.isFile() ? "f" : "";
                type += lfs.isSymlink() ? "s" : "";
                type += lfs.isEncrypted() ? "e" : "";

                String fileOwner = lfs.getOwner();
                String fileModDate = Instant.ofEpochSecond(lfs.getModificationTime()/1000L).toString();
                String fileAccDate = Instant.ofEpochSecond(lfs.getAccessTime()/1000L).toString();

                String filePerms = lfs.getPermission().toString();
                String size = twoDecimals.format((lfs.getLen() / 1024d)) + "K";

                // create row containing the file info and add it to the listOfRows
                Row r = RowFactory.create(
                        filePerms, fileOwner, size, fileModDate, fileAccDate, fileName, filePath, type);
                listOfRows.add(r);
            }

            // schema for the created rows
            final StructType schema =
                    new StructType(
                            new StructField[] {
                                    new StructField("permissions", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("owner", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("size", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("modificationDate", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("accessDate", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("path", DataTypes.StringType, true, new MetadataBuilder().build()),
                                    new StructField("type", DataTypes.StringType, true, new MetadataBuilder().build())
                            }
                    );

            // make a streaming dataset
            SparkSession ss = catCtx.getSparkSession();
            SQLContext sqlCtx = ss.sqlContext();
            ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
            MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlCtx, encoder);

            generated = rowMemoryStream.toDS();

            // create hdfs writer and query
            final String queryName = "list_hdfs_files_" + ((int)(Math.random() * 100000));
            DataStreamWriter<Row> listHdfsWriter = generated.
                    writeStream().outputMode("append").format("memory");
            StreamingQuery listHdfsQuery = catCtx.getInternalStreamingQueryListener().registerQuery(queryName, listHdfsWriter);

            // add all the generated data to the memory stream
            rowMemoryStream.addData(JavaConversions.asScalaBuffer(listOfRows));

            // wait for it to be done and then return it
            listHdfsQuery.awaitTermination();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (StreamingQueryException e) {
            throw new RuntimeException("Error awaiting termination: " + e);
        }
        return generated;
    }

    /**
     * Deletes the given path from HDFS
     * @param pathStr Local or HDFS path as a string
     * @return status
     */
    static Dataset<Row> deleteDataFromHdfs(final DPLParserCatalystContext catCtx, final String pathStr) {
        boolean success = false;
        String reason = "Unknown failure";
        Dataset<Row> generated;
        try {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(catCtx.getSparkSession().sparkContext().hadoopConfiguration());
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

        final StructType schema =
                new StructType(
                        new StructField[] {
                                new StructField("path", DataTypes.StringType, true, new MetadataBuilder().build()),
                                new StructField("operation", DataTypes.StringType, true, new MetadataBuilder().build()),
                                new StructField("success", DataTypes.StringType, true, new MetadataBuilder().build()),
                                new StructField("reason", DataTypes.StringType, true, new MetadataBuilder().build())
                        }
                );

        // make a streaming dataset
        SparkSession ss = catCtx.getSparkSession();
        SQLContext sqlCtx = ss.sqlContext();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlCtx, encoder);

        generated = rowMemoryStream.toDS();

        // create hdfs writer and query
        final String queryName = "delete_hdfs_file" + ((int)(Math.random() * 100000));
        DataStreamWriter<Row> deleteHdfsWriter = generated.
                writeStream().outputMode("append").format("memory");
        StreamingQuery deleteHdfsQuery = catCtx.getInternalStreamingQueryListener().registerQuery(queryName, deleteHdfsWriter);

        // add all the generated data to the memory stream
        rowMemoryStream.addData(JavaConversions.asScalaBuffer(Collections.singletonList(r)));

        // wait for it to be done and then return it
        try {
            deleteHdfsQuery.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException("Error awaiting termination: " + e);
        }

        return generated;
    }

    /**
     * Serializes HdfsSaveMetadata to a byte array
     * @param metadata input metadata
     * @return serialized as byte array
     */
    private static byte[] serializeMetadata(HdfsSaveMetadata metadata) {
        byte[] serialized;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeObject(metadata);
            serialized = baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing metadata object: " + e);
        }

        return serialized;
    }

    /**
     * Deserializes a byte array into a HdfsSaveMetadata object
     * @param serialized byte array
     * @return deserialized metadata object
     */
    private static HdfsSaveMetadata deserializeMetadata(byte[] serialized) {
        HdfsSaveMetadata deserialized;

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {

            deserialized = (HdfsSaveMetadata) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing metadata object: " + e);
        }

        return deserialized;
    }
}
