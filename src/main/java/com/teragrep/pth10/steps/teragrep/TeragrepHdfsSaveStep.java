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

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.commands.transformstatement.teragrep.HdfsSaveMetadata;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

/**
 * teragrep exec hdfs save: Save dataset to disk in avro format
 */
public final class TeragrepHdfsSaveStep extends TeragrepHdfsStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepHdfsSaveStep.class);

    private final DPLParserCatalystContext catCtx;
    public final boolean overwrite;
    public final String pathStr;
    public final String retentionSpan;
    public final Format format;
    public final boolean header;

    public enum Format {
        CSV, JSON, AVRO
    }

    public TeragrepHdfsSaveStep(
            DPLParserCatalystContext catCtx,
            boolean overwrite,
            String pathStr,
            String retentionSpan,
            Format format,
            boolean header
    ) {
        super();
        this.catCtx = catCtx;
        this.overwrite = overwrite;
        this.pathStr = pathStr;
        this.retentionSpan = retentionSpan;
        this.format = format;
        this.header = header;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        org.apache.hadoop.fs.FileSystem fs;
        Dataset<Row> convertedDataset = dataset;
        String applicationId = null;
        String paragraphId = null;
        boolean previousSaveWasStreaming = true;

        try {
            // Check for pre-existing data in the given path

            // Set filesystem object based on spark's hadoop configuration
            fs = org.apache.hadoop.fs.FileSystem.get(catCtx.getSparkSession().sparkContext().hadoopConfiguration());

            // get the path for specified folder
            org.apache.hadoop.fs.Path fsPath = new org.apache.hadoop.fs.Path(pathStr);

            // Check for existence of that folder; if exists and overwrite=true, delete it. Otherwise, cancel operation.
            // If it doesn't exist, continue as-is.
            if (fs.exists(fsPath) && fs.isDirectory(fsPath)) {
                if (overwrite) {
                    LOGGER
                            .info(
                                    "TG HDFS Save: Pre-existing data was found in specified path. Deleting pre-existing data."
                            );

                    // path=fsPath, recursive=true
                    fs.delete(fsPath, true);
                }
                else {
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

                    if (
                        applicationId != null && applicationId
                                .equals(catCtx.getSparkSession().sparkContext().applicationId()) && paragraphId != null
                                && paragraphId.equals(catCtx.getParagraphUrl()) && !previousSaveWasStreaming
                    ) {
                        // appId matches last save and was not streaming (=aggregated); allow to overwrite
                        // this is due to sequential mode visiting this multiple times -> metadata will exist after first batch and overwrite=false would block rest of the batches!
                        LOGGER
                                .info(
                                        "Previous HDFS save to this path was not streaming and appId matches last save; allowing overwrite and bypassing overwrite=false parameter."
                                );
                    }
                    else {
                        throw new RuntimeException(
                                "The specified path '" + pathStr + "' already exists, please select another path."
                        );
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
            metadata.setOriginalSchema(dataset.isStreaming() ? null : dataset.schema());
            metadata.setTimestamp(Timestamp.from(Instant.now()));
            metadata.setRetentionSpan(retentionSpan);
            metadata.setWasStreamingDataset(dataset.isStreaming());
            metadata.setApplicationId(catCtx.getSparkSession().sparkContext().applicationId());
            metadata.setParagraphId(catCtx.getParagraphUrl());

            if (format == Format.AVRO) {
                // Convert to avro-friendly names before save to bypass naming restrictions
                final Map<String, String> mapOfColumnNames = new HashMap<>();
                for (final StructField field : dataset.schema().fields()) {
                    // avro-friendly column names conversion
                    final String encodedName = "HEX"
                            .concat(Hex.encodeHexString(field.name().getBytes(StandardCharsets.UTF_8)));
                    convertedDataset = convertedDataset.withColumnRenamed(field.name(), encodedName);
                    mapOfColumnNames.put(encodedName, field.name());
                }

                metadata.setMapOfAvroColumnNames(mapOfColumnNames);
                metadata.setSchema(convertedDataset.schema());
            }
            else {
                metadata.setSchema(dataset.schema());
            }

            // serialize and write to hdfs
            byte[] mdataArray = serializeMetadata(metadata);

            org.apache.hadoop.fs.Path pathToMetadata = new org.apache.hadoop.fs.Path(pathStr + "/metadata.dpl");

            FSDataOutputStream out = fs.create(pathToMetadata);
            out.write(mdataArray);
            out.close();

        }
        catch (IOException e) {
            throw new RuntimeException("Saving metadata object failed due to: \n" + e);
        }

        LOGGER.info("Dataset in HDFS save was streaming={}", dataset.isStreaming());
        if (!dataset.isStreaming()) {
            // Non-streaming dataset, e.g. inside forEachBatch (sequential stack mode)
            final String cpPath = pathStr + "/checkpoint";
            DataFrameWriter<Row> hdfsSaveWriter;
            if (format == Format.AVRO) {
                hdfsSaveWriter = convertedDataset
                        .write()
                        .format("avro")
                        .mode(this.aggregatesUsedBefore ? SaveMode.Overwrite : SaveMode.Append)
                        .option("checkpointLocation", cpPath)
                        .option("path", pathStr.concat("/data"));
            }
            else if (format == Format.CSV) {
                hdfsSaveWriter = dataset
                        .write()
                        .format("csv")
                        .mode(this.aggregatesUsedBefore ? SaveMode.Overwrite : SaveMode.Append)
                        .option("checkpointLocation", cpPath)
                        .option("header", header)
                        .option("path", pathStr.concat("/data"));
            }
            else {
                throw new IllegalArgumentException("Format '" + format + "' is not supported.");
            }

            hdfsSaveWriter.save();
        }
        else {
            // query name and checkpoint location path (required)
            final String randomID = UUID.randomUUID().toString();
            final String queryName = "tg-hdfs-save-" + randomID;
            final String cpPath = pathStr + "/checkpoint";

            DataStreamWriter<Row> hdfsSaveWriter;
            if (format == Format.AVRO) {
                hdfsSaveWriter = convertedDataset
                        .writeStream()
                        .format("avro")
                        .trigger(Trigger.ProcessingTime(0))
                        .option("path", pathStr.concat("/data"))
                        .option("checkpointLocation", cpPath)
                        .outputMode(OutputMode.Append());
            }
            else if (format == Format.CSV) {
                hdfsSaveWriter = dataset
                        .writeStream()
                        .format("csv")
                        .trigger(Trigger.ProcessingTime(0))
                        .option("path", pathStr.concat("/data"))
                        .option("checkpointLocation", cpPath)
                        .option("header", header)
                        .outputMode(OutputMode.Append());
            }
            else {
                throw new IllegalArgumentException("Format '" + format + "' is not supported.");
            }

            // check for query completion
            StreamingQuery hdfsSaveQuery = catCtx
                    .getInternalStreamingQueryListener()
                    .registerQuery(queryName, hdfsSaveWriter);

            // await for listener to stop the hdfsSaveQuery
            hdfsSaveQuery.awaitTermination();
        }
        return dataset;
    }
}
