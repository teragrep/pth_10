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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Teragrep exec hdfs load: Load avro-formatted data from disk to memory
 */
public final class TeragrepHdfsLoadStep extends TeragrepHdfsStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepHdfsLoadStep.class);
    private final DPLParserCatalystContext catCtx;
    public final String pathStr;
    public final Format format;
    public final boolean header;
    public final String schema;

    public enum Format {
        CSV, JSON, AVRO
    }

    public TeragrepHdfsLoadStep(
            DPLParserCatalystContext catCtx,
            String pathStr,
            Format format,
            boolean header,
            String schema
    ) {
        this.catCtx = catCtx;
        this.pathStr = pathStr;
        this.format = format;
        this.header = header;
        this.schema = schema;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        Dataset<Row> rv = null;
        try {
            // get hadoop fs
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem
                    .get(catCtx.getSparkSession().sparkContext().hadoopConfiguration());
            // first check if there is any wildcards
            if (pathStr.contains("*")) {
                // wildcard * char present
                FileStatus[] statuses = fs.globStatus(new org.apache.hadoop.fs.Path(pathStr));
                for (FileStatus status : statuses) {
                    String sPath = status.getPath().toUri().getPath();
                    LOGGER.info("HDFS Load found a wildcard path <[{}]>", sPath);

                    if (rv == null) {
                        rv = processHdfsLoad(sPath, fs, false, schema);
                    }
                    else {
                        Dataset<Row> res = processHdfsLoad(sPath, fs, false, schema);
                        if (res != null) {
                            rv = rv.union(res);
                        }
                    }
                }

            }
            else {
                // no wildcard char present
                LOGGER.info("HDFS Load did not find a wildcard char, loading as single path");
                rv = processHdfsLoad(pathStr, fs, true, schema);
            }

        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        if (rv == null) {
            throw new RuntimeException(
                    "HDFS Load did not find any valid data in the given path, please double-check " + "the path."
            );
        }

        return rv;
    }

    /**
     * Used to process each of the paths and return the loaded dataset
     * 
     * @param pathStr HDFS path to the folder containing the 'metadata.dpl' file and '/data' folder.
     * @param fs      Hadoop FS object
     * @return loaded data as Dataset
     */
    private Dataset<Row> processHdfsLoad(String pathStr, FileSystem fs, boolean isSinglePath, String csvSchema)
            throws StreamingQueryException {
        // read metadata first
        HdfsSaveMetadata metadata;
        StructType schema = new StructType();
        Map<String, String> mapOfAvroNames = new HashMap<>();
        StructType originalSchema = new StructType();
        boolean wasStreaming = true;

        try {
            org.apache.hadoop.fs.Path metadataPath = new org.apache.hadoop.fs.Path(pathStr + "/metadata.dpl");

            if (fs.exists(metadataPath) && fs.isFile(metadataPath)) {
                // Metadata exists: read byte stream and deserialize into metadata object
                FSDataInputStream in = fs.open(metadataPath);
                byte[] metadataAsByteArray = new byte[in.available()];
                in.readFully(metadataAsByteArray);
                in.close();

                metadata = deserializeMetadata(metadataAsByteArray);
                schema = metadata.getSchema();
                wasStreaming = metadata.getWasStreamingDataset();
                originalSchema = metadata.getOriginalSchema();
                mapOfAvroNames = metadata.getMapOfAvroColumnNames();
            }
            else if (format == Format.AVRO && isSinglePath) {
                throw new RuntimeException(
                        "Could not find metadata in the specified path. Double-check the given path."
                );
            }
            else if (format == Format.AVRO) {
                throw new RuntimeException("Path '" + pathStr + "' did not contain the necessary metadata.");
            }

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        // return loaded data from disk using schema from metadata object
        final SparkSession ss = SparkSession.builder().getOrCreate();

        if (format == Format.AVRO) {
            // mapOfAvroNames should be null if using old hdfs save
            if (mapOfAvroNames == null) {
                // old pre-avro name fix save
                // TODO: Remove at some point in the future
                if (wasStreaming) {
                    // Standard streaming dataset, e.g. no aggregations or forEachBatch mode when saved
                    return ss.readStream().format("avro").schema(schema).load(pathStr.concat("/data"));
                }
                else {
                    // Non-streaming dataset, e.g. aggregations or forEachBatch mode when saved.

                    // read json dataset
                    Dataset<Row> jsonDataset = ss
                            .readStream()
                            .format("avro")
                            .schema(schema)
                            .load(pathStr.concat("/data"));

                    // explode into '$$dpl_internal_json_table$$' column
                    Dataset<Row> explodedJsonDs = jsonDataset
                            .withColumn(
                                    "$$dpl_internal_json_table$$", functions
                                            .explode(functions.from_json(functions.col("value"), DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))))
                            );

                    // get original column names and prefix with '$$dpl_internal_json_table$$.' to access them
                    final List<String> jsonTableFields = new ArrayList<>();
                    for (String field : originalSchema.fieldNames()) {
                        jsonTableFields.add("$$dpl_internal_json_table$$.".concat(field));
                    }

                    // select only prefixed columns
                    Seq<Column> selectCols = JavaConversions
                            .asScalaBuffer(jsonTableFields.stream().map(functions::col).collect(Collectors.toList()));
                    explodedJsonDs = explodedJsonDs.select(selectCols);
                    return explodedJsonDs;
                }
            }
            else {
                // new avro-friendly save
                // load data from disk
                Dataset<Row> out = ss.readStream().format("avro").schema(schema).load(pathStr.concat("/data"));

                // retrieve the original column names
                for (StructField field : out.schema().fields()) {
                    out = out.withColumnRenamed(field.name(), mapOfAvroNames.get(field.name()));
                }

                // get timechart span if '_time' column is present
                for (StructField field : out.schema().fields()) {
                    if (field.name().equals("_time")) {
                        LOGGER
                                .info(
                                        "Found '_time' column in HDFS load data, reading min and max for timechart range calculation."
                                );
                        AtomicLong earliest = new AtomicLong(Long.MAX_VALUE);
                        AtomicLong latest = new AtomicLong(Long.MIN_VALUE);
                        DataStreamWriter<Row> dsw = out.writeStream().foreachBatch((ds, i) -> {
                            if (!ds.isEmpty()) {
                                final long newEarliest = ds
                                        .agg(functions.min("_time"))
                                        .first()
                                        .getTimestamp(0)
                                        .getTime() / 1000L;
                                if (earliest.get() > newEarliest) {
                                    LOGGER.debug("Set default earliest: <{}>", newEarliest);
                                    earliest.set(newEarliest);
                                }

                                final long newLatest = ds.agg(functions.max("_time")).first().getTimestamp(0).getTime()
                                        / 1000L;
                                if (latest.get() < newLatest) {
                                    LOGGER.debug("Set default latest: <{}>", newLatest);
                                    latest.set(newLatest);
                                }
                            }
                            else {
                                LOGGER.info("Avro file was empty, returning an empty dataset.");
                            }
                        });

                        StreamingQuery sq = catCtx
                                .getInternalStreamingQueryListener()
                                .registerQuery(String.valueOf(UUID.randomUUID()), dsw);

                        sq.awaitTermination();

                        catCtx.setDplMinimumEarliest(earliest.get());
                        catCtx.setDplMaximumLatest(latest.get());
                        break;
                    }
                }

                return out;
            }
        }
        else if (format == Format.CSV) {
            // Standard csv format streaming dataset
            String fileFormat = "csv";
            DataStreamReader reader = ss.readStream();
            if (header) {
                reader = reader.option("header", "true");
            }
            else {
                reader = reader.option("header", "false");
            }

            // prioritize schema given in command
            if (csvSchema != null && !csvSchema.isEmpty()) {
                reader = reader.schema(generateSchemaFromCsvHeader(csvSchema));
            }
            else if (schema != null && !schema.isEmpty()) {
                // schema from metadata
                reader = reader.schema(schema);
            }
            else {
                // no schema, load all in _raw column
                // read as plain text to ignore delimiters
                fileFormat = "text";
                reader = reader.schema(new StructType(new StructField[] {
                        new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build())
                }));
            }

            // files saved with HDFS save use a directory-based path
            // 3rd party files may be single .csv files
            if (pathStr.endsWith(".csv")) {
                // append wildcard to make it a directory, structured streaming requires it
                return reader.format(fileFormat).load(pathStr.concat("*"));
            }
            else {
                return reader.format(fileFormat).load(pathStr.concat("/data"));
            }
        }
        else {
            throw new IllegalArgumentException("Format '" + format + "' is not supported.");
        }
    }

    /**
     * Generate a Spark-compatible schema from a comma-separated header "a, b, c, d"
     * 
     * @param csvHeader CSV-style header schema
     * @return StructType containing the same schema. All as StringType.
     */
    private StructType generateSchemaFromCsvHeader(final String csvHeader) {
        // check for empty schema
        if (csvHeader == null || csvHeader.isEmpty()) {
            throw new IllegalArgumentException("Empty csv schema provided!");
        }

        // split on delimiter
        final String[] fields = csvHeader.split(",");
        if (fields.length == 0) {
            throw new IllegalArgumentException("Invalid csv schema provided: " + csvHeader);
        }

        final StructField[] structFields = new StructField[fields.length];
        for (int i = 0; i < fields.length; i++) {
            structFields[i] = new StructField(fields[i], DataTypes.StringType, true, new MetadataBuilder().build());
        }

        return new StructType(structFields);
    }
}
