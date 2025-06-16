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
package com.teragrep.pth10.steps.subsearch;

import com.teragrep.pth10.ast.StepList;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Step for filtering the dataset generated in LogicalXMLStep. This Step needs its own StepList. It has to filter the
 * same dataset twice: first with the stepList given to it, and then it reads the results from that dataset and filters
 * the dataset with the results.
 */
public final class SubsearchStep extends AbstractSubsearchStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubsearchStep.class);

    public SubsearchStep(StepList stepList) {
        this.stepList = stepList;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        /*if (dataset == null) {
            // No main search data, only subsearch
            throw new IllegalStateException("SubsearchStep was not provided a initial dataset. ( This shouldn't matter for join-subsearch? )");
        }*/
        if (this.stepList == null || this.stepList.asList().isEmpty()) {
            throw new RuntimeException("SubsearchStep doesn't have any steps to execute!");
        }

        Dataset<Row> subSearchDs = this.stepList.executeSubsearch(dataset);

        if (getType() == SubSearchType.MAIN_SEARCH_FILTERING) {
            // Convert column names to avro-friendly names.
            // Add them to map and also rename in the dataframe.
            final Map<String, String> mapOfColumnNames = new HashMap<>();
            final StructField[] subsearchFields = subSearchDs.schema().fields();
            for (final StructField field : subsearchFields) {
                final String encodedName = "HEX"
                        .concat(Hex.encodeHexString(field.name().getBytes(StandardCharsets.UTF_8)));
                subSearchDs = subSearchDs.withColumnRenamed(field.name(), encodedName);
                mapOfColumnNames.put(encodedName, field.name());
            }

            final String randomID = UUID.randomUUID().toString();
            final String queryName = "subsearch-" + randomID;
            final String hdfsPath = this.hdfsPath;
            final String cpPath = hdfsPath + "checkpoint/sub/" + randomID;
            final String path = hdfsPath + "data/sub/" + randomID;
            DataStreamWriter<Row> subToDiskWriter = subSearchDs
                    .repartition(1)
                    .writeStream()
                    .format("avro")
                    .trigger(Trigger.ProcessingTime(0))
                    //  .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                    .option("checkpointLocation", cpPath)
                    .option("path", path)
                    .outputMode(OutputMode.Append());

            SparkSession ss = SparkSession.builder().getOrCreate();

            StreamingQuery subToDiskQuery = this.listener.registerQuery(queryName, subToDiskWriter);

            // await for listener to stop the subToDiskQuery
            subToDiskQuery.awaitTermination();

            // read subsearch data from disk and collect
            Dataset<Row> readFromDisk = ss.read().schema(subSearchDs.schema()).format("avro").load(path);

            // recover original field names
            final StructField[] fromDiskSchema = readFromDisk.schema().fields();
            for (final StructField field : fromDiskSchema) {
                readFromDisk = readFromDisk.withColumnRenamed(field.name(), mapOfColumnNames.get(field.name()));
            }

            List<Row> collected = readFromDisk.collectAsList();

            // generate filtering column
            Column filterColumn = null;
            for (Row collectedRow : collected) {
                for (int i = 0; i < collectedRow.length(); i++) {
                    String rowContent = collectedRow.get(i).toString();
                    if (filterColumn != null) {
                        filterColumn = filterColumn
                                .or(functions.col("_raw").rlike("(?i)^.*" + Pattern.quote(rowContent) + ".*$"));
                    }
                    else {
                        filterColumn = functions.col("_raw").rlike("(?i)^.*" + Pattern.quote(rowContent) + ".*$");
                    }
                }
            }

            if (filterColumn == null) {
                throw new IllegalStateException("Generated filter column via subsearch was null!");
            }
            else {
                LOGGER.info("Filter column: <{}>", filterColumn);
            }

            // filter main search dataset with column gotten from subSearch
            return dataset.where(filterColumn);
        }

        return subSearchDs;
    }
}
