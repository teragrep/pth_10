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

package com.teragrep.pth10.steps.join;

import com.teragrep.pth10.ast.DPLInternalStreamingQuery;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;

public class JoinStep extends AbstractJoinStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinStep.class);
    public JoinStep(Dataset<Row> dataset) {
        super(dataset);
    }

    @Override
    public Dataset<Row> get() {
        if (this.dataset == null || this.subSearchDataset == null) {
            return null;
        }

        // for subsearch save
        final StructType subSchema = this.subSearchDataset.schema();
        final String randomID = UUID.randomUUID().toString();
        final String checkpointPath = this.pathForSubsearchSave.concat("/checkpoint/").concat(randomID);
        final String path = this.pathForSubsearchSave.concat("/data/").concat(randomID).concat(".avro");
        final String queryName = "join_subsearch_query_".concat(randomID);

        // prefix for columns for subsearch
        // e.g. "_time" -> "R__time"
        final String subSearchPrefix = "R_";

        final SparkSession ss = SparkSession.builder().getOrCreate();

        // Create subsearch to disk writer and start query
        DataStreamWriter<Row> subToDiskWriter =
                this.subSearchDataset
                        .writeStream()
                        .format("avro")
                        .trigger(Trigger.ProcessingTime(0))
                        .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                        .option("checkpointLocation", checkpointPath)
                        .option("path", path)
                        .outputMode("append");

        // Use StreamingQueryListener to stop query when no progress is detected
        StreamingQuery subToDiskQuery = this.getCatCtx().getInternalStreamingQueryListener().registerQuery(queryName, subToDiskWriter);

        // Await for StreamingQueryListener to call stop()
        try {
            subToDiskQuery.awaitTermination();
        } catch (StreamingQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // Read from disk to dataframe
        this.subSearchDataset = ss.sqlContext().read().format("avro").schema(subSchema).load(path);
        LOGGER.info("subsearch ds.count= " + this.subSearchDataset.count());

        // max parameter
        if (max != null && max != 0) {
            // parameter is given with any other value than 0
            // max=0 will not limit
            LOGGER.info("Sub search limit set to " + max);
            this.subSearchDataset = this.subSearchDataset.limit(max);
        }
        else if (max != null) {
            LOGGER.info("Sub search limit set to 0 (unlimited)");
            // nothing needs to be done
        }
        else {
            // default value is 1 (no parameter given)
            LOGGER.info("Sub search limit set to 1 (default)");
            this.subSearchDataset = this.subSearchDataset.limit(1);
        }

        // Expression for joining datasets together
        Column joinExpr = null;

        // Grab column names, and prefix subsearch dataset
        // to separate them from the left side
        String[] originalLeftSideCols = this.dataset.columns();

        for (String colName : this.subSearchDataset.columns()) {
            this.subSearchDataset = this.subSearchDataset.withColumnRenamed(colName, subSearchPrefix + colName);
        }

        String[] originalRightSideCols = this.subSearchDataset.columns();

        // Build joinExpr used for joining left and right side datasets
        // Also rename join fields to include a prefix for subsearch columns (default R_)
        // It is used to remove the duplicates after the join
        for (String fieldName : this.listOfFields) {
            LOGGER.info("Building joinExpr with field: " + fieldName);

            if (joinExpr == null) {
                joinExpr = this.dataset.col(fieldName).equalTo(this.subSearchDataset.col(subSearchPrefix + fieldName));
            }
            else {
                joinExpr = joinExpr.and(this.dataset.col(fieldName).equalTo(this.subSearchDataset.col(subSearchPrefix + fieldName)));
            }
        }

        // If parameters usetime=true, earlier=true
        if (usetime != null && usetime && earlier != null && earlier) {
            LOGGER.info("usetime=true, earlier=true (with joinExpr)");
            joinExpr = joinExpr.and(this.dataset.col("_time").geq(this.subSearchDataset.col(subSearchPrefix + "_time")));
        }
        // If parameters usetime=true, earlier=false
        else if (usetime != null && usetime && earlier != null && !earlier) {
            LOGGER.info("usetime=true, earlier=false (with joinExpr)");
            joinExpr = joinExpr.and(this.dataset.col("_time").leq(this.subSearchDataset.col(subSearchPrefix + "_time")));
        }

        Dataset<Row> result = null;
        // Perform the join using the constructed joinExpr in joinMode (default joinMode is inner)
        result = this.dataset.join(this.subSearchDataset, joinExpr, joinMode == null ? "inner" : joinMode);

        // drop all subsearch fields which were used to join the dataframes together
        for (String fieldName : listOfFields) {
            result = result.drop(subSearchPrefix + fieldName);
        }

        // Overwrite left side dataset's columns with the values from right side
        // Check that the column exists in subsearch and is still in the dataset
        // and use coalesce to overwrite
        if (overwrite != null && overwrite) {
            for (String colName : originalLeftSideCols) {
                if (Arrays.toString(originalRightSideCols).contains(subSearchPrefix + colName) && !listOfFields.contains(colName)) {
                    result = result.withColumn(colName, functions.coalesce(functions.col(colName), functions.col(subSearchPrefix + colName))).drop(subSearchPrefix + colName);
                }
            }
        }

        return result;
    }
}
