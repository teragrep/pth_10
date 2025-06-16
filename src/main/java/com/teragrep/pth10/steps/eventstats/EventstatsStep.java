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
package com.teragrep.pth10.steps.eventstats;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class EventstatsStep extends AbstractEventstatsStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventstatsStep.class);

    public EventstatsStep() {
        super();
        this.properties.add(CommandProperty.AGGREGATE);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        // perform aggregation
        Dataset<Row> aggDs = null;
        Column mainAgg = listOfAggregations.remove(0);
        Seq<Column> seqOfAggs = JavaConversions.asScalaBuffer(listOfAggregations);

        if (byInstruction != null) {
            LOGGER.info("Performing BY-grouped aggregation");
            aggDs = dataset.groupBy(byInstruction).agg(mainAgg, seqOfAggs);
        }
        else {
            LOGGER.info("Performing direct aggregation");
            aggDs = dataset.agg(mainAgg, seqOfAggs);
        }

        assert aggDs != null : "Aggregated dataset was null";

        // Get schemas
        StructType schema = dataset.schema();
        StructType aggSchema = aggDs.schema();

        // consts for saving to hdfs
        final String rndId = UUID.randomUUID().toString();
        final String pathForSave = this.hdfsPath;
        final String queryName = "eventstats_query_" + rndId;
        final String checkpointPath = pathForSave + "checkpoint/" + rndId;
        final String path = pathForSave + "data/" + rndId + ".avro";

        LOGGER
                .info(
                        String
                                .format(
                                        "Initializing a stream query for eventstats: name: '%s', Path(avro): '%s', Checkpoint path: '%s'",
                                        queryName, path, checkpointPath
                                )
                );

        // save ds to HDFS, and perform join on that
        DataStreamWriter<Row> writer = dataset
                .writeStream()
                .format("avro")
                .trigger(Trigger.ProcessingTime(0))
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .option("checkpointLocation", checkpointPath)
                .option("path", path)
                .outputMode("append");

        // start query and wait for finish
        SparkSession ss = SparkSession.builder().getOrCreate();

        StreamingQuery query = this.catCtx.getInternalStreamingQueryListener().registerQuery(queryName, writer);

        // Await for StreamingQueryListener to call stop()
        LOGGER.info("Awaiting for the filesink/hdfs save query to end...");
        query.awaitTermination();

        // Get saved ds (-> non-streaming) from storage, and join on aggregated (-> streaming) dataset

        Dataset<Row> savedDs = ss.sqlContext().read().format("avro").schema(schema).load(path);
        Dataset<Row> resultDs = null;

        assert savedDs != null : "Dataset read from file sink was null";

        if (byInstruction != null) {
            resultDs = savedDs.join(aggDs, byInstruction);
        }
        else {
            resultDs = savedDs.crossJoin(aggDs);
        }

        assert resultDs != null : "Joined dataset was null";

        // Used to rearrange the columns, join mangles them up a bit
        String[] aggSchemaFields = aggSchema.fieldNames();
        List<String> schemaFields = new ArrayList<>(Arrays.asList(schema.fieldNames()));

        for (String aggColName : aggSchemaFields) {
            if (!schemaFields.contains(aggColName)) {
                schemaFields.add(aggColName);
            }
        }

        Seq<Column> rearranged = JavaConversions
                .asScalaBuffer(schemaFields.stream().map(functions::col).collect(Collectors.toList()));
        resultDs = resultDs.select(rearranged); // rearrange to look more like original dataset

        return resultDs;
    }
}
