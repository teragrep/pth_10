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
import com.teragrep.functions.dpf_03.BloomFilterAggregator;
import com.teragrep.pth10.steps.teragrep.aggregate.ColumnBinaryListingDataset;
import com.teragrep.pth10.steps.teragrep.bloomfilter.BloomFilterForeachPartitionFunction;
import com.teragrep.pth10.steps.teragrep.bloomfilter.BloomFilterTable;
import com.teragrep.pth10.steps.teragrep.bloomfilter.FilterTypes;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * teragrep exec bloom
 */
public final class TeragrepBloomStep extends AbstractStep {

    public enum BloomMode {
        UPDATE, CREATE, ESTIMATE, AGGREGATE, DEFAULT
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepBloomStep.class);

    private final Config zeppelinConfig;
    public final BloomMode mode;
    private final String tableName;
    private final String regex;
    private final String inputCol;
    private final String outputCol;
    private final String estimateCol;

    public TeragrepBloomStep(
            Config zeppelinConfig,
            BloomMode mode,
            String inputCol,
            String outputCol,
            String estimateCol
    ) {
        this(zeppelinConfig, mode, "table_name", "default_regex", inputCol, outputCol, estimateCol);
    }

    public TeragrepBloomStep(
            Config zeppelinConfig,
            BloomMode mode,
            String tableName,
            String regex,
            String inputCol,
            String outputCol,
            String estimateCol
    ) {
        this.zeppelinConfig = zeppelinConfig;
        this.mode = mode;
        this.tableName = tableName;
        this.regex = regex;
        this.inputCol = inputCol;
        this.outputCol = outputCol;
        this.estimateCol = estimateCol;

        if (mode == BloomMode.ESTIMATE || mode == BloomMode.AGGREGATE) {
            // estimate is run as an aggregation
            this.properties.add(CommandProperty.AGGREGATE);
        }
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        Dataset<Row> rv;
        switch (this.mode) {
            case CREATE:
                rv = createBloomFilter(dataset);
                break;
            case UPDATE:
                rv = updateBloomFilter(dataset);
                break;
            case ESTIMATE:
                rv = estimateSize(dataset);
                break;
            case AGGREGATE:
                rv = aggregate(dataset);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Selected bloom command is not supported. "
                                + "Supported commands: exec bloom create, exec bloom update, exec bloom estimate," + "."
                );
        }

        return rv;
    }

    /**
     * Create and store a bloom filter byte generated from Datasets rows _raw column (Ignores duplicates)
     *
     * @param dataset Dataset that is used to update database
     * @return Dataset unmodified
     */
    private Dataset<Row> createBloomFilter(Dataset<Row> dataset) {
        new FilterTypes(zeppelinConfig).saveToDatabase(regex);
        new BloomFilterTable(zeppelinConfig, tableName).create();
        dataset.foreachPartition(new BloomFilterForeachPartitionFunction(zeppelinConfig, tableName, regex));
        return dataset;
    }

    /**
     * Create and store a bloom filter byte arrays generated from Datasets rows _raw column (Replaces duplicates)
     *
     * @param dataset Dataset that is used to update database
     * @return Dataset unmodified
     */
    private Dataset<Row> updateBloomFilter(Dataset<Row> dataset) {
        new FilterTypes(zeppelinConfig).saveToDatabase(regex);
        new BloomFilterTable(zeppelinConfig, tableName).create();
        dataset.foreachPartition(new BloomFilterForeachPartitionFunction(zeppelinConfig, tableName, regex, true));
        return dataset;
    }

    private Dataset<Row> estimateSize(Dataset<Row> dataset) {
        return dataset
                .select(functions.col("partition"), functions.explode_outer(functions.col(inputCol)).as("token"))
                .groupBy("partition")
                .agg(functions.approxCountDistinct("token").as(outputCol));
    }

    public Dataset<Row> aggregate(final Dataset<Row> dataset) {
        final ColumnBinaryListingDataset colBinaryListingDataset = new ColumnBinaryListingDataset(dataset, inputCol);
        final BloomFilterAggregator bloomFilterAggregator = new BloomFilterAggregator(
                inputCol,
                estimateCol,
                new FilterTypes(this.zeppelinConfig).sortedMap()
        );
        return colBinaryListingDataset
                .dataset()
                .groupBy("partition")
                .agg(bloomFilterAggregator.toColumn().as("bloomfilter"));
    }
}
