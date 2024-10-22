/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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

import com.teragrep.functions.dpf_03.BloomFilterAggregator;
import com.teragrep.pth10.steps.AbstractStep;
import com.teragrep.pth10.steps.teragrep.aggregate.BinaryListColumn;
import com.teragrep.pth10.steps.teragrep.bloomfilter.BloomFilterForeachPartitionFunction;
import com.teragrep.pth10.steps.teragrep.bloomfilter.BloomFilterTable;
import com.teragrep.pth10.steps.teragrep.bloomfilter.FilterTypes;
import com.teragrep.pth10.steps.teragrep.bloomfilter.LazyConnection;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.SortedMap;

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
        this.zeppelinConfig = zeppelinConfig;
        this.mode = mode;
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
        writeFilterTypes(this.zeppelinConfig);
        final BloomFilterTable table = new BloomFilterTable(zeppelinConfig);
        table.create();
        dataset.foreachPartition(new BloomFilterForeachPartitionFunction(this.zeppelinConfig));
        return dataset;
    }

    /**
     * Create and store a bloom filter byte arrays generated from Datasets rows _raw column (Replaces duplicates)
     *
     * @param dataset Dataset that is used to update database
     * @return Dataset unmodified
     */
    private Dataset<Row> updateBloomFilter(Dataset<Row> dataset) {
        writeFilterTypes(this.zeppelinConfig);
        final BloomFilterTable table = new BloomFilterTable(zeppelinConfig);
        table.create();
        dataset.foreachPartition(new BloomFilterForeachPartitionFunction(this.zeppelinConfig, true));
        return dataset;
    }

    private Dataset<Row> estimateSize(Dataset<Row> dataset) {
        return dataset
                .select(functions.col("partition"), functions.explode(functions.col(inputCol)).as("token"))
                .groupBy("partition")
                .agg(functions.approxCountDistinct("token").as(outputCol));
    }

    public Dataset<Row> aggregate(final Dataset<Row> dataset) {
        final BinaryListColumn inputColumnToBytes = new BinaryListColumn(dataset, inputCol);
        final BloomFilterAggregator agg = new BloomFilterAggregator(
                inputCol,
                estimateCol,
                new FilterTypes(this.zeppelinConfig).sortedMap()
        );
        return inputColumnToBytes.dataset().groupBy("partition").agg(agg.toColumn().as("bloomfilter"));
    }

    private void writeFilterTypes(final Config config) {
        final FilterTypes filterTypes = new FilterTypes(config);
        final Connection connection = new LazyConnection(config).get();
        final SortedMap<Long, Double> filterSizeMap = filterTypes.sortedMap();
        final String pattern = filterTypes.pattern();
        for (final Map.Entry<Long, Double> entry : filterSizeMap.entrySet()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER
                        .info(
                                "Writing filtertype (expected <[{}]>, fpp: <[{}]>, pattern: <[{}]>)", entry.getKey(),
                                entry.getValue(), pattern
                        );
            }
            final String sql = "INSERT IGNORE INTO `filtertype` (`expectedElements`, `targetFpp`, `pattern`) VALUES (?, ?, ?)";
            try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setInt(1, entry.getKey().intValue()); // filtertype.expectedElements
                stmt.setDouble(2, entry.getValue()); // filtertype.targetFpp
                stmt.setString(3, pattern); // filtertype.pattern
                stmt.executeUpdate();
                stmt.clearParameters();
                connection.commit();
            }
            catch (SQLException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER
                            .error(
                                    "Error writing filter[expected: <{}>, fpp: <{}>, pattern: <{}>] into database",
                                    entry.getKey(), entry.getValue(), pattern
                            );
                }
                throw new RuntimeException(e);
            }
        }
    }
}
