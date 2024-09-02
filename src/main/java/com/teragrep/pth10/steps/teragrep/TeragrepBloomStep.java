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

import com.teragrep.functions.dpf_03.BloomFilterAggregator;
import com.teragrep.pth10.steps.AbstractStep;
import com.teragrep.pth10.steps.teragrep.bloomfilter.BloomFilterForeachPartitionFunction;
import com.teragrep.pth10.steps.teragrep.bloomfilter.FilterSizes;
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
public class TeragrepBloomStep extends AbstractStep {
    public enum BloomMode {
        UPDATE,
        CREATE,
        ESTIMATE,
        AGGREGATE,
        DEFAULT
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepBloomStep.class);

    private final Config zeppelinConfig;
    public final BloomMode mode;
    private String inputCol;
    private String outputCol;
    private String estimateCol;

    // Bloom filter consts
    public final static String BLOOMDB_USERNAME_CONFIG_ITEM = "dpl.pth_10.bloom.db.username";
    public final static String BLOOMDB_PASSWORD_CONFIG_ITEM = "dpl.pth_10.bloom.db.password";
    public final static String BLOOMDB_URL_CONFIG_ITEM = "dpl.pth_06.bloom.db.url";
    public final static String BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM = "dpl.pth_06.bloom.db.fields";
    public final static Double MAX_FPP = 0.01;

    public TeragrepBloomStep(Config zeppelinConfig, BloomMode mode,
                             String inputCol, String outputCol, String estimateCol) {
        this.zeppelinConfig = zeppelinConfig;
        this.mode = mode;
        this.inputCol = inputCol;
        this.outputCol = outputCol;
        this.estimateCol = estimateCol;

        if (mode == BloomMode.ESTIMATE) {
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
                throw new UnsupportedOperationException("Selected bloom command is not supported. " +
                        "Supported commands: exec bloom create, exec bloom update, exec bloom estimate," +
                        ".");
        }

        return rv;
    }

    /**
     * Create and store a bloom filter byte generated from Datasets rows _raw column (Ignores duplicates)
     * @param dataset Dataset that is used to update database
     * @return Dataset unmodified
     */
    private Dataset<Row> createBloomFilter(Dataset<Row> dataset) {

        writeFilterSizesToDatabase(this.zeppelinConfig);

        dataset.foreachPartition(new BloomFilterForeachPartitionFunction(this.zeppelinConfig));

        return dataset;
    }

    /**
     * Create and store a bloom filter byte arrays generated from Datasets rows _raw column (Replaces duplicates)
     * @param dataset Dataset that is used to update database
     * @return Dataset unmodified
     */
    private Dataset<Row> updateBloomFilter(Dataset<Row> dataset) {

        writeFilterSizesToDatabase(this.zeppelinConfig);

        dataset.foreachPartition(new BloomFilterForeachPartitionFunction(this.zeppelinConfig, true));

        return dataset;
    }

    private Dataset<Row> estimateSize(Dataset<Row> dataset) {
        return dataset.select(
                        functions.col("partition"),
                        functions.explode(
                                functions.col(inputCol)
                        ).as("token")
                )
                .groupBy("partition")
                .agg(
                        functions.approxCountDistinct("token")
                                .as(outputCol)
                );
    }

    public Dataset<Row> aggregate(Dataset<Row> dataset) {

        FilterSizes filterSizes = new FilterSizes(this.zeppelinConfig);

        BloomFilterAggregator agg =
                new BloomFilterAggregator(inputCol, estimateCol, filterSizes.asSortedMap());

        return dataset.groupBy("partition")
                .agg(agg.toColumn().as("bloomfilter"));

    }

    private void writeFilterSizesToDatabase(Config config) {

        FilterSizes filterSizes = new FilterSizes(config);
        Connection connection = new LazyConnection(config).get();
        SortedMap<Long, Double> filterSizeMap = filterSizes.asSortedMap();

        for(Map.Entry<Long,Double> entry : filterSizeMap.entrySet()) {
            LOGGER.info("Writing filtertype[expected: <{}>, fpp: <{}>] to bloomdb.filtertype",
                    entry.getKey(), entry.getValue());

            String sql = "INSERT IGNORE INTO `filtertype` (`expectedElements`, `targetFpp`) VALUES (?, ?)";

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {

                stmt.setInt(1, entry.getKey().intValue()); // filtertype.expectedElements
                stmt.setDouble(2, entry.getValue()); // filtertype.targetFpp
                stmt.executeUpdate();
                stmt.clearParameters();

                connection.commit();

            } catch (SQLException e) {
                LOGGER.error("Error writing filter[expected: <{}>, fpp: <{}>] into database", entry.getKey(), entry.getValue());
                throw new RuntimeException(e);
            }
        }
    }
}
