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
import com.teragrep.pth10.steps.teragrep.bloomfilter.*;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

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
    private final FilterTypes filterTypes;
    private final LazyConnection connection;

    public TeragrepBloomStep(
            Config zeppelinConfig,
            BloomMode mode,
            String inputCol,
            String outputCol,
            String estimateCol
    ) {
        this(
                zeppelinConfig,
                mode,
                inputCol,
                outputCol,
                estimateCol,
                new FilterTypes(zeppelinConfig),
                new LazyConnection(zeppelinConfig)
        );
    }

    public TeragrepBloomStep(
            Config zeppelinConfig,
            BloomMode mode,
            String inputCol,
            String outputCol,
            String estimateCol,
            FilterTypes filterTypes,
            LazyConnection connection
    ) {
        this.zeppelinConfig = zeppelinConfig;
        this.mode = mode;
        this.inputCol = inputCol;
        this.outputCol = outputCol;
        this.estimateCol = estimateCol;
        this.connection = connection;
        this.filterTypes = filterTypes;

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
        createFilterTypeTable();
        writeFilterTypes();
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
        createFilterTypeTable();
        writeFilterTypes();
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

    public Dataset<Row> aggregate(Dataset<Row> dataset) {
        final SortedMap<Long, Double> map = new TreeMap<>();
        final List<FilterField> fieldList = filterTypes.fieldList();
        for (final FilterField field : fieldList) {
            map.put(field.expected(), field.fpp());
        }
        final BloomFilterAggregator agg = new BloomFilterAggregator(inputCol, estimateCol, map);
        return dataset.groupBy("partition").agg(agg.toColumn().as("bloomfilter"));
    }

    private void writeFilterTypes() {
        final List<FilterField> fieldList = filterTypes.fieldList();
        final String pattern = filterTypes.pattern();
        final Connection conn = connection.get();
        for (final FilterField field : fieldList) {
            final int expectedInt = field.expectedIntValue();
            final double fpp = field.fpp();
            if (LOGGER.isInfoEnabled()) {
                LOGGER
                        .info(
                                "Writing filtertype (expected <[{}]>, fpp: <[{}]>, pattern: <[{}]>)", expectedInt, fpp,
                                pattern
                        );
            }
            final String sql = "INSERT IGNORE INTO `filtertype` (`expectedElements`, `targetFpp`, `pattern`) VALUES (?, ?, ?)";
            try (final PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, expectedInt); // filtertype.expectedElements
                stmt.setDouble(2, fpp); // filtertype.targetFpp
                stmt.setString(3, pattern); // filtertype.pattern
                stmt.executeUpdate();
                stmt.clearParameters();
                conn.commit();
            }
            catch (SQLException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER
                            .error(
                                    "Error writing filter[expected: <{}>, fpp: <{}>, pattern: <{}>] into database",
                                    expectedInt, fpp, pattern
                            );
                }
                throw new RuntimeException(e);
            }
        }
    }

    private void createFilterTypeTable() {
        // from pth-06/database/bloomdb
        final String sql = "CREATE TABLE IF NOT EXISTS `filtertype` ("
                + "    `id`               bigint(20) UNSIGNED   NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "    `expectedElements` bigint(20) UNSIGNED   NOT NULL,"
                + "    `targetFpp`        DOUBLE(2, 2) UNSIGNED NOT NULL,"
                + "    `pattern`          VARCHAR(2048)         NOT NULL,"
                + "    UNIQUE KEY (`expectedElements`, `targetFpp`, `pattern`)" + ") ENGINE = InnoDB"
                + "  DEFAULT CHARSET = utf8mb4" + "  COLLATE = utf8mb4_unicode_ci;";
        try (final PreparedStatement statement = connection.get().prepareStatement(sql)) {
            statement.execute();
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating `filtertype` table: " + e.getMessage());
        }
    }
}
