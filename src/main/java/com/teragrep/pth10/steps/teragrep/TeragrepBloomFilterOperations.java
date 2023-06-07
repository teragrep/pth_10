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

import com.teragrep.functions.dpf_03.TokenAggregator;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.util.sketch.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import static com.teragrep.pth10.steps.teragrep.AbstractTeragrepStep.*;

public class TeragrepBloomFilterOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepBloomFilterOperations.class);

    /**
     * Create and store a bloom filter byte arrays generated from Datasets rows _raw column (Ignores duplicates)
     * @param ds Dataset that is used to update database
     * @return Dataset unmodified
     */
    static Dataset<Row> createBloomFilter(Dataset<Row> ds, DPLParserCatalystContext catCtx, Config zplnConfig) {

        if (!ds.isStreaming()) {
            throw new RuntimeException("Dataset was not streaming");
        }

        Connection conn = getSQLConnection(zplnConfig);
        if (conn == null) {
            throw new RuntimeException("Connection was null");
        }

        // Use TokenAggregator to convert col(_raw) into a list of String tokens
        TokenAggregator tokenAggregator = new TokenAggregator("_raw");
        Column tokenAggregatorColumn = tokenAggregator.toColumn();
        Dataset<Row> tokenizedDataset = ds.groupBy("partition")
                .agg(tokenAggregatorColumn)
                .withColumnRenamed("TokenAggregator(org.apache.spark.sql.Row)", "tokens");

        System.out.println("dpf_03.TokenAggregator called");

        // Spark writeStream foreachBatch: For all partitions in dataset create a bloom filter and save to DB
        DataStreamWriter<Row> databaseWriter = tokenizedDataset
                .writeStream()
                .outputMode("update")
                .foreachBatch((batchDataset, batchId) -> {
                    if (!batchDataset.isEmpty()) {
                        batchDataset.map((MapFunction<Row, String>) r -> r.getString(0), Encoders.STRING())
                                .collectAsList().forEach(partition -> {
                                    Column currentColumn = functions.col("partition").equalTo(partition);

                                    int numItems = batchDataset.where(currentColumn)
                                        .select(functions.size(functions.col("tokens")))
                                        .first().getInt(0);

                                    BloomFilter filter = createSizedFilter(batchDataset, currentColumn, numItems);
                                    String databaseTableName;

                                    if (numItems < SMALL_EXPECTED_NUM_ITEMS) {
                                        databaseTableName = SMALL_FILTER_TABLE_NAME;
                                    } else if (numItems < MEDIUM_EXPECTED_NUM_ITEMS) {
                                        databaseTableName = MEDIUM_FILTER_TABLE_NAME;
                                    } else {
                                        databaseTableName = LARGE_FILTER_TABLE_NAME;
                                    }

                                    try {
                                        String sql = String.format("INSERT IGNORE INTO %s (`partition_id`, `filter`) VALUES (?,?)", databaseTableName);
                                        PreparedStatement ps = conn.prepareStatement(sql);

                                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                        filter.writeTo(baos);

                                        InputStream is = new ByteArrayInputStream(baos.toByteArray());
                                        ps.setInt(1, Integer.parseInt(partition));
                                        ps.setBlob(2, is);
                                        ps.executeUpdate();

                                        is.close();
                                        baos.close();

                                    } catch (IOException e) {
                                        LOGGER.error("Error serializing data");
                                        e.printStackTrace();
                                        throw new RuntimeException(e);
                                    } catch (SQLException e) {
                                        LOGGER.error("Error writing to database");
                                        e.printStackTrace();
                                        throw new RuntimeException(e);
                                    }
                                });
                    } else { LOGGER.info("Empty batch"); }
                });

        final String randomID = UUID.randomUUID().toString();
        final String queryName = "tg-bloom-create-" + randomID;

        StreamingQuery query = catCtx.getInternalStreamingQueryListener().registerQuery(queryName, databaseWriter);

        try { // Wait until query is finished
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally { // Close connection
            try {
                conn.close();
            } catch (SQLException e ) {
                LOGGER.error("Error closing SQL connection");
                e.printStackTrace();
            }
        }
        return ds;
    }

    /**
     * Create and store a bloom filter byte arrays generated from Datasets rows _raw column (Replaces duplicates)
     * @param ds Dataset that is used to update database
     * @return Dataset unmodified
     */
    static Dataset<Row> updateBloomFilter(Dataset<Row> ds, DPLParserCatalystContext catCtx, Config zplnConfig) {

        if (!ds.isStreaming()) {
            throw new RuntimeException("Dataset was not streaming");
        }

        Connection conn = getSQLConnection(zplnConfig);
        if (conn == null) {
            throw new RuntimeException("Connection was null");
        }

        // Use TokenAggregator to convert col(_raw) into a list of String tokens
        TokenAggregator tokenAggregator = new TokenAggregator("_raw");
        Column tokenAggregatorColumn = tokenAggregator.toColumn();
        Dataset<Row> tokenizedDataset = ds.groupBy("partition")
                .agg(tokenAggregatorColumn)
                .withColumnRenamed("TokenAggregator(org.apache.spark.sql.Row)", "tokens");

        System.out.println("dpf_03.TokenAggregator called");
        LOGGER.warn("dpf_03.TokenAggregator called");

        // Spark writeStream foreachBatch: For all partitions in dataset create a bloom filter and save to DB
        DataStreamWriter<Row> databaseWriter = tokenizedDataset
                .writeStream()
                .outputMode("update")
                .foreachBatch((batchDataset, batchId) -> {
                    if (!batchDataset.isEmpty()) {
                        batchDataset.map((MapFunction<Row, String>)  r -> r.getString(0), Encoders.STRING())
                                .collectAsList().forEach(partition -> {
                                    Column currentColumn = functions.col("partition").equalTo(partition);

                                    int numItems = batchDataset.where(currentColumn)
                                        .select(functions.size(functions.col("tokens")))
                                        .first().getInt(0);

                                    BloomFilter filter = createSizedFilter(batchDataset, currentColumn, numItems);
                                    String databaseTableName;

                                    if (numItems < SMALL_EXPECTED_NUM_ITEMS) {
                                        databaseTableName = SMALL_FILTER_TABLE_NAME;
                                    } else if (numItems < MEDIUM_EXPECTED_NUM_ITEMS) {
                                        databaseTableName = MEDIUM_FILTER_TABLE_NAME;
                                    } else {
                                        databaseTableName = LARGE_FILTER_TABLE_NAME;
                                    }

                                    try {
                                        String sql = String.format("INSERT INTO %s (`partition_id`, `filter`) " +
                                            "VALUES(?,?) ON DUPLICATE KEY UPDATE filter=VALUES(`filter`)",
                                            databaseTableName);

                                        PreparedStatement ps = conn.prepareStatement(sql);

                                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                        filter.writeTo(baos);

                                        InputStream is = new ByteArrayInputStream(baos.toByteArray());
                                        ps.setInt(1, Integer.parseInt(partition));
                                        ps.setBlob(2, is);
                                        ps.executeUpdate();

                                        is.close();
                                        baos.close();

                                    } catch (IOException e) {
                                        LOGGER.error("Error serializing data");
                                        e.printStackTrace();
                                        throw new RuntimeException(e);
                                    } catch (SQLException e) {
                                        LOGGER.error("Error writing to database");
                                        e.printStackTrace();
                                        throw new RuntimeException(e);
                                    }
                                });
                    } else { LOGGER.info("Empty batch"); }
                });

        final String randomID = UUID.randomUUID().toString();
        final String queryName = "tg-update-create-" + randomID;

        StreamingQuery query = catCtx.getInternalStreamingQueryListener().registerQuery(queryName, databaseWriter);

        try { // Wait until query is finished
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally { // Close connection
            try {
                conn.close();
            } catch ( SQLException e ) {
                LOGGER.error("Error closing SQL connection");
                e.printStackTrace();
            }
        }
        return ds;
    }

    /**
     * Create a bloom filter from given dataset depending on the number of items inside
     * @param dataset dataset that is used
     * @param column column used to create the filter
     * @param numItems number of items in the column
     * @return generated bloom filter object
     */
    private static BloomFilter createSizedFilter(Dataset<Row> dataset, Column column, int numItems) {
        BloomFilter filter;
        if (numItems < SMALL_EXPECTED_NUM_ITEMS) {
            filter = dataset.where(column).stat().bloomFilter(functions.explode(dataset.col("tokens")),
                    SMALL_EXPECTED_NUM_ITEMS, SMALL_FALSE_POSITIVE_PROBABILITY);

        } else if (numItems < MEDIUM_EXPECTED_NUM_ITEMS) {
            filter = dataset.where(column).stat().bloomFilter(functions.explode(dataset.col("tokens")),
                    MEDIUM_EXPECTED_NUM_ITEMS, MEDIUM_FALSE_POSITIVE_PROBABILITY);

        } else {
            filter = dataset.where(column).stat().bloomFilter(functions.explode(dataset.col("tokens")),
                    LARGE_EXPECTED_NUM_ITEMS, LARGE_FALSE_POSITIVE_PROBABILITY);
        }
        return filter;
    }

    /**
     * Helper method to get SQL connection from zeppelinConfig
     */
    private static Connection getSQLConnection(Config zeppelinConfig) {

        Connection conn = null;
        String username;
        String password;
        String databaseUrl;

        if (zeppelinConfig == null) {
            LOGGER.error("TeragrepStep.getSQLConnection(): zeppelinConfig was null");
            return null;
        }

        // Get SQL username
        if (zeppelinConfig.hasPath(DATABASE_USERNAME_CONFIG_ITEM)) {
            username = zeppelinConfig.getString(DATABASE_USERNAME_CONFIG_ITEM);
            if (username == null || username.equals("")) {
                throw new RuntimeException("Database username not set.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + DATABASE_USERNAME_CONFIG_ITEM + "'.");
        }

        // Get SQL password
        if (zeppelinConfig.hasPath(DATABASE_PASSWORD_CONFIG_ITEM)) {
            password = zeppelinConfig.getString(DATABASE_PASSWORD_CONFIG_ITEM);
            if (password == null || password.equals("")) {
                throw new RuntimeException("Database password not set.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + DATABASE_PASSWORD_CONFIG_ITEM + "'.");
        }

        // Get SQL url
        if (zeppelinConfig.hasPath(DATABASE_DEFAULT_URL_CONFIG_ITEM)) {
            databaseUrl = zeppelinConfig.getString(DATABASE_DEFAULT_URL_CONFIG_ITEM);
            if (databaseUrl == null || databaseUrl.equals("")) {
                throw new RuntimeException("Database url not set.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + DATABASE_DEFAULT_URL_CONFIG_ITEM + "'.");
        }

        try { // Get SQL connection
            conn = DriverManager.getConnection(databaseUrl, username, password);
            LOGGER.info("SQL connection created");
        } catch (SQLException e) {
            LOGGER.error("Error getting SQL connection from zeppelinConfig");
        }

        return conn;
    }
}
