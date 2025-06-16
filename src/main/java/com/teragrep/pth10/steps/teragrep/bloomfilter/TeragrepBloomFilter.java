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
package com.teragrep.pth10.steps.teragrep.bloomfilter;

import org.apache.spark.util.sketch.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TeragrepBloomFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepBloomFilter.class);

    private final String partitionID;
    private final BloomFilter filter;
    private final Connection connection;
    private final FilterTypes filterTypes;
    private final String tableName;
    private final String regex;

    public TeragrepBloomFilter(
            String partition,
            byte[] bytes,
            Connection connection,
            FilterTypes filterTypes,
            String tableName,
            String regex
    ) {
        this(partition, new BloomFilterBlob(bytes).toBloomFilter(), connection, filterTypes, tableName, regex);
    }

    public TeragrepBloomFilter(
            String partition,
            BloomFilter filter,
            Connection connection,
            FilterTypes filterTypes,
            String tableName,
            String regex
    ) {
        this.partitionID = partition;
        this.filter = filter;
        this.filterTypes = filterTypes;
        this.connection = connection;
        this.tableName = tableName;
        this.regex = regex;
    }

    /**
     * Write filter bytes to database
     *
     * @param overwrite Set if existing filter data will be overwritten
     */
    public void saveFilter(final Boolean overwrite) {
        final long bitSize = filter.bitSize();
        final long selectedExpectedNumOfItems;
        final Double selectedFpp;
        final Map<Long, Long> bitSizeMap = filterTypes.bitSizeMap();
        if (bitSizeMap.containsKey(bitSize)) {
            final long expectedItems = bitSizeMap.get(bitSize);
            selectedExpectedNumOfItems = expectedItems;
            selectedFpp = filterTypes.sortedMap().get(expectedItems);
        }
        else {
            throw new IllegalArgumentException("no such filterSize <[" + bitSize + "]>");
        }
        final String sql = sqlString(overwrite);
        LOGGER.debug("Save filter SQL: <{}>", sql);
        try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                LOGGER
                        .debug(
                                "Saving filter expected <[{}]>, fpp <[{}]>, pattern <[{}]>, overwrite existing data=<{}>",
                                selectedExpectedNumOfItems, selectedFpp, regex, overwrite
                        );
                filter.writeTo(baos);
                InputStream is = new ByteArrayInputStream(baos.toByteArray());
                stmt.setLong(1, Long.parseLong(partitionID)); // bloomfilter.partition_id
                stmt.setLong(2, selectedExpectedNumOfItems); // filtertype.expectedElements
                stmt.setDouble(3, selectedFpp); // filtertype.targetFpp
                stmt.setString(4, regex); // filtertype.pattern
                stmt.setBlob(5, is); // bloomfilter.filter
                stmt.executeUpdate();
                stmt.clearParameters();
                is.close();
                connection.commit();
            }
            catch (IOException e) {
                throw new RuntimeException("Error serializing data: " + e);
            }
            catch (SQLException e) {
                throw new RuntimeException("Error writing to database: " + e);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error generating a prepared statement: " + e);
        }
    }

    private String sqlString(final Boolean overwriteExisting) {
        final String sql;
        if (overwriteExisting) {
            sql = "REPLACE INTO `" + tableName + "` (`partition_id`, `filter_type_id`,`filter`) " + "VALUES(?,"
                    + "(SELECT `id` FROM `filtertype` WHERE expectedElements=? AND targetFpp=? AND pattern=?)," + "?)";
        }
        else {
            sql = "INSERT IGNORE INTO `" + tableName + "` (`partition_id`, `filter_type_id`,`filter`) " + "VALUES(?,"
                    + "(SELECT `id` FROM `filtertype` WHERE expectedElements=? AND targetFpp=? AND pattern=?)," + "?)";
        }
        return sql;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        final TeragrepBloomFilter cast = (TeragrepBloomFilter) o;
        return Objects.equals(partitionID, cast.partitionID) && Objects.equals(filter, cast.filter) && Objects
                .equals(connection, cast.connection) && Objects.equals(filterTypes, cast.filterTypes)
                && Objects.equals(tableName, cast.tableName) && Objects.equals(regex, cast.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionID, filter, connection, filterTypes, tableName, regex);
    }
}
