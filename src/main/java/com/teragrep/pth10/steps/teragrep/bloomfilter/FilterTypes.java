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

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.typesafe.config.Config;
import org.apache.spark.util.sketch.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.reflect.TypeToken;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public final class FilterTypes implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterTypes.class);

    private final Config config;

    public FilterTypes(Config config) {
        this.config = config;
    }

    /**
     * Filter sizes as sorted map
     * <p>
     * Keys = filter expected num of items, values = filter FPP
     *
     * @return SortedMap of filter configuration
     */
    public SortedMap<Long, Double> sortedMap() {
        final SortedMap<Long, Double> sizesMapFromJson = new TreeMap<>();
        final List<BloomFilterConfiguration> filterConfigurationList;

        try {
            final Gson gson = new Gson();
            filterConfigurationList = gson.fromJson(sizesJsonString(), new TypeToken<List<BloomFilterConfiguration>>() {
            }.getType());
        }
        catch (final JsonIOException | JsonSyntaxException e) {
            throw new IllegalArgumentException(
                    "Error parsing 'dpl.pth_06.bloom.db.fields' option to JSON. ensure that filter size options are formated as an JSON array and that there are no duplicate values. "
                            + "example '[{expected: 1000, fpp: 0.01},{expected: 2000, fpp: 0.01}]'. message: "
                            + e.getMessage()
            );
        }

        for (final BloomFilterConfiguration configuration : filterConfigurationList) {
            final Long expectedNumOfItems = configuration.expectedNumOfItems();
            final Double falsePositiveProbability = configuration.falsePositiveProbability();
            sizesMapFromJson.put(expectedNumOfItems, falsePositiveProbability);
        }

        final boolean hasDuplicates = new HashSet<>(filterConfigurationList).size() != filterConfigurationList.size();
        if (hasDuplicates) {
            throw new IllegalArgumentException("Found duplicate values in 'dpl.pth_06.bloom.db.fields'");
        }
        return sizesMapFromJson;
    }

    public Map<Long, Long> bitSizeMap() {
        final Map<Long, Double> filterSizes = sortedMap();
        final Map<Long, Long> bitsizeToExpectedItemsMap = new HashMap<>();
        // Calculate bitSizes
        for (final Map.Entry<Long, Double> entry : filterSizes.entrySet()) {
            final BloomFilter bf = BloomFilter.create(entry.getKey(), entry.getValue());
            bitsizeToExpectedItemsMap.put(bf.bitSize(), entry.getKey());
        }
        return bitsizeToExpectedItemsMap;
    }

    private String sizesJsonString() {
        final String jsonString;
        final String BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM = "dpl.pth_06.bloom.db.fields";
        if (config.hasPath(BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM)) {
            jsonString = config.getString(BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM);
            if (jsonString == null || jsonString.isEmpty() || "null".equals(jsonString)) {
                throw new RuntimeException("Bloom filter size fields was not configured.");
            }
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM + "'.");
        }
        return jsonString;
    }

    /** Save filter types with a given regex pattern */
    public void saveToDatabase(String regex) {
        final Connection connection = new LazyConnection(config).get();
        final SortedMap<Long, Double> filterSizeMap = sortedMap();
        for (final Map.Entry<Long, Double> entry : filterSizeMap.entrySet()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER
                        .info(
                                "Writing filtertype (expected <[{}]>, fpp: <[{}]>, pattern: <[{}]>)", entry.getKey(),
                                entry.getValue(), regex
                        );
            }
            final String sql = "INSERT IGNORE INTO `filtertype` (`expectedElements`, `targetFpp`, `pattern`) VALUES (?, ?, ?)";
            try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setInt(1, entry.getKey().intValue()); // filtertype.expectedElements
                stmt.setDouble(2, entry.getValue()); // filtertype.targetFpp
                stmt.setString(3, regex); // filtertype.pattern
                stmt.executeUpdate();
                stmt.clearParameters();
                connection.commit();
            }
            catch (SQLException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER
                            .error(
                                    "Error writing filter[expected: <{}>, fpp: <{}>, pattern: <{}>] into database",
                                    entry.getKey(), entry.getValue(), regex
                            );
                }
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        final FilterTypes cast = (FilterTypes) o;
        return Objects.equals(config, cast.config);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(config);
    }
}
