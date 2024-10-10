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
package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;
import org.apache.spark.util.sketch.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.reflect.TypeToken;

import java.io.Serializable;
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
        final Gson gson = new Gson();
        final List<JsonObject> jsonArray = gson.fromJson(sizesJsonString(), new TypeToken<List<JsonObject>>() {
        }.getType());
        for (final JsonObject object : jsonArray) {
            if (object.has("expected") && object.has("fpp")) {
                final Long expectedNumOfItems = Long.parseLong(object.get("expected").toString());
                final Double fpp = Double.parseDouble(object.get("fpp").toString());
                if (sizesMapFromJson.containsKey(expectedNumOfItems)) {
                    LOGGER.error("Duplicate value of expected number of items value: <[{}]>", expectedNumOfItems);
                    throw new RuntimeException("Duplicate entry expected num of items");
                }
                sizesMapFromJson.put(expectedNumOfItems, fpp);
            }
            else {
                throw new RuntimeException("JSON did not have expected values of 'expected' or 'fpp'");
            }
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

    public String pattern() {
        final String pattern;
        final String BLOOM_PATTERN_CONFIG_ITEM = "dpl.pth_06.bloom.pattern";
        if (config.hasPath(BLOOM_PATTERN_CONFIG_ITEM)) {
            final String patternFromConfig = config.getString(BLOOM_PATTERN_CONFIG_ITEM);
            if (patternFromConfig == null || patternFromConfig.isEmpty()) {
                throw new RuntimeException("Bloom filter pattern was not configured.");
            }
            pattern = patternFromConfig.trim();
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + BLOOM_PATTERN_CONFIG_ITEM + "'.");
        }
        return pattern;
    }

    public String tableName() {
        final String tableName;
        final String BLOOM_TABLE_NAME_ITEM = "dpl.pth_06.bloom.table.name";
        if (config.hasPath(BLOOM_TABLE_NAME_ITEM)) {
            final String tableNameFromConfig = config.getString(BLOOM_TABLE_NAME_ITEM);
            if (tableNameFromConfig == null || tableNameFromConfig.isEmpty()) {
                throw new RuntimeException("Bloom filter table name was not configured.");
            }
            tableName = tableNameFromConfig.replaceAll("\\s", "").trim();
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + BLOOM_TABLE_NAME_ITEM + "'.");
        }

        return tableName;
    }

    public String journalDBName() {
        final String journalDBName;
        final String JOURNALDB_TABLE_NAME_ITEM = "dpl.pth_06.archive.db.journaldb.name";
        if (config.hasPath(JOURNALDB_TABLE_NAME_ITEM)) {
            final String journalDBNameFromConfig = config.getString(JOURNALDB_TABLE_NAME_ITEM);
            if (journalDBNameFromConfig == null || journalDBNameFromConfig.isEmpty()) {
                throw new RuntimeException("Journaldb name was not configured.");
            }
            journalDBName = journalDBNameFromConfig;
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + JOURNALDB_TABLE_NAME_ITEM + "'.");
        }
        return journalDBName;
    }

    private String sizesJsonString() {
        final String jsonString;
        final String BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM = "dpl.pth_06.bloom.db.fields";
        if (config.hasPath(BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM)) {
            jsonString = config.getString(BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM);
            if (jsonString == null || jsonString.isEmpty()) {
                throw new RuntimeException("Bloom filter size fields was not configured.");
            }
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM + "'.");
        }
        return jsonString;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final FilterTypes cast = (FilterTypes) object;
        return config.equals(cast.config);
    }
}
