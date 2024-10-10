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
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.reflect.TypeToken;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
    public List<FilterField> fieldList() {
        final List<FilterField> fieldList;
        try {
            final Gson gson = new Gson();
            fieldList = gson.fromJson(sizesJsonString(), new TypeToken<List<FilterField>>() {
            }.getType());
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Error reading 'dpl.pth_06.bloom.db.fields' option to JSON, check that option is formated as JSON array and that there are no duplicate values: "
                            + e.getMessage()
            );
        }
        final boolean hasDuplicates = new HashSet<>(fieldList).size() != fieldList.size();
        if (hasDuplicates) {
            throw new RuntimeException("Found duplicate values in 'dpl.pth_06.bloom.db.fields'");
        }
        return fieldList;
    }

    public Map<Long, Long> bitSizeMap() {
        final List<FilterField> fieldList = fieldList();
        System.out.println(fieldList);
        final Map<Long, Long> bitsizeToExpectedItemsMap = new HashMap<>();
        // Calculate bitSizes
        for (final FilterField field : fieldList) {
            bitsizeToExpectedItemsMap.put(field.bitSize(), field.expected());
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
