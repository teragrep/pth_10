/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022, 2023  Suomen Kanuuna Oy
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

package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;
import org.apache.spark.util.sketch.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static com.teragrep.pth10.steps.teragrep.TeragrepBloomStep.BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM;

public class FilterSizes implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterSizes.class);

    private final Config config;
    private final ArrayList<SortedMap<Long,Double>> mapCache = new ArrayList<>(1);
    private final ArrayList<Map<Long,Long>> bitSizeMapCache = new ArrayList<>(1);

    public FilterSizes(Config config) {
        this.config = config;
    }

    /**
     * Filter sizes as sorted map
     * <p>
     * Keys = filter expected num of items,
     * values = filter FPP
     * @return SortedMap of filter configuration
     */
    public SortedMap<Long, Double> asSortedMap() {
        if (mapCache.isEmpty()) {
            SortedMap<Long, Double> resultMap = mapFromConfig();
            mapCache.add(resultMap);
        }
        return mapCache.get(0);
    }

    public Map<Long, Long> asBitsizeSortedMap() {
        if (bitSizeMapCache.isEmpty()) {
            Map<Long, Double> filterSizes = asSortedMap();
            Map<Long, Long> bitsizeToExpectedItemsMap = new HashMap<>();
            // Calculate bitSizes
            for (Map.Entry<Long, Double> entry : filterSizes.entrySet()) {
                BloomFilter bf = BloomFilter.create(entry.getKey(), entry.getValue());
                bitsizeToExpectedItemsMap.put(bf.bitSize(), entry.getKey());
            }
            bitSizeMapCache.add(bitsizeToExpectedItemsMap);
        }
        return bitSizeMapCache.get(0);
    }

    private SortedMap<Long, Double> mapFromConfig() {

        SortedMap<Long, Double> sizesMapFromJson = new TreeMap<>();
        Gson gson = new Gson();

        List<JsonObject> jsonArray = gson.fromJson(
                sizesJsonString(),
                new TypeToken<List<JsonObject>>(){}.getType()
        );

        for (JsonObject object : jsonArray) {
            if (object.has("expected") && object.has("fpp")) {
                Long expectedNumOfItems = Long.parseLong(object.get("expected").toString());
                Double fpp = Double.parseDouble(object.get("fpp").toString());
                if (sizesMapFromJson.containsKey(expectedNumOfItems)) {
                    LOGGER.error("Duplicate value of expected number of items value: <{}>", expectedNumOfItems);
                    throw new RuntimeException("Duplicate entry expected num of items");
                }
                sizesMapFromJson.put(expectedNumOfItems, fpp);
            } else {
                throw new RuntimeException("JSON did not have expected values of 'expected' or 'fpp'");
            }
        }
        return sizesMapFromJson;
    }

    private String sizesJsonString() {
        String jsonString;
        if (config.hasPath(BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM)) {
            jsonString = config.getString(BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM);
            if (jsonString == null || jsonString.isEmpty()) {
                throw new RuntimeException("Bloom filter fields not configured.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + BLOOM_NUMBER_OF_FIELDS_CONFIG_ITEM + "'.");
        }
        return jsonString;
    }
}
