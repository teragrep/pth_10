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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class FilterSizesTest {

    @Test
    public void filterSizeMapTest() {

        Properties properties = new Properties();

        properties.put("dpl.pth_06.bloom.db.fields", "[" +
                "{expected: 1000, fpp: 0.01}," +
                "{expected: 2000, fpp: 0.01}," +
                "{expected: 3000, fpp: 0.01}" +
                "]");

        Config config = ConfigFactory.parseProperties(properties);
        FilterSizes sizeMap = new FilterSizes(config);

        Map<Long, Double> resultMap = sizeMap.asSortedMap();

        assertEquals(0.01, resultMap.get(1000L));
        assertEquals(0.01, resultMap.get(2000L));
        assertEquals(0.01, resultMap.get(3000L));
        assertEquals(3, resultMap.size());

    }
    @Test
    public void bitSizeMapTest() {

        Properties properties = new Properties();

        properties.put("dpl.pth_06.bloom.db.fields", "[" +
                "{expected: 1000, fpp: 0.01}," +
                "{expected: 2000, fpp: 0.01}," +
                "{expected: 3000, fpp: 0.01}" +
                "]");

        Config config = ConfigFactory.parseProperties(properties);
        FilterSizes sizeMap = new FilterSizes(config);

        Map<Long, Long> bitSizeMap = sizeMap.asBitsizeSortedMap();

        assertEquals(1000L,
                bitSizeMap.get(BloomFilter.create(1000,0.01).bitSize())
        );
        assertEquals(2000L,
                bitSizeMap.get(BloomFilter.create(2000,0.01).bitSize())
        );
        assertEquals(3000L,
                bitSizeMap.get(BloomFilter.create(3000,0.01).bitSize())
        );
        assertEquals(3, bitSizeMap.size());

    }
}