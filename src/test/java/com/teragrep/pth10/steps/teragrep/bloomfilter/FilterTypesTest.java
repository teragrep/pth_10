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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class FilterTypesTest {

    @Test
    public void testFieldListMethod() {
        Properties properties = new Properties();
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.01},"
                                + "{expected: 3000, fpp: 0.01}" + "]"
                );
        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes = new FilterTypes(config);
        List<FilterField> fieldList = filterTypes.fieldList();
        assertEquals(1000, fieldList.get(0).expected());
        assertEquals(2000, fieldList.get(1).expected());
        assertEquals(3000, fieldList.get(2).expected());
        assertEquals(0.01, fieldList.get(0).fpp());
        assertEquals(0.01, fieldList.get(1).fpp());
        assertEquals(0.01, fieldList.get(2).fpp());
        assertEquals(3, fieldList.size());
    }

    @Test
    public void testMalformattedFieldsOption() {
        Properties properties = new Properties();
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.01},"
                                + "{expected: 3000, fpp: 0.01}"
                );
        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes = new FilterTypes(config);
        RuntimeException exception = assertThrows(RuntimeException.class, filterTypes::fieldList);
        String expectedError = "Error reading 'dpl.pth_06.bloom.db.fields' option to JSON, check that option is formated as JSON array and that there are no duplicate values: ";
        Assertions.assertTrue(exception.getMessage().contains(expectedError));
    }

    @Test
    public void testDuplicateValues() {
        Properties properties = new Properties();
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 1000, fpp: 0.01},"
                                + "{expected: 3000, fpp: 0.01}"
                );
        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes = new FilterTypes(config);
        RuntimeException exception = assertThrows(RuntimeException.class, filterTypes::fieldList);
        String expectedError = "Error reading 'dpl.pth_06.bloom.db.fields' option to JSON, check that option is formated as JSON array and that there are no duplicate values: ";
        Assertions.assertTrue(exception.getMessage().contains(expectedError));
    }

    @Test
    public void testBitSizeMapMethod() {
        Properties properties = new Properties();
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.01},"
                                + "{expected: 3000, fpp: 0.01}" + "]"
                );

        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes = new FilterTypes(config);
        Map<Long, Long> bitSizeMap = filterTypes.bitSizeMap();
        Assertions.assertEquals(1000L, bitSizeMap.get(BloomFilter.create(1000, 0.01).bitSize()));
        Assertions.assertEquals(2000L, bitSizeMap.get(BloomFilter.create(2000, 0.01).bitSize()));
        Assertions.assertEquals(3000L, bitSizeMap.get(BloomFilter.create(3000, 0.01).bitSize()));
        Assertions.assertEquals(3, bitSizeMap.size());

    }

    @Test
    public void testPatternMethod() {
        String pattern = "test Regex ";
        Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.pattern", pattern);
        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes = new FilterTypes(config);
        assertEquals("test Regex", filterTypes.pattern());
    }

    @Test
    public void testTableTableNameMethod() {
        String name = "test Table ";
        Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.table.name", name);
        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes = new FilterTypes(config);
        assertEquals("testTable", filterTypes.tableName());
    }

    @Test
    public void testEquals() {
        Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.table.name", "test");
        properties.put("dpl.pth_06.bloom.pattern", "pattern");
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.01},"
                                + "{expected: 3000, fpp: 0.01}" + "]"
                );
        Config config = ConfigFactory.parseProperties(properties);
        FilterTypes filterTypes1 = new FilterTypes(config);
        FilterTypes filterTypes2 = new FilterTypes(config);
        filterTypes1.fieldList();
        filterTypes1.pattern();
        filterTypes1.tableName();
        filterTypes1.bitSizeMap();
        assertEquals(filterTypes1, filterTypes2);
    }

    @Test
    public void testNotEquals() {
        Properties properties1 = new Properties();
        properties1.put("dpl.pth_06.bloom.table.name", "test");
        Properties properties2 = new Properties();
        properties2.put("dpl.pth_06.bloom.table.name", "not_test");
        Config config1 = ConfigFactory.parseProperties(properties1);
        Config config2 = ConfigFactory.parseProperties(properties2);
        FilterTypes filterTypes1 = new FilterTypes(config1);
        FilterTypes filterTypes2 = new FilterTypes(config2);
        assertNotEquals(filterTypes1, filterTypes2);
    }
}
