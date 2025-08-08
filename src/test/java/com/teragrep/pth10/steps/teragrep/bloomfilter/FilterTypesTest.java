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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public final class FilterTypesTest {

    private final String username = "sa";
    private final String password = "";
    private final String connectionUrl = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    private final Connection conn = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(connectionUrl, username, password));

    @BeforeEach
    public void setup() {
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP ALL OBJECTS").execute(); // h2 clear database
        });
        String createFilterType = "CREATE TABLE `filtertype` ("
                + "`id`               bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "`expectedElements` bigint(20) NOT NULL," + "`targetFpp`        DOUBLE UNSIGNED NOT NULL,"
                + "`pattern`          VARCHAR(255) NOT NULL)";
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement(createFilterType).execute();
        });

    }

    @AfterEach
    public void teardown() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    public void testSortedMapMethod() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Map<Long, Double> resultMap = filterTypes.sortedMap();
        assertEquals(0.01, resultMap.get(1000L));
        assertEquals(0.02, resultMap.get(2000L));
        assertEquals(0.03, resultMap.get(3000L));
        assertEquals(3, resultMap.size());

    }

    @Test
    public void testSortedMapMethodWithEmptyStringJSON() {
        Config config = ConfigFactory.parseProperties(emptyStringProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Exception exception = assertThrows(RuntimeException.class, () -> {
            filterTypes.sortedMap();
        });
        Assertions.assertEquals("Bloom filter size fields was not configured.", exception.getMessage());
    }

    @Test
    public void testSortedMapMethodWithNullStringJSON() {
        Config config = ConfigFactory.parseProperties(nullStringProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Exception exception = assertThrows(RuntimeException.class, () -> {
            filterTypes.sortedMap();
        });
        Assertions.assertEquals("Bloom filter size fields was not configured.", exception.getMessage());
    }

    @Test
    public void testBitSizeMapMethod() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Map<Long, Long> bitSizeMap = filterTypes.bitSizeMap();
        Assertions.assertEquals(1000L, bitSizeMap.get(BloomFilter.create(1000, 0.01).bitSize()));
        Assertions.assertEquals(2000L, bitSizeMap.get(BloomFilter.create(2000, 0.02).bitSize()));
        Assertions.assertEquals(3000L, bitSizeMap.get(BloomFilter.create(3000, 0.03).bitSize()));
        Assertions.assertEquals(3, bitSizeMap.size());

    }

    @Test
    public void testWriteFilterTypesToDatabase() {
        String regex = "test_regex";
        Config config = ConfigFactory.parseProperties(defaultProperties());
        Assertions.assertDoesNotThrow(() -> new FilterTypes(config).saveToDatabase(regex));

        Assertions.assertDoesNotThrow(() -> {
            ResultSet result = conn.prepareStatement("SELECT * FROM filtertype").executeQuery();
            int loops = 0;
            List<Long> expectedSizeList = new ArrayList<>();
            List<Double> fppList = new ArrayList<>();
            while (result.next()) {
                expectedSizeList.add(result.getLong(2));
                fppList.add(result.getDouble(3));
                Assertions.assertEquals(regex, result.getString(4));
                loops++;
            }
            Assertions.assertEquals(3, loops);
            Assertions.assertEquals(3, expectedSizeList.size());
            Assertions.assertEquals(3, fppList.size());
            Assertions.assertEquals(Arrays.asList(1000L, 2000L, 3000L), expectedSizeList);
            Assertions.assertEquals(Arrays.asList(0.01, 0.02, 0.03), fppList);
            result.close();
        });
    }

    @Test
    public void testMalformattedFieldsOption() {
        final Properties properties = new Properties();
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "{expected: 1000, fpp: 0.01},{expected: 2000, fpp: 0.01},{expected: 3000, fpp: 0.01}"
                );
        final Config config = ConfigFactory.parseProperties(properties);
        final FilterTypes filterTypes = new FilterTypes(config);
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, filterTypes::sortedMap);
        final String expectedMessage = "Error parsing 'dpl.pth_06.bloom.db.fields' option to JSON. ensure that filter size options are formated as an JSON array and that there are no duplicate values. example '[{expected: 1000, fpp: 0.01},{expected: 2000, fpp: 0.01}]'. message:";
        Assertions.assertTrue(exception.getMessage().startsWith(expectedMessage));
    }

    @Test
    public void testDuplicateValues() {
        final Properties properties = new Properties();
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[{expected: 1000, fpp: 0.01},{expected: 1000, fpp: 0.01},{expected: 3000, fpp: 0.01}]"
                );
        final Config config = ConfigFactory.parseProperties(properties);
        final FilterTypes filterTypes = new FilterTypes(config);
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, filterTypes::sortedMap);
        final String expectedMessage = "Found duplicate values in 'dpl.pth_06.bloom.db.fields'";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testEquals() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes1 = new FilterTypes(config);
        FilterTypes filterTypes2 = new FilterTypes(config);
        filterTypes1.sortedMap();

        assertEquals(filterTypes1, filterTypes2);
    }

    @Test
    public void testNotEquals() {
        Properties properties1 = new Properties();
        Properties properties2 = new Properties();
        properties1
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[{expected: 10000, fpp: 0.01}, {expected: 20000, fpp: 0.03}, {expected: 30000, fpp: 0.05}]"
                );
        properties2
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[{expected: 20000, fpp: 0.01}, {expected: 20000, fpp: 0.03}, {expected: 30000, fpp: 0.05}]"
                );
        Config config1 = ConfigFactory.parseProperties(properties1);
        Config config2 = ConfigFactory.parseProperties(properties2);
        FilterTypes filterTypes1 = new FilterTypes(config1);
        FilterTypes filterTypes2 = new FilterTypes(config2);
        assertNotEquals(filterTypes1, filterTypes2);
    }

    @Test
    public void testEqualsVerifier() {
        EqualsVerifier.forClass(FilterTypes.class).withNonnullFields("config").verify();
    }

    private Properties defaultProperties() {
        Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.02},"
                                + "{expected: 3000, fpp: 0.03}" + "]"
                );
        return properties;
    }

    private Properties emptyStringProperties() {
        Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties.put("dpl.pth_06.bloom.db.fields", "");
        return properties;
    }

    private Properties nullStringProperties() {
        Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties.put("dpl.pth_06.bloom.db.fields", "null");
        return properties;
    }
}
