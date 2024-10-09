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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TeragrepBloomFilterTest {

    private final String pattern = "[a-zA-Z]*$";
    private LazyConnection lazyConnection;
    private FilterTypes filterTypes;
    private final BloomFilter emptyFilter = BloomFilter.create(100, 0.01);
    private List<FilterField> filterFields;
    private final String tableName = "bloomfilter_test";

    @BeforeAll
    void setEnv() {
        Properties properties = new Properties();
        String username = "sa";
        properties.put("dpl.pth_10.bloom.db.username", username);
        String password = "";
        properties.put("dpl.pth_10.bloom.db.password", password);
        String connectionUrl = "jdbc:h2:~/test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 10000, fpp: 0.01}," + "{expected: 20000, fpp: 0.03},"
                                + "{expected: 30000, fpp: 0.05}" + "]"
                );
        properties.put("dpl.pth_06.bloom.pattern", pattern);
        properties.put("dpl.pth_06.bloom.table.name", tableName);
        Config config = ConfigFactory.parseProperties(properties);
        lazyConnection = new LazyConnection(config);
        Connection conn = lazyConnection.get();
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP ALL OBJECTS").execute(); // h2 clear database
        });
        Assertions.assertDoesNotThrow(() -> {
            Class.forName("org.h2.Driver");
        });
        filterTypes = new FilterTypes(config);
        filterFields = filterTypes.fieldList();
        String createFilterType = "CREATE TABLE `filtertype` ("
                + "`id`               bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "`expectedElements` bigint(20) NOT NULL," + "`targetFpp`        DOUBLE UNSIGNED NOT NULL,"
                + "`pattern`          VARCHAR(255) NOT NULL)";
        String insertSql = "INSERT INTO `filtertype` " + "(`expectedElements`, `targetFpp`, `pattern`)"
                + " VALUES (?, ?, ?)";
        String createTable = "CREATE TABLE `" + tableName + "` ("
                + "    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "    `partition_id` BIGINT(20) UNSIGNED NOT NULL,"
                + "    `filter_type_id` BIGINT(20) UNSIGNED NOT NULL," + "    `filter` LONGBLOB NOT NULL" + ");";
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement(createFilterType).execute();
            conn.prepareStatement(createTable).execute();
        });
        int loops = 0;
        for (FilterField field : filterFields) {
            loops++;
            Assertions.assertDoesNotThrow(() -> {
                PreparedStatement stmt = conn.prepareStatement(insertSql);
                stmt.setInt(1, field.expectedIntValue()); // filtertype.expectedElements
                stmt.setDouble(2, field.fpp()); // filtertype.targetFpp
                stmt.setString(3, pattern);
                stmt.executeUpdate();
                stmt.clearParameters();
                conn.commit();
            });
        }
        Assertions.assertEquals(3, loops);
    }

    @AfterAll
    public void tearDown() {
        Connection conn = lazyConnection.get();
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP ALL OBJECTS").execute(); // h2 clear database
        });
    }

    // -- Tests --

    @Test
    void testSavingToDatabase() {
        List<String> tokens = new ArrayList<>(Collections.singletonList("one"));
        Row row = generatedRow(filterFields, tokens);
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        BloomFilter rawFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(filterBytes)));
        TeragrepBloomFilter filter = new TeragrepBloomFilter(partition, rawFilter, lazyConnection.get(), filterTypes);
        filter.saveFilter(false);
        FilterField first = filterFields.get(0);
        String sql = "SELECT `filter` FROM `" + tableName + "`";
        Assertions.assertDoesNotThrow(() -> {
            ResultSet rs = lazyConnection.get().prepareStatement(sql).executeQuery();
            int cols = rs.getMetaData().getColumnCount();
            BloomFilter resultFilter = emptyFilter;
            int loops = 0;
            while (rs.next()) {
                byte[] bytes = rs.getBytes(1);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                resultFilter = BloomFilter.readFrom(bais);
                loops++;
            }
            Assertions.assertEquals(4, loops);
            Assertions.assertNotNull(resultFilter);
            Assertions.assertEquals(1, cols);
            Assertions.assertTrue(resultFilter.mightContain("one"));
            Assertions.assertFalse(resultFilter.mightContain("neo"));
            Assertions.assertTrue(resultFilter.expectedFpp() <= first.expected());
            rs.close();
        });
    }

    @Test
    void testSavingToDatabaseWithOverwrite() {
        List<String> tokens = new ArrayList<>(Collections.singletonList("one"));
        Row row = generatedRow(filterFields, tokens);
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        BloomFilter rawFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(filterBytes)));
        TeragrepBloomFilter filter = new TeragrepBloomFilter(partition, rawFilter, lazyConnection.get(), filterTypes);
        filter.saveFilter(true);
        String sql = "SELECT `filter` FROM `" + tableName + "`";
        Assertions.assertDoesNotThrow(() -> {
            BloomFilter resultFilter = emptyFilter;
            ResultSet rs = lazyConnection.get().prepareStatement(sql).executeQuery();
            while (rs.next()) {
                byte[] bytes = rs.getBytes(1);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                resultFilter = BloomFilter.readFrom(bais);
            }
            int cols = rs.getMetaData().getColumnCount();
            Assertions.assertNotNull(resultFilter);
            Assertions.assertEquals(1, cols);
            Assertions.assertTrue(resultFilter.mightContain("one"));
            Assertions.assertFalse(resultFilter.mightContain("neo"));
            Assertions.assertTrue(resultFilter.expectedFpp() <= 0.01D);
            Assertions.assertDoesNotThrow(rs::close);
        });
        // Create second filter that will overwrite first one
        List<String> secondTokens = new ArrayList<>(Collections.singletonList("neo"));
        Row secondRow = generatedRow(filterFields, secondTokens);
        String secondPartition = secondRow.getString(0);
        byte[] secondFilterBytes = (byte[]) secondRow.get(1);
        BloomFilter rawFilter2 = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(secondFilterBytes)));
        TeragrepBloomFilter secondFilter = new TeragrepBloomFilter(
                secondPartition,
                rawFilter2,
                lazyConnection.get(),
                filterTypes
        );
        secondFilter.saveFilter(true);
        String secondSql = "SELECT `filter` FROM `" + tableName + "`";
        Assertions.assertDoesNotThrow(() -> {
            ResultSet secondRs = lazyConnection.get().prepareStatement(secondSql).executeQuery();
            BloomFilter secondResultFilter = emptyFilter;
            while (secondRs.next()) {
                byte[] bytes = secondRs.getBytes(1);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                secondResultFilter = BloomFilter.readFrom(bais);
            }
            int secondCols = secondRs.getMetaData().getColumnCount();

            Assertions.assertNotNull(secondResultFilter);
            Assertions.assertEquals(1, secondCols);
            Assertions.assertFalse(secondResultFilter.mightContain("one"));
            Assertions.assertTrue(secondResultFilter.mightContain("neo"));
            Assertions.assertTrue(secondResultFilter.expectedFpp() <= 0.01D);
            secondRs.close();
        });
    }

    @Test
    void testCorrectFilterSizeSelection() {
        List<String> tokens = new ArrayList<>();
        tokens.add("one");
        for (int i = 1; i < 1500; i++) {
            tokens.add("token:" + i);
        }
        Row row = generatedRow(filterFields, tokens);
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        BloomFilter rawFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(filterBytes)));
        TeragrepBloomFilter filter = new TeragrepBloomFilter(partition, rawFilter, lazyConnection.get(), filterTypes);
        filter.saveFilter(false);
        long size = Long.MAX_VALUE;
        FilterField current = null;
        for (FilterField field : filterFields) {
            long expected = field.expected();
            if (size > expected && expected >= tokens.size()) {
                size = expected;
                current = field;
            }
        }
        String sql = "SELECT `filter` FROM `" + tableName + "`";
        FilterField finalCurrent = current;
        Assertions.assertDoesNotThrow(() -> {
            ResultSet rs = lazyConnection.get().prepareStatement(sql).executeQuery();
            int cols = rs.getMetaData().getColumnCount();
            BloomFilter resultFilter = emptyFilter;
            int loops = 0;
            while (rs.next()) {
                loops++;
                byte[] bytes = rs.getBytes(1);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                resultFilter = BloomFilter.readFrom(bais);
            }
            Assertions.assertEquals(1, loops);
            Assertions.assertNotNull(resultFilter);
            Assertions.assertEquals(1, cols);
            Assertions.assertTrue(resultFilter.mightContain("one"));
            Assertions.assertFalse(resultFilter.mightContain("neo"));
            Assertions.assertNotNull(finalCurrent);
            Assertions.assertTrue(resultFilter.expectedFpp() <= finalCurrent.fpp());
            rs.close();
        });
    }

    @Test
    public void testPatternSavedToDatabase() {
        String sql = "SELECT `pattern` FROM `filtertype` GROUP BY `pattern`";
        Assertions.assertDoesNotThrow(() -> {
            ResultSet rs = lazyConnection.get().prepareStatement(sql).executeQuery();
            String pattern = "";
            while (rs.next()) {
                pattern = rs.getString(1);
            }
            Assertions.assertEquals(this.pattern, pattern);
        });
    }

    @Test
    public void testEquals() {
        List<String> tokens = new ArrayList<>(Collections.singletonList("one"));
        Row row = generatedRow(filterFields, tokens);
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        BloomFilter rawFilter = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream(filterBytes)));
        TeragrepBloomFilter filter1 = new TeragrepBloomFilter(partition, rawFilter, lazyConnection.get(), filterTypes);
        TeragrepBloomFilter filter2 = new TeragrepBloomFilter(partition, rawFilter, lazyConnection.get(), filterTypes);
        filter1.saveFilter(false);
        Assertions.assertEquals(filter1, filter2);
    }

    @Test
    public void testNotEqualsTokens() {
        List<String> tokens1 = new ArrayList<>(Collections.singletonList("one"));
        List<String> tokens2 = new ArrayList<>(Collections.singletonList("two"));
        Row row1 = generatedRow(filterFields, tokens1);
        Row row2 = generatedRow(filterFields, tokens2);
        BloomFilter rawFilter1 = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream((byte[]) row1.get(1))));
        BloomFilter rawFilter2 = Assertions
                .assertDoesNotThrow(() -> BloomFilter.readFrom(new ByteArrayInputStream((byte[]) row2.get(1))));
        TeragrepBloomFilter filter1 = new TeragrepBloomFilter(
                row1.getString(0),
                rawFilter1,
                lazyConnection.get(),
                filterTypes
        );
        TeragrepBloomFilter filter2 = new TeragrepBloomFilter(
                row2.getString(0),
                rawFilter2,
                lazyConnection.get(),
                filterTypes
        );
        Assertions.assertNotEquals(filter1, filter2);
    }

    // -- Helper methods --

    private Row generatedRow(List<FilterField> fieldList, List<String> tokens) {
        FilterField current = fieldList.get(fieldList.size() - 1);
        for (FilterField field : fieldList) {
            Long expected = field.expected();
            if (expected < current.expected() && expected >= tokens.size()) {
                current = field;
            }
        }
        BloomFilter bf = BloomFilter.create(current.expectedIntValue(), current.fpp());
        tokens.forEach(bf::put);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Assertions.assertDoesNotThrow(() -> {
            bf.writeTo(baos);
        });
        return RowFactory.create("1", baos.toByteArray());
    }
}
