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
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TeragrepBloomFilterTest {

    private final String username = "sa";
    private final String password = "";
    private final String connectionUrl = "jdbc:h2:~/test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    private LazyConnection lazyConnection;
    private FilterSizes filterSizes;
    private final BloomFilter emptyFilter = BloomFilter.create(100, 0.01);

    private SortedMap<Long, Double> sizeMap;

    @BeforeAll
    void setEnv() throws ClassNotFoundException, SQLException {

        Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 10000, fpp: 0.01}," + "{expected: 20000, fpp: 0.03},"
                                + "{expected: 30000, fpp: 0.05}" + "]"
                );

        Config config = ConfigFactory.parseProperties(properties);
        lazyConnection = new LazyConnection(config);
        Connection conn = lazyConnection.get();

        conn.prepareStatement("DROP ALL OBJECTS").execute(); //h2 clear database

        filterSizes = new FilterSizes(config);
        sizeMap = filterSizes.asSortedMap();

        Class.forName("org.h2.Driver");
        sizeMap.put(10000L, 0.01);
        sizeMap.put(20000L, 0.03);
        sizeMap.put(30000L, 0.05);

        String createFilterType = "CREATE TABLE `filtertype` ("
                + "`id`               bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "`expectedElements` bigint(20) NOT NULL," + "`targetFpp`        DOUBLE UNSIGNED NOT NULL" + ");";

        String createBloomFilter = "CREATE TABLE `bloomfilter` ("
                + "    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "    `partition_id` BIGINT(20) UNSIGNED NOT NULL,"
                + "    `filter_type_id` BIGINT(20) UNSIGNED NOT NULL," + "    `filter` LONGBLOB NOT NULL" + ");";

        String insertSql = "INSERT INTO `filtertype` (`expectedElements`, `targetFpp`) VALUES (?, ?)";

        conn.prepareStatement(createFilterType).execute();
        conn.prepareStatement(createBloomFilter).execute();

        for (Map.Entry<Long, Double> entry : sizeMap.entrySet()) {
            try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {

                stmt.setInt(1, entry.getKey().intValue()); // filtertype.expectedElements
                stmt.setDouble(2, entry.getValue()); // filtertype.targetFpp
                stmt.executeUpdate();
                stmt.clearParameters();
                conn.commit();
            }
        }

        //ResultSet rs = conn.prepareStatement("SELECT * FROM `filtertype`;").executeQuery();
    }

    @AfterEach
    void afterEach() throws SQLException {
        lazyConnection.get().prepareStatement("TRUNCATE TABLE `bloomfilter`").execute();
    }

    // -- Tests --

    @Test
    void filterSaveNoOverwriteTest() {

        List<String> tokens = new ArrayList<>(Collections.singletonList("one"));
        Row row = Assertions.assertDoesNotThrow(() -> generatedRow(sizeMap, tokens));
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        TeragrepBloomFilter filter = new TeragrepBloomFilter(partition, filterBytes, lazyConnection.get(), filterSizes);
        filter.saveFilter(false);

        Map.Entry<Long, Double> entry = sizeMap.entrySet().iterator().next();
        String sql = "SELECT `filter` FROM `bloomfilter`;";

        ResultSet rs = Assertions.assertDoesNotThrow(() -> lazyConnection.get().prepareStatement(sql).executeQuery());

        int cols = Assertions.assertDoesNotThrow(() -> rs.getMetaData().getColumnCount());
        BloomFilter resultFilter = emptyFilter;

        while (Assertions.assertDoesNotThrow(() -> rs.next())) {
            byte[] bytes = Assertions.assertDoesNotThrow(() -> rs.getBytes(1));
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            resultFilter = Assertions.assertDoesNotThrow(() -> BloomFilter.readFrom(bais));
        }

        Assertions.assertNotNull(resultFilter);
        Assertions.assertEquals(1, cols);
        Assertions.assertTrue(resultFilter.mightContain("one"));
        Assertions.assertFalse(resultFilter.mightContain("neo"));
        Assertions.assertTrue(resultFilter.expectedFpp() <= entry.getValue());

        Assertions.assertDoesNotThrow(rs::close);
    }

    @Test
    void filterSaveOverwriteTest() {

        List<String> tokens = new ArrayList<>(Collections.singletonList("one"));
        Row row = Assertions.assertDoesNotThrow(() -> generatedRow(sizeMap, tokens));
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        TeragrepBloomFilter filter = new TeragrepBloomFilter(partition, filterBytes, lazyConnection.get(), filterSizes);
        filter.saveFilter(true);

        String sql = "SELECT `filter` FROM `bloomfilter`;";

        ResultSet rs = Assertions.assertDoesNotThrow(() -> lazyConnection.get().prepareStatement(sql).executeQuery());

        int cols = Assertions.assertDoesNotThrow(() -> rs.getMetaData().getColumnCount());
        BloomFilter resultFilter = emptyFilter;

        while (Assertions.assertDoesNotThrow(() -> rs.next())) {
            byte[] bytes = Assertions.assertDoesNotThrow(() -> rs.getBytes(1));
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            resultFilter = Assertions.assertDoesNotThrow(() -> BloomFilter.readFrom(bais));
        }

        Assertions.assertNotNull(resultFilter);
        Assertions.assertEquals(1, cols);
        Assertions.assertTrue(resultFilter.mightContain("one"));
        Assertions.assertFalse(resultFilter.mightContain("neo"));
        Assertions.assertTrue(resultFilter.expectedFpp() <= 0.01D);
        Assertions.assertDoesNotThrow(rs::close);

        // Create second filter that will overwrite first one

        List<String> secondTokens = new ArrayList<>(Collections.singletonList("neo"));
        Row secondRow = Assertions.assertDoesNotThrow(() -> generatedRow(sizeMap, secondTokens));
        String secondPartition = secondRow.getString(0);
        byte[] secondFilterBytes = (byte[]) secondRow.get(1);
        TeragrepBloomFilter secondFilter = new TeragrepBloomFilter(
                secondPartition,
                secondFilterBytes,
                lazyConnection.get(),
                filterSizes
        );
        secondFilter.saveFilter(true);

        String secondSql = "SELECT `filter` FROM `bloomfilter`;";

        ResultSet secondRs = Assertions
                .assertDoesNotThrow(() -> lazyConnection.get().prepareStatement(secondSql).executeQuery());

        int secondCols = Assertions.assertDoesNotThrow(() -> secondRs.getMetaData().getColumnCount());
        BloomFilter secondResultFilter = emptyFilter;

        while (Assertions.assertDoesNotThrow(() -> secondRs.next())) {
            byte[] bytes = Assertions.assertDoesNotThrow(() -> secondRs.getBytes(1));
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            secondResultFilter = Assertions.assertDoesNotThrow(() -> BloomFilter.readFrom(bais));
        }

        Assertions.assertNotNull(secondResultFilter);
        Assertions.assertEquals(1, secondCols);
        Assertions.assertFalse(secondResultFilter.mightContain("one"));
        Assertions.assertTrue(secondResultFilter.mightContain("neo"));
        Assertions.assertTrue(secondResultFilter.expectedFpp() <= 0.01D);
        Assertions.assertDoesNotThrow(secondRs::close);

    }

    @Test
    void correctSizeSelectionTest() {
        List<String> tokens = new ArrayList<>();
        tokens.add("one");
        for (int i = 1; i < 1500; i++) {
            tokens.add("token:" + i);
        }
        Row row = Assertions.assertDoesNotThrow(() -> generatedRow(sizeMap, tokens));
        String partition = row.getString(0);
        byte[] filterBytes = (byte[]) row.get(1);
        TeragrepBloomFilter filter = new TeragrepBloomFilter(partition, filterBytes, lazyConnection.get(), filterSizes);
        filter.saveFilter(false);

        long size = Long.MAX_VALUE;

        for (long key : sizeMap.keySet()) {
            if (size > key && key >= tokens.size()) {
                size = key;
            }
        }

        Double fpp = sizeMap.get(size);
        String sql = "SELECT `filter` FROM `bloomfilter`;";

        ResultSet rs = Assertions.assertDoesNotThrow(() -> lazyConnection.get().prepareStatement(sql).executeQuery());

        int cols = Assertions.assertDoesNotThrow(() -> rs.getMetaData().getColumnCount());
        BloomFilter resultFilter = emptyFilter;

        while (Assertions.assertDoesNotThrow(() -> rs.next())) {
            byte[] bytes = Assertions.assertDoesNotThrow(() -> rs.getBytes(1));
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            resultFilter = Assertions.assertDoesNotThrow(() -> BloomFilter.readFrom(bais));
        }

        Assertions.assertNotNull(resultFilter);
        Assertions.assertEquals(1, cols);
        Assertions.assertTrue(resultFilter.mightContain("one"));
        Assertions.assertFalse(resultFilter.mightContain("neo"));
        Assertions.assertTrue(resultFilter.expectedFpp() <= fpp);

        Assertions.assertDoesNotThrow(rs::close);
    }

    // -- Helper methods --

    private Row generatedRow(SortedMap<Long, Double> filterMap, List<String> tokens) throws IOException {
        long size = filterMap.lastKey();

        for (long key : filterMap.keySet()) {
            if (key < size && key >= tokens.size()) {
                size = key;
            }
        }

        BloomFilter bf = BloomFilter.create(size, filterMap.get(size));

        for (String token : tokens) {
            bf.put(token);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bf.writeTo(baos);

        return RowFactory.create("1", baos.toByteArray());
    }
}
