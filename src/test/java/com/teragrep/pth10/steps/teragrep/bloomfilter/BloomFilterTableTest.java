/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022, 2023, 2024  Suomen Kanuuna Oy
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
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BloomFilterTableTest {
    Properties properties;

    @BeforeAll
    void setEnv() {
        String username = "sa";
        String password = "";
        String connectionUrl = "jdbc:h2:~/test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
        this.properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        Config config = ConfigFactory.parseProperties(properties);
        Connection connection = new LazyConnection(config).get();
        Assertions.assertDoesNotThrow(() -> {
            connection.prepareStatement("DROP ALL OBJECTS").execute(); // h2 clear database
        });
        Assertions.assertDoesNotThrow(() -> {
            Class.forName("org.h2.Driver");
        });
    }

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(() -> {
            Config config = ConfigFactory.parseProperties(properties);
            Connection connection = new LazyConnection(config).get();
            connection.prepareStatement("DROP ALL OBJECTS").execute(); // h2 clear database
        });
    }

    @Test
    void testInvalidInputCharacters() {
        String injection = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        properties.put("dpl.pth_06.bloom.table.name", injection);
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable injectionTable = new BloomFilterTable(config, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, injectionTable::create);
        Assertions.assertEquals(e.getMessage(), "dpl.pth_06.bloom.table.name malformed name, only use alphabets, numbers and _");
    }

    @Test
    void testInputOverMaxLimit() {
        String tooLongName = "testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati";
        properties.put("dpl.pth_06.bloom.table.name", tooLongName);
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table = new BloomFilterTable(config, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::create);
        Assertions.assertEquals(e.getMessage(), "dpl.pth_06.bloom.table.name was too long, allowed maximum length is 100 characters");
    }

    @Test
    void testCreate() {
        String tableName = "test_table";
        properties.put("dpl.pth_06.bloom.table.name", tableName);
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table = new BloomFilterTable(config, true);
        table.create();
        String sql = "SHOW COLUMNS FROM " + tableName + ";";
        Assertions.assertDoesNotThrow(() -> {
            ResultSet rs = new LazyConnection(config).get().prepareStatement(sql)
                    .executeQuery();
            int cols = 0;
            List<String> columnList = new ArrayList<>(4);
            while (rs.next()) {
                columnList.add(rs.getString(1));
                cols++;
            }
            Assertions.assertEquals(cols, 4);
            Assertions.assertEquals(columnList.get(0), "id");
            Assertions.assertEquals(columnList.get(1), "partition_id");
            Assertions.assertEquals(columnList.get(2), "filter_type_id");
            Assertions.assertEquals(columnList.get(3), "filter");
            rs.close();
        });
    }

    @Test
    void testCreateFailure() {
        String tableName = "test_table";
        properties.put("dpl.pth_06.bloom.table.name", tableName);
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table = new BloomFilterTable(config);
        Assertions.assertThrows(RuntimeException.class, table::create);
    }
}