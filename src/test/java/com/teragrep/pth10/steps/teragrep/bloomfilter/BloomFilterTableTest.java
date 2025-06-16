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
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BloomFilterTableTest {

    private final String username = "sa";
    private final String password = "";
    private final String connectionUrl = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    private final Connection connection = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(connectionUrl, username, password));

    @AfterAll
    void tearDown() {
        Assertions.assertDoesNotThrow(connection::close);
    }

    @Test
    public void testInvalidInputCharacters() {
        Properties properties = getDefaultProperties();
        String injection = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable injectionTable = new BloomFilterTable(config, injection, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, injectionTable::create);
        Assertions
                .assertEquals(
                        "malformed SQL input <[test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')]>, only use alphabets, numbers and _",
                        e.getMessage()
                );
    }

    @Test
    void testInputOverMaxLimit() {
        Properties properties = getDefaultProperties();
        String tooLongName = "testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati";
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table = new BloomFilterTable(config, tooLongName, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::create);
        Assertions
                .assertEquals(
                        "SQL input <[testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati]> was too long, allowed maximum length is 100 characters",
                        e.getMessage()
                );
    }

    @Test
    void testCreateToDatabase() {
        Properties properties = getDefaultProperties();
        String tableName = "test_table";
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table = new BloomFilterTable(config, tableName, true);
        table.create();
        String sql = "SHOW COLUMNS FROM " + tableName + ";";
        Assertions.assertDoesNotThrow(() -> {
            ResultSet rs = connection.prepareStatement(sql).executeQuery();
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
    void testCreateExceptionOnConstraintFailure() {
        Properties properties = getDefaultProperties();
        String tableName = "test_table";
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table = new BloomFilterTable(config, tableName);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::create);
        String expectedMessage = "Error creating bloom filter table: org.h2.jdbc.JdbcSQLSyntaxErrorException: Schema \"journaldb\" not found; SQL statement:\n"
                + "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL,CONSTRAINT `test_table_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)ON DELETE CASCADE,CONSTRAINT `test_table_ibfk_2` FOREIGN KEY (partition_id) REFERENCES journaldb.logfile (id)ON DELETE CASCADE); [90079-224]";
        Assertions.assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    void testEquals() {
        String tableName1 = "test_table";
        Properties properties = getDefaultProperties();
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table1 = new BloomFilterTable(config, tableName1, true);
        BloomFilterTable table2 = new BloomFilterTable(config, tableName1, true);
        table1.create();
        Assertions.assertEquals(table1, table2);
    }

    @Test
    void testNotEqualsName() {
        String tableName1 = "test_table";
        String tableName2 = "table_test";
        Properties properties = getDefaultProperties();
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table1 = new BloomFilterTable(config, tableName1, true);
        BloomFilterTable table2 = new BloomFilterTable(config, tableName2, true);
        table1.create();
        Assertions.assertNotEquals(table1, table2);
    }

    @Test
    void testNotEqualsIgnoreConstraints() {
        Properties properties = getDefaultProperties();
        String tableName = "test_table";
        Config config = ConfigFactory.parseProperties(properties);
        BloomFilterTable table1 = new BloomFilterTable(config, tableName, true);
        BloomFilterTable table2 = new BloomFilterTable(config, tableName, false);
        table1.create();
        Assertions.assertNotEquals(table1, table2);
    }

    @Test
    void testEqualsVerifier() {
        EqualsVerifier.forClass(BloomFilterTable.class).withNonnullFields("tableSQL", "conn").verify();
    }

    private Properties getDefaultProperties() {
        final Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties.put("dpl.pth_06.archive.db.journaldb.name", "journaldb");
        return properties;
    }
}
