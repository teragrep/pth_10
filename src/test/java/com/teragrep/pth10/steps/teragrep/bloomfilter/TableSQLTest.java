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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TableSQLTest {

    @Test
    public void testCreateTableSQL() {
        String name = "test_table";
        TableSQL table = new TableSQL(name);
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL,CONSTRAINT `test_table_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)ON DELETE CASCADE,CONSTRAINT `test_table_ibfk_2` FOREIGN KEY (partition_id) REFERENCES journaldb.logfile (id)ON DELETE CASCADE);";
        Assertions.assertEquals(e, table.createTableSQL());
    }

    @Test
    public void testCreateTableSQLJournalDBOption() {
        String name = "test_table";
        TableSQL table = new TableSQL(name, "customName");
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL,CONSTRAINT `test_table_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)ON DELETE CASCADE,CONSTRAINT `test_table_ibfk_2` FOREIGN KEY (partition_id) REFERENCES customName.logfile (id)ON DELETE CASCADE);";
        Assertions.assertEquals(e, table.createTableSQL());
    }

    @Test
    public void testCreateTableSQLInvalidJournalDBOption() {
        String name = "test_table";
        TableSQL table = new TableSQL(name, "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')");
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::createTableSQL);
        Assertions
                .assertEquals(
                        "malformed SQL input <[test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')]>, only use alphabets, numbers and _",
                        e.getMessage()
                );
    }

    @Test
    public void testCreateTableSQLInvalidJournalDBOptionIgnoreConstraints() {
        String name = "test_table";
        // not validated when ignoreConstraint set to true
        String ignoredInput = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        TableSQL table = new TableSQL(name, ignoredInput, true);
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL);";
        Assertions.assertEquals(e, table.createTableSQL());
    }

    @Test
    public void testIgnoreConstraintsCreateTableSQL() {
        String name = "test_table";
        TableSQL table = new TableSQL(name, true);
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL);";
        Assertions.assertEquals(e, table.createTableSQL());
    }

    @Test
    public void testInvalidInputCharacters() {
        String injection = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        TableSQL table = new TableSQL(injection);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::createTableSQL);
        Assertions
                .assertEquals(
                        "malformed SQL input <[test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')]>, only use alphabets, numbers and _",
                        e.getMessage()
                );
    }

    @Test
    public void testInvalidInputCharactersIgnoreConstraintsCreateTableSQL() {
        String injection = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        TableSQL table = new TableSQL(injection, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::createTableSQL);
        Assertions
                .assertEquals(
                        "malformed SQL input <[test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')]>, only use alphabets, numbers and _",
                        e.getMessage()
                );
    }

    @Test
    public void testInputOverMaxLimit() {
        String tooLongName = "testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati";
        TableSQL table = new TableSQL(tooLongName);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::createTableSQL);
        Assertions
                .assertEquals(
                        "SQL input <[testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati]> was too long, allowed maximum length is 100 characters",
                        e.getMessage()
                );
    }

    @Test
    public void testInputOverMaxLimitIgnoreConstraintsCreateTableSQL() {
        String tooLongName = "testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati";
        TableSQL table = new TableSQL(tooLongName, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::createTableSQL);
        Assertions
                .assertEquals(
                        "SQL input <[testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati]> was too long, allowed maximum length is 100 characters",
                        e.getMessage()
                );
    }

    @Test
    public void testEquality() {
        String name = "test_table";
        TableSQL table1 = new TableSQL(name);
        TableSQL table2 = new TableSQL(name);
        Assertions.assertEquals(table1, table2);
    }

    @Test
    public void testNotEqualNames() {
        TableSQL table1 = new TableSQL("table_1");
        TableSQL table2 = new TableSQL("table_2");
        Assertions.assertNotEquals(table1, table2);
    }

    @Test
    public void testNotEqualIgnoreConstraints() {
        TableSQL table1 = new TableSQL("table_1", true);
        TableSQL table2 = new TableSQL("table_2");
        Assertions.assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsVerifier() {
        EqualsVerifier
                .forClass(TableSQL.class)
                .withNonnullFields("validPattern", "name", "journalDBName", "ignoreConstraints")
                .verify();
    }
}
