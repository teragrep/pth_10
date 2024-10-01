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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CreateTableSQLTest {

    @Test
    public void testSql() {
        String name = "test_table";
        CreateTableSQL table = new CreateTableSQL(name);
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL,CONSTRAINT `test_table_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)ON DELETE CASCADE,CONSTRAINT `test_table_ibfk_2` FOREIGN KEY (partition_id) REFERENCES journaldb.logfile (id)ON DELETE CASCADE);";
        Assertions.assertEquals(e, table.sql());
    }

    @Test
    public void testIgnoreConstraintsSql() {
        String name = "test_table";
        CreateTableSQL table = new CreateTableSQL(name, true);
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL);";
        Assertions.assertEquals(e, table.sql());
    }

    @Test
    public void testInvalidInputCharacters() {
        String injection = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        CreateTableSQL table = new CreateTableSQL(injection);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::sql);
        Assertions
                .assertEquals(
                        "dpl.pth_06.bloom.table.name malformed name, only use alphabets, numbers and _", e.getMessage()
                );
    }

    @Test
    public void testInvalidInputCharactersIgnoreConstraintsSql() {
        String injection = "test;%00SELECT%00CONCAT('DROP%00TABLE%00IF%00EXISTS`',table_name,'`;')";
        CreateTableSQL table = new CreateTableSQL(injection, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::sql);
        Assertions
                .assertEquals(
                        "dpl.pth_06.bloom.table.name malformed name, only use alphabets, numbers and _", e.getMessage()
                );
    }

    @Test
    public void testInputOverMaxLimit() {
        String tooLongName = "testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati";
        CreateTableSQL table = new CreateTableSQL(tooLongName);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::sql);
        Assertions
                .assertEquals(
                        "dpl.pth_06.bloom.table.name was too long, allowed maximum length is 100 characters",
                        e.getMessage()
                );
    }

    @Test
    public void testInputOverMaxLimitIgnoreConstraintsSql() {
        String tooLongName = "testname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestname_thatistoolongtestnamethati";
        CreateTableSQL table = new CreateTableSQL(tooLongName, true);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::sql);
        Assertions
                .assertEquals(
                        "dpl.pth_06.bloom.table.name was too long, allowed maximum length is 100 characters",
                        e.getMessage()
                );
    }

    @Test
    public void testEquality() {
        String name = "test_table";
        CreateTableSQL table1 = new CreateTableSQL(name);
        CreateTableSQL table2 = new CreateTableSQL(name);
        Assertions.assertEquals(table1, table2);
    }

    @Test
    public void testNotEqualNames() {
        CreateTableSQL table1 = new CreateTableSQL("table_1");
        CreateTableSQL table2 = new CreateTableSQL("table_2");
        Assertions.assertNotEquals(table1, table2);
    }

    @Test
    public void testNotEqualIgnoreConstraints() {
        CreateTableSQL table1 = new CreateTableSQL("table_1", true);
        CreateTableSQL table2 = new CreateTableSQL("table_2");
        Assertions.assertNotEquals(table1, table2);
    }
}
