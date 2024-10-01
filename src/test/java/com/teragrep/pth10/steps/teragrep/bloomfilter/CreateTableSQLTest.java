package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

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
        CreateTableSQL table = new CreateTableSQL(name);
        String e = "CREATE TABLE IF NOT EXISTS `test_table`(`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE,`filter_type_id` BIGINT UNSIGNED NOT NULL,`filter` LONGBLOB NOT NULL);";
        Assertions.assertEquals(e, table.ignoreConstraintsSql());
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
        CreateTableSQL table = new CreateTableSQL(injection);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::ignoreConstraintsSql);
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
        CreateTableSQL table = new CreateTableSQL(tooLongName);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, table::ignoreConstraintsSql);
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
}