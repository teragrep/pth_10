package com.teragrep.pth10;

import com.teragrep.pth10.ast.OrColumn;
import org.apache.spark.sql.Column;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OrColumnTest {
    @Test
    void testOrColumn() {
        OrColumn orColumn = new OrColumn(
                new Column("a"),
                new Column("b")
        );

        Assertions.assertEquals(new Column("a").or(new Column("b")), orColumn.column());
    }

    @Test
    void testOrColumnWithOneColumn() {
        OrColumn orColumn = new OrColumn(
                new Column("a")
        );

        Assertions.assertEquals(new Column("a"), orColumn.column());
    }

    @Test
    void testOrColumnWithNoColumn() {
        OrColumn orColumn = new OrColumn();

        IllegalStateException ise = Assertions.assertThrows(IllegalStateException.class, orColumn::column);
        Assertions.assertEquals("No columns found", ise.getMessage());
    }

    @Test
    void testEquals() {
        OrColumn orColumn = new OrColumn();
        OrColumn orColumn2 = new OrColumn();
        Assertions.assertEquals(orColumn, orColumn2);
    }

    @Test
    void testNotEquals() {
        OrColumn orColumn = new OrColumn();
        OrColumn orColumn2 = new OrColumn(new Column("a"));
        Assertions.assertNotEquals(orColumn, orColumn2);
    }
}
