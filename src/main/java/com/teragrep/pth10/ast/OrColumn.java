package com.teragrep.pth10.ast;

import org.apache.spark.sql.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class OrColumn {
    private final List<Column> columns;

    public OrColumn(Column... columns) {
        this(Arrays.asList(columns));
    }

    public OrColumn(List<Column> columns) {
        this.columns = columns;
    }

    public Column column() {
        if (columns.isEmpty()) {
            throw new IllegalStateException("No columns found");
        }

        Column rv = columns.get(0);
        for (Column current : columns.subList(1, columns.size())) {
            rv = rv.or(current);
        }
        return rv;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrColumn orColumn = (OrColumn) o;
        return Objects.equals(columns, orColumn.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columns);
    }
}
