package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.steps.AbstractStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class TeragrepGetConfigStep extends AbstractStep {
    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        return null;
    }
}
