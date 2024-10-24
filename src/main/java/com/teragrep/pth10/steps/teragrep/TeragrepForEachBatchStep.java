package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.steps.AbstractStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class TeragrepForEachBatchStep extends AbstractStep {
    public TeragrepForEachBatchStep() {
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY); // Switch to sequential mode
        this.properties.add(CommandProperty.USES_INTERNAL_BATCHCOLLECT); // Skip using batch collect
    }
    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        return dataset;
    }
}
