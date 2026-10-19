package com.teragrep.pth_10;


import com.teragrep.pth_10.ast.FlattenedStringArrayColumn;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@EnabledIfSystemProperty(
        named = "runPerformanceTests",
        matches = "true"
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FlattenedStringArrayColumnPerformanceTest {

    private SparkSession spark;

    @BeforeAll
    void setenv() {
        this.spark = SparkSession
                .builder()
                .appName("FlattenedStringArrayColumn performance")
                .master("local[2]")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    void tearDown() {
        this.spark.stop();
    }

    @ParameterizedTest
    @CsvSource({"1", "100", "1000"})
    public void arrayLengthOfAppendedColumns(final int arrayLength) {
        final Dataset<Row> dataset = cachedDataset(5000, 16);
        final Column mvField = functions.array_repeat(functions.col("value"), arrayLength);

        final Column flattenedColumn = new FlattenedStringArrayColumn(mvField, mvField).column();
        final Column arrayFunctionColumn = functions.flatten(functions.array(mvField, mvField));

        report("array length " + arrayLength, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({"8", "128", "1024"})
    public void valueLengthOfAppendedColumns(final int valueLength)  {
        final Dataset<Row> dataset = cachedDataset(5000, valueLength);
        final Column value = functions.col("value");

        final Column flattenedColumn = new FlattenedStringArrayColumn(value, value).column();
        final Column arrayFunctionColumn = functions.array(value, value);
        report("value length " + valueLength, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({"1000", "10000", "50000"})
    public void rowCountOfTheDataset(final long rowCount) {
        final Dataset<Row> dataset = cachedDataset(rowCount, 16);
        final Column value = functions.col("value");

        final Column flattenedColumn = new FlattenedStringArrayColumn(value, value).column();
        final Column arrayFunctionColumn = functions.array(value, value);

        report("row count " + rowCount, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({"2", "5", "10"})
    public void amountOfAppendedColumns(final int columnCount) {
        final Dataset<Row> dataset = cachedDataset(5000, 16);
        final List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            columns.add(functions.col("value"));
        }

        final Column flattenedColumn = new FlattenedStringArrayColumn(columns).column();
        final Column arrayFunctionColumn = functions.array(columns.toArray(new Column[0]));

        report("appended columns " + columnCount, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({"1", "3", "5"})
    public void nestedAppends(final int depth) {
        final Dataset<Row> dataset = cachedDataset(5000, 16);
        final Column value = functions.col("value");

        Column flattenedColumn = value;
        Column arrayFunctionColumn = functions.array(value);
        for (int i = 0; i < depth; i++) {
            flattenedColumn = new FlattenedStringArrayColumn(flattenedColumn, value).column();
            arrayFunctionColumn = functions.flatten(functions.array(arrayFunctionColumn, functions.array(value)));
        }

        report("nested appends " + depth, dataset, flattenedColumn, arrayFunctionColumn);
    }

    private Dataset<Row> cachedDataset(final long rowCount, final int valueLength){
        final Column padding = functions.repeat(functions.lit("x"), valueLength);
        final Column value = functions.concat(functions.lit("row-"), functions.col("id").cast("string"), padding);

        final Dataset<Row> dataset = this.spark.range(rowCount).withColumn("value", value).select("value").cache();
        dataset.foreach((ForeachFunction<Row>) row -> {
        });
        return dataset;
    }

    private void report(
            final String name,
            final Dataset<Row> dataset,
            final Column flattenedColumn,
            final Column arrayFunctionColumn
    ) {
        final Row sample = dataset.select(flattenedColumn.alias("flattened"), arrayFunctionColumn.alias("arrayFunctions")).first();
        final List<Object> flattened = sample.getList(0);
        final List<Object> arrayFunctions = sample.getList(1);

        System.out.println(name + " flattened size " + flattened.size() + " first " + flattened.get(0));
        System.out.println(name + " arrayFunctions size " + arrayFunctions.size() + " first " + arrayFunctions.get(0));
        Assertions.assertEquals(arrayFunctions, flattened, "Both columns should give the same values");
        final long flattenedMillis = medianMillis(dataset, flattenedColumn);
        final long arrayFunctionMillis = medianMillis(dataset, arrayFunctionColumn);
        dataset.unpersist();
        System.out.printf("%-4s flattened %2d ms arrayFunctions %2d ms%n", name, flattenedMillis, arrayFunctionMillis);
    }

    private long medianMillis(final Dataset<Row> dataset, final Column column) {
        int WARMUP_RUNS = 2;
        for (int i = 0; i < WARMUP_RUNS; i++) {
            consume(dataset, column);
        }

        int MEASURED_RUNS = 5;
        final long[] runs = new long[MEASURED_RUNS];
        for (int i = 0; i < MEASURED_RUNS; i++) {
            runs[i] = consume(dataset, column);
        }

        Arrays.sort(runs);
        return runs[runs.length / 2];
    }

    private long consume(final Dataset<Row> dataset, final Column column) {
        final Dataset<Row> selected = dataset.select(column.alias("result"));

        final long start = System.nanoTime();
        selected.foreach((ForeachFunction<Row>) row -> {
        });

        return (System.nanoTime() - start) / 1000000L;
    }
}
