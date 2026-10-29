/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
    @CsvSource({
            "1", "100", "1000"
    })
    public void testArrayLengthOfAppendedColumns(final int arrayLength) {
        final Dataset<Row> dataset = cachedDataset(100000, 16);
        final Column mvField = functions.array_repeat(functions.col("value"), arrayLength);

        final Column flattenedColumn = new FlattenedStringArrayColumn(mvField, mvField).column();
        final Column arrayFunctionColumn = functions.flatten(functions.array(mvField, mvField));

        report("array length " + arrayLength, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({
            "8", "128", "1024"
    })
    public void testValueLengthOfAppendedColumns(final int valueLength) {
        final Dataset<Row> dataset = cachedDataset(500000, valueLength);
        final Column value = functions.col("value");

        final Column flattenedColumn = new FlattenedStringArrayColumn(value, value).column();
        final Column arrayFunctionColumn = functions.array(value, value);
        report("value length " + valueLength, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({
            "1000", "10000", "50000", "500000"
    })
    public void testRowCountOfTheDataset(final long rowCount) {
        final Dataset<Row> dataset = cachedDataset(rowCount, 16);
        final Column value = functions.col("value");

        final Column flattenedColumn = new FlattenedStringArrayColumn(value, value).column();
        final Column arrayFunctionColumn = functions.array(value, value);

        report("row count " + rowCount, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({
            "2", "5", "10"
    })
    public void testAmountOfAppendedColumns(final int columnCount) {
        final Dataset<Row> dataset = cachedDataset(500000, 16);
        final List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            columns.add(functions.col("value"));
        }

        final Column flattenedColumn = new FlattenedStringArrayColumn(columns).column();
        final Column arrayFunctionColumn = functions.array(columns.toArray(new Column[0]));

        report("appended columns " + columnCount, dataset, flattenedColumn, arrayFunctionColumn);
    }

    @ParameterizedTest
    @CsvSource({
            "1", "3", "5"
    })
    public void testNestedAppends(final int depth) {
        final Dataset<Row> dataset = cachedDataset(500000, 16);
        final Column value = functions.col("value");

        Column flattenedColumn = value;
        Column arrayFunctionColumn = functions.array(value);
        for (int i = 0; i < depth; i++) {
            flattenedColumn = new FlattenedStringArrayColumn(flattenedColumn, value).column();
            arrayFunctionColumn = functions.flatten(functions.array(arrayFunctionColumn, functions.array(value)));
        }

        report("nested appends " + depth, dataset, flattenedColumn, arrayFunctionColumn);
    }

    private Dataset<Row> cachedDataset(final long rowCount, final int valueLength) {
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
        final Row sample = dataset
                .select(flattenedColumn.alias("flattened"), arrayFunctionColumn.alias("arrayFunctions"))
                .first();
        final List<Object> flattened = sample.getList(0);
        final List<Object> arrayFunctions = sample.getList(1);

        Assertions.assertEquals(arrayFunctions, flattened, "Both columns should give the same values");
        final long flattenedMillis = medianMillis(dataset, flattenedColumn);
        final long arrayFunctionMillis = medianMillis(dataset, arrayFunctionColumn);
        dataset.unpersist();
        System.out
                .printf("%-24s flattened %6d ms | arrayFunctions %6d ms%n", name, flattenedMillis, arrayFunctionMillis);
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
