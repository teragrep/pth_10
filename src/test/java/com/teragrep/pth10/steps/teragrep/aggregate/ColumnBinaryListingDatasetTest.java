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
package com.teragrep.pth10.steps.teragrep.aggregate;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ColumnBinaryListingDatasetTest {

    SparkSession spark;

    @BeforeAll
    public void setup() {
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.network.timeout", "5s")
                .config("spark.network.timeoutInterval", "5s") // should be less than or equal to spark.network.timeout
                .config("spark.executor.heartbeatInterval", "2s") // should be "significantly less" than spark.network.timeout
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") // added for kafka tests
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testBinaryColumnType() {
        List<byte[]> bytes = Arrays
                .asList("one".getBytes(StandardCharsets.UTF_8), "two".getBytes(StandardCharsets.UTF_8), "three".getBytes(StandardCharsets.UTF_8));
        Row row = RowFactory.create(bytes);
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("tokens", DataTypes.createArrayType(DataTypes.BinaryType), false)
        });
        Dataset<Row> dataset = spark.createDataFrame(Collections.singletonList(row), schema);
        ColumnBinaryListingDataset columnDataset = new ColumnBinaryListingDataset(dataset, "tokens");
        Dataset<Row> binaryListingDataset = columnDataset.dataset();
        List<Object> list1 = dataset.first().getList(0);
        List<Object> list2 = binaryListingDataset.first().getList(0);
        Assertions.assertEquals(list1.size(), list2.size());
        Object bytes1 = list1.get(0);
        Object bytes2 = list2.get(0);
        Assertions.assertEquals(bytes1.getClass(), byte[].class);
        Assertions.assertEquals(bytes1.getClass(), bytes2.getClass());
        Assertions.assertTrue(Arrays.equals((byte[]) bytes1, (byte[]) bytes2));
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testStringColumnType() {
        List<String> stringList = Arrays.asList("one", "two", "three");
        Row row = RowFactory.create(stringList);
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("tokens", DataTypes.createArrayType(DataTypes.StringType), false)
        });
        Dataset<Row> dataset = spark.createDataFrame(Collections.singletonList(row), schema);
        ColumnBinaryListingDataset columnDataset = new ColumnBinaryListingDataset(dataset, "tokens");
        Dataset<Row> binaryListingDataset = columnDataset.dataset();
        List<Object> list1 = dataset.first().getList(0);
        List<Object> list2 = binaryListingDataset.first().getList(0);
        Assertions.assertNotEquals(list1, list2);
        Assertions.assertEquals(list1.size(), list2.size());
        Object firstString = list1.get(0);
        Object firstBytes = list2.get(0);
        Assertions.assertNotEquals(firstString, firstBytes);
        Assertions.assertEquals(firstString.getClass(), String.class);
        Assertions.assertEquals(firstBytes.getClass(), byte[].class);
        Assertions.assertEquals("one", new String((byte[]) firstBytes, StandardCharsets.UTF_8));
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testUnsupportedTypeException() {
        List<Integer> intList = Arrays.asList(1, 2, 3);
        Row row = RowFactory.create(intList);
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("tokens", DataTypes.createArrayType(DataTypes.IntegerType), false)
        });
        Dataset<Row> dataset = spark.createDataFrame(Collections.singletonList(row), schema);
        ColumnBinaryListingDataset columnDataset = new ColumnBinaryListingDataset(dataset, "tokens");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, columnDataset::dataset);
        String expectedMessage = "Input column <tokens> has unsupported column type <ArrayType(IntegerType,true)>, supported types are ArrayType(BinaryType), ArrayType(StringType)";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }
}
