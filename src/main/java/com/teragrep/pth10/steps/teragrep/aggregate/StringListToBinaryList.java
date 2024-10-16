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
package com.teragrep.pth10.steps.teragrep.aggregate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import java.nio.charset.StandardCharsets;

/** Transforms datasets inputCol array members from String to byte[] or BinaryType */
public final class StringListToBinaryList {

    private final Dataset<Row> dataset;
    private final String inputCol;

    public StringListToBinaryList(Dataset<Row> dataset, String inputCol) {
        this.dataset = dataset;
        this.inputCol = inputCol;
    }

    public Dataset<Row> dataset() {
        // use scala WrappedArray to avoid casting errors with Java in spark
        final UDF1<WrappedArray<String>, WrappedArray<byte[]>> udf = stringList -> {
            final byte[][] bytesArray = new byte[stringList.length()][];
            for (int i = 0; i < stringList.length(); i++) {
                bytesArray[i] = stringList.apply(i).getBytes(StandardCharsets.UTF_8);
            }
            return WrappedArray.make(bytesArray);
        };
        return dataset
                .withColumn(inputCol, functions.udf(udf, DataTypes.createArrayType(DataTypes.BinaryType)).apply(dataset.col(inputCol)));
    }
}
