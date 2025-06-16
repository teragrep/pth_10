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
package com.teragrep.pth10.steps.rangemap;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class RangemapStep extends AbstractRangemapStep {

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        // check values
        if (this.sourceField == null) {
            throw new IllegalArgumentException("Field parameter is required!");
        }

        // register rangemap udf
        UserDefinedFunction udf = functions.udf(new RangemapUDF(), DataTypes.createArrayType(DataTypes.StringType));
        SparkSession.builder().getOrCreate().udf().register("RangemapUDF", udf);

        // add map entries into key and value arrays
        final List<Column> keys = new ArrayList<>();
        final List<Column> values = new ArrayList<>();
        for (final Map.Entry<String, String[]> ent : attributeRangeMap.entrySet()) {
            keys.add(functions.lit(ent.getKey()));
            values.add(functions.lit(ent.getValue()));
        }

        // create spark map from those arrays
        Column keyCol = functions.array(JavaConversions.asScalaBuffer(keys));
        Column valueCol = functions.array(JavaConversions.asScalaBuffer(values));
        Column mapCol = functions.map_from_arrays(keyCol, valueCol);

        // apply udf to column "range"
        return dataset
                .withColumn("range", functions.callUDF("RangemapUDF", functions.col(sourceField), functions.lit(defaultValue), mapCol));
    }
}
