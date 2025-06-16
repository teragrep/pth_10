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
package com.teragrep.pth10.steps.replace;

import com.teragrep.pth10.ast.commands.transformstatement.replace.ReplaceCmd;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;
import java.util.Map;

public final class ReplaceStep extends AbstractReplaceStep {

    public ReplaceStep(List<String> listOfFields, Map<String, String> replacements) {
        super(listOfFields, replacements);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        // If list of fields are not given (empty IN clause), replace in all
        // except the "_time" column. If the "_time" column needs to be replaced, the
        // user must use the IN clause to include it manually.
        if (this.listOfFields.isEmpty()) {
            for (String fieldName : dataset.schema().fieldNames()) {
                if (!fieldName.equalsIgnoreCase("_time")) {
                    this.listOfFields.add(fieldName);
                }
            }
        }

        // Register the UDF used for the replace command
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("UDF_Replace", new ReplaceCmd(), DataTypes.StringType);

        // Apply the replace function to all given fields
        for (String field : this.listOfFields) {
            for (String contentToReplace : this.replacements.keySet()) {
                Column res = functions
                        .callUDF("UDF_Replace", functions.col(field), functions.lit(contentToReplace), functions.lit(this.replacements.get(contentToReplace)));

                dataset = dataset.withColumn(field, res);
            }
        }

        return dataset;
    }
}
