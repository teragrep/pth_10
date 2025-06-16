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
package com.teragrep.pth10.steps.rex;

import com.teragrep.jpr_01.JavaPcre;
import com.teragrep.pth10.ast.commands.transformstatement.rex.RexExtractModeUDF;
import com.teragrep.pth10.ast.commands.transformstatement.rex.RexSedModeUDF;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;

public final class RexStep extends AbstractRexStep {

    public RexStep() {
        super();
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        final SparkSession ss = SparkSession.builder().getOrCreate();
        Dataset<Row> res = dataset;

        if (this.isSedMode()) {
            // sed mode
            UserDefinedFunction udf = functions.udf(new RexSedModeUDF(), DataTypes.StringType);
            ss.udf().register("RexSedUDF", udf);
            Column udfResult = functions.callUDF("RexSedUDF", functions.col(this.field), functions.lit(this.regexStr));
            res = res.withColumn(this.field, udfResult);
        }
        else {
            // extract mode
            UserDefinedFunction udf = functions
                    .udf(new RexExtractModeUDF(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
            ss.udf().register("RexExtractUDF", udf);
            Column udfResult = functions
                    .callUDF("RexExtractUDF", functions.col(this.field), functions.lit(this.regexStr));

            final String outputCol = "$$dpl_internal_rex_result$$";
            res = res.withColumn(outputCol, udfResult);

            // compile regex pattern to get name table
            final JavaPcre pcre = new JavaPcre();
            pcre.compile_java(regexStr);
            Map<String, Integer> nameTable = pcre.get_name_table();

            // go through the capture group names and if value is null, apply NullValue, otherwise value
            for (String name : nameTable.keySet()) {
                res = res
                        .withColumn(name, functions.when(functions.isnull(res.col(outputCol).getItem(name)), functions.lit(catCtx.nullValue.value()).cast(DataTypes.StringType)).otherwise(res.col(outputCol).getItem(name)));
            }

            // drop intermediate result column
            res = res.drop(outputCol);

            // clean compile
            // Note: Can throw IllegalStateException if nothing to free but should
            // realistically never happen as it is only called after compile
            pcre.jcompile_free();
        }

        return res;
    }
}
