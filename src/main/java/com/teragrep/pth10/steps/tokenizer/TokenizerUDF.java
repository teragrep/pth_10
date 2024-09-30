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
package com.teragrep.pth10.steps.tokenizer;

import com.teragrep.functions.dpf_03.ByteArrayListAsStringListUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class TokenizerUDF implements TokenizerApplicable {

    private final AbstractTokenizerStep.TokenizerFormat format;
    private final String inputCol;
    private final String outputCol;

    public TokenizerUDF(AbstractTokenizerStep.TokenizerFormat format, String inputCol, String outputCol) {
        this.format = format;
        this.inputCol = inputCol;
        this.outputCol = outputCol;
    }

    public Dataset<Row> appliedDataset(final Dataset<Row> dataset) {
        final UserDefinedFunction tokenizerUDF = functions
                .udf(
                        new com.teragrep.functions.dpf_03.TokenizerUDF(),
                        DataTypes.createArrayType(DataTypes.BinaryType, false)
                );

        if (format == AbstractTokenizerStep.TokenizerFormat.BYTES) {
            return dataset.withColumn(outputCol, tokenizerUDF.apply(functions.col(inputCol)));
        }
        else if (format == AbstractTokenizerStep.TokenizerFormat.STRING) {
            final UserDefinedFunction byteArrayListAsStringListUDF = functions
                    .udf(new ByteArrayListAsStringListUDF(), DataTypes.createArrayType(StringType));
            return dataset
                    .withColumn(outputCol, byteArrayListAsStringListUDF.apply(tokenizerUDF.apply(functions.col(inputCol))));
        }
        throw new IllegalStateException("Unexpected tokenizerFormat: " + format);
    }
}
