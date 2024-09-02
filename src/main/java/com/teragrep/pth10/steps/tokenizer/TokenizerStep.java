/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
import com.teragrep.functions.dpf_03.TokenizerUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Runs the dpf_03 TokenAggregator on given field
 * - Returns a Row with type String[]
 */
public final class TokenizerStep extends AbstractTokenizerStep {
    public TokenizerStep(AbstractTokenizerStep.TokenizerFormat tokenizerFormat, String inputCol, String outputCol) {
        super();
        this.tokenizerFormat = tokenizerFormat;
        this.inputCol = inputCol;
        this.outputCol = outputCol;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        // dpf_03 custom tokenizer udf
        UserDefinedFunction tokenizerUDF =
                functions.udf(new TokenizerUDF(), DataTypes.createArrayType(DataTypes.BinaryType, false));



        if (this.tokenizerFormat == AbstractTokenizerStep.TokenizerFormat.BYTES) {
            return dataset.withColumn(this.getOutputCol(), tokenizerUDF.apply(functions.col(this.getInputCol())));
        } else if (this.tokenizerFormat == AbstractTokenizerStep.TokenizerFormat.STRING) {
            UserDefinedFunction byteArrayListAsStringListUDF = functions.udf(new ByteArrayListAsStringListUDF(), DataTypes.createArrayType(StringType));
            return dataset.withColumn(this.getOutputCol(), byteArrayListAsStringListUDF.apply(tokenizerUDF.apply(functions.col(this.getInputCol()))));
        }

        throw new IllegalStateException("Unexpected tokenizerFormat: " + this.tokenizerFormat);
    }
}
