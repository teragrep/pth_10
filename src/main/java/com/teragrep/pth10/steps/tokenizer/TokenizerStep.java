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
import com.teragrep.functions.dpf_03.TokenizerUDF;
import com.teragrep.functions.dpf_03.RegexTokenizerUDF;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Runs the dpf_03 TokenAggregator on given field - Returns a Row with type String[], if dpl.pth_06.bloom.pattern option
 * is present uses regex filtering for resulting tokens
 */
public final class TokenizerStep extends AbstractTokenizerStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenizerStep.class);
    private final String BLOOM_PATTERN_CONFIG_ITEM = "dpl.pth_06.bloom.pattern";

    public TokenizerStep(
            Config config,
            AbstractTokenizerStep.TokenizerFormat tokenizerFormat,
            String inputCol,
            String outputCol
    ) {
        super();
        this.config = config;
        this.tokenizerFormat = tokenizerFormat;
        this.inputCol = inputCol;
        this.outputCol = outputCol;

    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }
        System.out.println(configContainsPattern());
        // dpf_03 custom regex tokenizer udf
        if (configContainsPattern()) {
            final String pattern = config.getString(BLOOM_PATTERN_CONFIG_ITEM).trim();
            LOGGER.info("Tokenizer selected regex tokenizer using pattern <[{}]>", pattern);
            final UserDefinedFunction regexUDF = functions
                    .udf(new RegexTokenizerUDF(), DataTypes.createArrayType(DataTypes.BinaryType, false));

            if (this.tokenizerFormat == AbstractTokenizerStep.TokenizerFormat.BYTES) {
                return dataset
                        .withColumn("regex_column", functions.lit(pattern))
                        .withColumn(this.getOutputCol(), regexUDF.apply(functions.col(this.getInputCol()), functions.col("regex_column"))).drop("regex_column"); // drop literal from result
            }
            else {
                throw new UnsupportedOperationException(
                        "TokenizerFormat.STRING is not supported with regex tokenizer, remove bloom.pattern option"
                );
            }
        }

        // dpf_03 custom tokenizer udf
        final UserDefinedFunction tokenizerUDF = functions
                .udf(new TokenizerUDF(), DataTypes.createArrayType(DataTypes.BinaryType, false));

        if (this.tokenizerFormat == AbstractTokenizerStep.TokenizerFormat.BYTES) {
            return dataset.withColumn(this.getOutputCol(), tokenizerUDF.apply(functions.col(this.getInputCol())));
        }
        else if (this.tokenizerFormat == AbstractTokenizerStep.TokenizerFormat.STRING) {
            final UserDefinedFunction byteArrayListAsStringListUDF = functions
                    .udf(new ByteArrayListAsStringListUDF(), DataTypes.createArrayType(StringType));
            return dataset
                    .withColumn(
                            this.getOutputCol(),
                            byteArrayListAsStringListUDF.apply(tokenizerUDF.apply(functions.col(this.getInputCol())))
                    );
        }

        throw new IllegalStateException("Unexpected tokenizerFormat: " + this.tokenizerFormat);
    }

    private boolean configContainsPattern() {
        if (config != null && config.hasPath(BLOOM_PATTERN_CONFIG_ITEM)) {
            final String patternFromConfig = config.getString(BLOOM_PATTERN_CONFIG_ITEM);
            return patternFromConfig != null && !patternFromConfig.isEmpty();
        }
        else {
            return false;
        }
    }
}
