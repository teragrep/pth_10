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
package com.teragrep.pth_10.steps.teragrep;

import com.teragrep.functions.dpf_02.AbstractStep;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TeragrepRegexExtractionStep extends AbstractStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepRegexExtractionStep.class);

    private final String regex;
    private final String inputCol;
    private final String outputCol;

    public TeragrepRegexExtractionStep(String regex, String inputCol, String outputCol) {
        this.regex = regex;
        this.inputCol = inputCol;
        this.outputCol = outputCol;
    }

    @Override
    public Dataset<Row> get(final Dataset<Row> dataset) throws StreamingQueryException {
        LOGGER
                .info(
                        "TeragrepRegexExtractionStep using regex pattern: <[{}]> from input col <[{}]> to output col <[{}]>",
                        regex, inputCol, outputCol
                );

        final String nullSafeColumn = "nullSafe_" + inputCol;

        // create a column where null values are replaced with empty string
        final Dataset<Row> withNullSafeCol = dataset
                .withColumn(nullSafeColumn, functions.when(functions.col(inputCol).isNull(), functions.lit("")).otherwise(functions.col(inputCol)));

        final RegexTokenizer tokenizer = new RegexTokenizer()
                .setInputCol(nullSafeColumn)
                .setOutputCol(outputCol)
                .setPattern(regex)
                .setGaps(false);

        // create tokens column
        final Dataset<Row> withTokens = tokenizer.transform(withNullSafeCol);

        // remove null safe column from result
        return withTokens.drop(nullSafeColumn);
    }
}
