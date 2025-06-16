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
package com.teragrep.pth10.steps.spath;

import com.teragrep.pth10.ast.MapTypeColumn;
import com.teragrep.pth10.ast.QuotedText;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.commands.evalstatement.UDFs.Spath;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;

import java.util.*;

public final class SpathStep extends AbstractSpathStep {

    public SpathStep() {
        super();
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        if (dataset == null) {
            return null;
        }

        SparkSession ss = SparkSession.builder().getOrCreate();

        // udf returns as a map for auto-extraction support
        final MapType returnType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

        // register udf with SparkSession and get column
        ss.udf().register("UDF_Spath", new Spath(catCtx.nullValue), returnType);
        Column spathExpr = functions
                .callUDF(
                        "UDF_Spath", // name of UDF
                        functions.col(inputColumn), // Input column (actual data to run spath on)
                        functions.lit(path), // Path to extract data from, usually mainkey.someotherkey
                        functions.lit(inputColumn), // Name of input column (no data)
                        functions.lit(outputColumn) // Name of output column (no data)
                );

        // Not in auto-extraction mode: can just return the first and only value from the map
        if (!autoExtractionMode) {
            return dataset
                    .withColumn(new UnquotedText(new TextString(outputColumn)).read(), spathExpr.getItem(new QuotedText(new TextString(path), "`").read()));
        }

        //
        // auto-extraction mode
        //

        // set of keys in map
        Set<String> keys;

        // apply udf
        Dataset<Row> withAppliedUdfDs = dataset.withColumn(outputColumn, spathExpr);

        MapTypeColumn mapColumn = new MapTypeColumn(withAppliedUdfDs, outputColumn, this.getCatCtx());
        keys = mapColumn.getKeys();

        // Go through the list and get values for each of the keys
        // Each key is a new column with the cell contents being the value for that key

        // Check for nulls; return an empty string if null, otherwise value for given key
        // use substring to remove backticks that were added to escape dots in key name
        for (String key : keys) {
            withAppliedUdfDs = withAppliedUdfDs
                    .withColumn(new UnquotedText(new TextString(key)).read(), functions.when(
                            /* if key.value == null */
                            functions.isnull(withAppliedUdfDs.col(outputColumn).getItem(key)),
                            /* then return empty string */
                            functions.lit("")
                    )
                            /* otherwise return key.value */
                            .otherwise(withAppliedUdfDs.col(outputColumn).getItem(key))
                    );
        }

        // Output column can be dropped
        withAppliedUdfDs = withAppliedUdfDs.drop(outputColumn);

        return withAppliedUdfDs;
    }
}
