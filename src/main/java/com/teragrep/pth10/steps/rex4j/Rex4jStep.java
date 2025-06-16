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
package com.teragrep.pth10.steps.rex4j;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.commands.transformstatement.rex4j.NamedGroupsRex;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class Rex4jStep extends AbstractRex4jStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(Rex4jStep.class);
    private final DPLParserCatalystContext catCtx;

    public Rex4jStep(DPLParserCatalystContext catCtx) {
        super();
        this.catCtx = catCtx;
    }

    // TODO Implement maxMatch parameter; however it has never been implemented before
    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        if (sedMode != null) { // In mode=sed
            // Escape regex string; spark's internal regexp will otherwise cause '\n' to become 'n'
            regexStr = StringEscapeUtils.escapeJava(regexStr);

            // regexStr is s|y / regexp / replacement / flags (g|Nth occurrence)
            String[] sed = regexStr.split("/");

            // FIXME Implement character substitute mode and Nth occurrence flag
            // y/ and /N, where N>0
            if (sed.length < 4) {
                throw new RuntimeException(
                        "Invalid sedMode string given in rex4j: " + regexStr + "\nExpected: s/regexp/replacement/g"
                );
            }

            if (!sed[0].equals("s")) {
                throw new UnsupportedOperationException(
                        "Only replace strings mode (s/) is supported as of now. Expected: s, Actual: " + sed[0]
                );
            }

            if (!sed[3].equals("g")) {
                throw new UnsupportedOperationException(
                        "Only global flag (/g) is supported as of now. Expected: g, Actual: " + sed[3]
                );
            }

            Column rex = functions.regexp_replace(new Column(field), sed[1], sed[2]);
            return dataset.withColumn(field, rex);
        }
        else { // default mode
            Map<String, Integer> fields = NamedGroupsRex.getNamedGroups(regexStr);

            // a namedGroup must exist
            if (fields.isEmpty()) {
                throw new IllegalArgumentException(
                        "Error in rex4j command, regexp-string missing mandatory match groups."
                );
            }

            // go through multi extraction groups
            Dataset<Row> res = dataset;
            Column rex;
            for (Map.Entry<String, Integer> me : fields.entrySet()) {
                Integer in = me.getValue();
                // perform regexp_extract
                rex = functions.regexp_extract(functions.col(field), regexStr, in);
                res = res
                        .withColumn(me.getKey(), functions.when(rex.eqNullSafe(functions.lit("")), functions.lit(catCtx.nullValue.value())).otherwise(rex));
            }
            return res;
        }
    }
}
