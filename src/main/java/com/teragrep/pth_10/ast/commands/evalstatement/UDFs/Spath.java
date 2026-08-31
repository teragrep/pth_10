/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
package com.teragrep.pth_10.ast.commands.evalstatement.UDFs;

import com.google.gson.*;
import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * UDF for command spath(json/xml, spath)<br>
 * <p>
 * First, the given (assumed to be) JSON/XML string is tried to be parsed as JSON, and if that fails, XML parsing is
 * attempted. Otherwise the function will return an empty result, or the original input if the input and output column
 * are set to the same column.
 * </p>
 * A separate 'xpath' command can be used for xpath expressions.
 *
 * @author eemhu
 */
public final class Spath implements UDF4<String, String, String, String, Map<String, String>>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Spath.class);
    private static final long serialVersionUID = 1L;

    public Spath() {
    }

    /**
     * Returns result of spath as a map Keys wrapped in backticks to escape dots, spark uses them for maps
     * 
     * @param input           json/xml input
     * @param spathExpr       spath/xpath expression
     * @param nameOfInputCol  name of input column
     * @param nameOfOutputCol name of output column
     * @return map of results
     */
    @Override
    public Map<String, String> call(String input, String spathExpr, String nameOfInputCol, String nameOfOutputCol)
            throws Exception {
        Map<String, String> result;
        // try json
        try {
            result = new SpathFlatJson(input).asMap();
        }
        catch (JsonSyntaxException | ClassCastException json_fail) {
            LOGGER.warn("Processing failed as JSON, trying XML parsing. Error: <{}>", json_fail.getMessage());
            // try xml
            result = new SpathXml(input, spathExpr, nameOfInputCol, nameOfOutputCol).asMap();
        }
        LOGGER.info("Parsing result <{}>", result);
        final Map<String, String> spathResult;
        // check for spath expression
        if (spathExpr != null) {
            spathResult = result
                    .entrySet()
                    .stream()
                    // filter columns that do not match the spath expression
                    .filter(e -> !spathExpr.equals(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        }
        else {
            //return all culumns in Auto-extraction
            spathResult = result;
        }
        return spathResult;
    }
}
