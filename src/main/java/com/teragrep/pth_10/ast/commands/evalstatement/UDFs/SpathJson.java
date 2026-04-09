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
package com.teragrep.pth_10.ast.commands.evalstatement.UDFs;

import com.google.gson.*;
import com.teragrep.pth_10.ast.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class SpathJson {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpathJson.class);
    private final String input;
    private final String spathExpression;
    private final String inputColumn;
    private final String outputColumn;
    private final NullValue nullValue;

    /**
     * Returns result of spath as a map Keys wrapped in backticks to escape dots, spark uses them for maps
     *
     * @param input           json input
     * @param spathExpression spath expression
     * @param inputColumn     name of input column
     * @param outputColumn    name of output column
     */

    public SpathJson(String input, String spathExpression, String inputColumn, String outputColumn) {
        this(input, spathExpression, inputColumn, outputColumn, new NullValue());
    }

    private SpathJson(
            String input,
            String spathExpression,
            String inputColumn,
            String outputColumn,
            NullValue nullValue
    ) {
        this.input = input;
        this.spathExpression = spathExpression;
        this.inputColumn = inputColumn;
        this.outputColumn = outputColumn;
        this.nullValue = nullValue;
    }

    public Map<String, String> asMap() throws JsonSyntaxException, ClassCastException {
        // Map to return at the end of this function
        Map<String, String> result = new HashMap<>();

        final Gson gson = new Gson();
        final JsonElement jsonElem = gson.fromJson(input, JsonElement.class);

        if (jsonElem == null || jsonElem.isJsonNull()) {
            LOGGER.debug("Json input was 'null', returning empty result");
            return result;
        }
        else if (jsonElem.isJsonObject() || jsonElem.isJsonArray() || jsonElem.isJsonPrimitive()) {
            result.putAll(new SpathFlatJson(jsonElem).asMap());
        }

        return result;
    }
}
