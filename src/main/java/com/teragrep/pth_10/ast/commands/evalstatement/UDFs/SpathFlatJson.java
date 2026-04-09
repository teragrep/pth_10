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

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.teragrep.pth_10.ast.QuotedText;
import com.teragrep.pth_10.ast.TextString;
import com.teragrep.pth_10.ast.UnquotedText;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class SpathFlatJson {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpathFlatJson.class);
    private final JsonElement root;


    public SpathFlatJson(final JsonElement root) {
        this.root = root;
    }

    Map<String, String> asMap() {
        final Map<String, String> result;
        if (root == null || root.isJsonNull()) {
            result = Collections.emptyMap();
        }
        else {
            result = new HashMap<>();
            traverseJson(root, "", result);
        }
        return result;
    }

    private void traverseJson(final JsonElement element, final String path, final Map<String, String> result) {
        if (element == null || element.isJsonNull()) {
            return;
        }

        if (element.isJsonObject()) {
            for (final Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
                final String key = entry.getKey();
                final String newPath = path.isEmpty() ? key : path + "." + key;
                traverseJson(entry.getValue(), newPath, result);
            }
        }
        else if (element.isJsonArray()) {
            LOGGER.info("Given JsonElement was JsonArray");
            for (final JsonElement jsonElement : element.getAsJsonArray()) {
                traverseJson(jsonElement, path, result);
            }
        }
        else if (element.isJsonPrimitive()) {
            putPrimitive(result, path, element.getAsJsonPrimitive());
        }
        else {
            throw new RuntimeException("Unrecognized JSON element: " + element);
        }
    }

    private void putPrimitive(Map<String, String> result, final String path, final JsonPrimitive primitive) {
        if (path.isEmpty()) {
            LOGGER.debug("Key was empty");
            return;
        }
        final String value = new UnquotedText(new TextString(StringEscapeUtils.unescapeJson(primitive.toString())))
                .read();
        final String key = new QuotedText(new TextString(path), "`").read();
        result.put(key, value);
    }
}
