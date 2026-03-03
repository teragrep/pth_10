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
package com.teragrep.pth_10.steps.teragrep.migrate;

import com.google.gson.JsonParseException;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;

import java.io.StringReader;
import java.util.Objects;

public final class ParsedJson {

    private final String jsonString;

    public ParsedJson(final String jsonString) {
        this.jsonString = jsonString;
    }

    public JsonObject toJsonObject() {
        final JsonStructure jsonStructure = toJsonStructure();
        if (!jsonStructure.getValueType().equals(JsonValue.ValueType.OBJECT)) {
            throw new IllegalArgumentException("Value <" + jsonString + "> could not be parsed to JSON object");
        }
        return toJsonStructure().asJsonObject();
    }

    public JsonArray toJsonArray() {
        final JsonStructure jsonStructure = toJsonStructure();
        if (!jsonStructure.getValueType().equals(JsonValue.ValueType.ARRAY)) {
            throw new IllegalArgumentException("Value <" + jsonString + "> could not be parsed to JSON array");
        }
        return jsonStructure.asJsonArray();
    }

    private JsonStructure toJsonStructure() {
        try (
                final StringReader reader = new StringReader(jsonString); final JsonReader jsonReader = Json.createReader(reader)
        ) {
            return jsonReader.read();
        }
        catch (final JsonException | JsonParseException | IllegalStateException e) {
            throw new IllegalArgumentException("Failed to read <" + jsonString + "> to JSON: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(final Object object) {
        final boolean rv;
        if (object == null) {
            rv = false;
        }
        else if (getClass() != object.getClass()) {
            rv = false;
        }
        else {
            final ParsedJson parsedJson = (ParsedJson) object;
            rv = Objects.equals(jsonString, parsedJson.jsonString);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(jsonString);
    }
}
