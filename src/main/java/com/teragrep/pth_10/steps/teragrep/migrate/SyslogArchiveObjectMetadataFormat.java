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
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;

final class SyslogArchiveObjectMetadataFormat implements ArchiveObjectMetadataFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogArchiveObjectMetadataFormat.class);

    @Override
    public ResolvedFormat parsed(final String json) {
        ResolvedFormat result;
        try {
            final JsonObject root = toJsonObject(json);
            final JsonObject object = root.getJsonObject("object");
            final JsonObject timestamp = root.getJsonObject("timestamp");
            final String format = root.getString("format");
            if (!"rfc5424".equalsIgnoreCase(format)) {
                result = new StubArchiveObjectMetadata();
            }
            else {
                result = new ResolvedFormatImpl(
                        format,
                        object.getString("bucket"),
                        object.getString("path"),
                        object.getString("partition"),
                        Long.toString(timestamp.getJsonNumber("epoch").longValue()),
                        timestamp.getString("rfc5424timestamp"),
                        timestamp.getString("path-extracted"),
                        timestamp.getString("path-extracted-precision"),
                        timestamp.getString("source")
                );
            }
        }
        catch (final RuntimeException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Error parsing JSON <{}> message <{}>", json, e.getMessage());
            }
            result = new StubArchiveObjectMetadata();
        }
        return result;
    }

    private JsonObject toJsonObject(final String jsonString) {
        final JsonStructure structure;
        try (
                final StringReader reader = new StringReader(jsonString); final JsonReader jsonReader = Json.createReader(reader)
        ) {
            structure = jsonReader.read();
        }
        catch (final JsonException | JsonParseException | IllegalStateException e) {
            throw new IllegalArgumentException("Failed to read <" + jsonString + "> to JSON: " + e.getMessage(), e);
        }

        if (!structure.getValueType().equals(JsonValue.ValueType.OBJECT)) {
            throw new IllegalArgumentException("Value <" + jsonString + "> could not be parsed to JSON object");
        }
        return structure.asJsonObject();
    }
}
