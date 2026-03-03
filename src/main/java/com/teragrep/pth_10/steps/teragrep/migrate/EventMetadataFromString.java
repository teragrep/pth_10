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

import jakarta.json.JsonObject;

import java.util.Objects;

public final class EventMetadataFromString implements EventMetadata {

    private final JsonObject jsonObject;

    public EventMetadataFromString(final String jsonString) {
        this(new ParsedJson(jsonString));
    }

    public EventMetadataFromString(final ParsedJson parsedJson) {
        this(parsedJson.toJsonObject());
    }

    public EventMetadataFromString(final JsonObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    @Override
    public boolean isSyslog() {
        return "rfc5424".equalsIgnoreCase(jsonObject.getString("format"));
    }

    @Override
    public String format() {
        return jsonObject.getString("format");
    }

    @Override
    public String bucket() {
        return jsonObject.getJsonObject("object").getString("bucket");
    }

    @Override
    public String path() {
        return jsonObject.getJsonObject("object").getString("path");
    }

    @Override
    public String partition() {
        return jsonObject.getJsonObject("object").getString("partition");
    }

    @Override
    public String rfc5242Timestamp() {
        if (!isSyslog()) {
            throw new UnsupportedOperationException("rfc5242Timestamp() not available for non-syslog metadata");
        }
        return jsonObject.getJsonObject("timestamp").getString("rfc5242timestamp");
    }

    @Override
    public String pathExtracted() {
        return jsonObject.getJsonObject("timestamp").getString("path-extracted");
    }

    @Override
    public String pathExtractedPrecision() {
        return jsonObject.getJsonObject("timestamp").getString("path-extracted-precision");
    }

    @Override
    public String source() {
        return jsonObject.getJsonObject("timestamp").getString("source");
    }

    @Override
    public String epoch() {
        if (!isSyslog()) {
            throw new UnsupportedOperationException("epoch() not available for non-syslog metadata");
        }
        return String.valueOf(jsonObject.getJsonObject("timestamp").getJsonNumber("epoch").longValue());
    }

    @Override
    public String toString() {
        return jsonObject.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        final EventMetadataFromString that = (EventMetadataFromString) o;
        return Objects.equals(jsonObject, that.jsonObject);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(jsonObject);
    }
}
