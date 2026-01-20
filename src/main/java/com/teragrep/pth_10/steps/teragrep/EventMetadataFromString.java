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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Objects;

public final class EventMetadataFromString implements EventMetadata {

    private final String value;

    public EventMetadataFromString(final String value) {
        this.value = value;
    }

    @Override
    public boolean isSyslog() {
        return "rfc5424".equalsIgnoreCase(jsonObject().get("format").getAsString());
    }

    @Override
    public String format() {
        return jsonObject().get("format").getAsString();
    }

    @Override
    public String bucket() {
        return jsonObject().getAsJsonObject("object").get("bucket").getAsString();
    }

    @Override
    public String path() {
        return jsonObject().getAsJsonObject("object").get("path").getAsString();
    }

    @Override
    public String originalTimestamp() {
        return jsonObject().getAsJsonObject("timestamp").get("original").getAsString();
    }

    @Override
    public String epoch() {
        return jsonObject().getAsJsonObject("timestamp").get("epoch").getAsString();
    }

    private JsonObject jsonObject() {
        final JsonObject jsonObject;
        try {
            final JsonElement element = JsonParser.parseString(value);
            if (!element.isJsonObject()) {
                // to catch
                throw new IllegalArgumentException();
            }
            jsonObject = element.getAsJsonObject();
        }
        catch (final RuntimeException exception) {
            throw new IllegalArgumentException(
                    "could not parse epoch migration even metadata JSON from _raw column, ensure that archive is set to epoch migration mode",
                    exception
            );
        }

        return jsonObject;
    }

    @Override
    public String toString() {
        return jsonObject().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        EventMetadataFromString that = (EventMetadataFromString) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
