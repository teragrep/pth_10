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

final class NonSyslogEvent implements EventMetadata {

    private final String bucket;
    private final String path;
    private final String partition;
    private final String pathExtracted;
    private final String pathExtractedPrecision;
    private final String source;

    NonSyslogEvent(final String json) {
        this(new ParsedJson(json).toJsonObject());
    }

    NonSyslogEvent(final JsonObject root) {
        this(root.getJsonObject("object"), root.getJsonObject("timestamp"));
    }

    NonSyslogEvent(final JsonObject object, final JsonObject timestamp) {
        this(
                object.getString("bucket"),
                object.getString("path"),
                object.getString("partition"),
                timestamp.getString("path-extracted"),
                timestamp.getString("path-extracted-precision"),
                timestamp.getString("source")
        );
    }

    NonSyslogEvent(
            final String bucket,
            final String path,
            final String partition,
            final String pathExtracted,
            final String pathExtractedPrecision,
            final String source
    ) {
        this.bucket = bucket;
        this.path = path;
        this.partition = partition;
        this.pathExtracted = pathExtracted;
        this.pathExtractedPrecision = pathExtractedPrecision;
        this.source = source;
    }

    @Override
    public boolean isSyslog() {
        return false;
    }

    @Override
    public String format() {
        return "unknown";
    }

    @Override
    public String bucket() {
        return bucket;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public String partition() {
        return partition;
    }

    @Override
    public String epoch() {
        throw new UnsupportedOperationException("epoch() not supported for NonSyslogEvent");
    }

    @Override
    public String rfc5424Timestamp() {
        throw new UnsupportedOperationException("rfc5424Timestamp() not supported for NonSyslogEvent");
    }

    @Override
    public String pathExtracted() {
        return pathExtracted;
    }

    @Override
    public String pathExtractedPrecision() {
        return pathExtractedPrecision;
    }

    @Override
    public String source() {
        return source;
    }
}
