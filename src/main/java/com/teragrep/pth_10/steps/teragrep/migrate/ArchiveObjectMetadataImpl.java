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

final class ArchiveObjectMetadataImpl implements ArchiveObjectMetadata {

    private final String format;
    private final String bucket;
    private final String path;
    private final String partition;
    private final String epoch;
    private final String rfc5424Timestamp;
    private final String pathExtracted;
    private final String pathExtractedPrecision;
    private final String source;

    ArchiveObjectMetadataImpl(
            final String format,
            final String bucket,
            final String path,
            final String partition,
            final String epoch,
            final String rfc5424Timestamp,
            final String pathExtracted,
            final String pathExtractedPrecision,
            final String source
    ) {
        this.format = format;
        this.bucket = bucket;
        this.path = path;
        this.partition = partition;
        this.epoch = epoch;
        this.rfc5424Timestamp = rfc5424Timestamp;
        this.pathExtracted = pathExtracted;
        this.pathExtractedPrecision = pathExtractedPrecision;
        this.source = source;
    }

    @Override
    public String format() {
        return format;
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
        return epoch;
    }

    @Override
    public String rfc5424Timestamp() {
        return rfc5424Timestamp;
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

    @Override
    public boolean isStub() {
        return false;
    }
}
