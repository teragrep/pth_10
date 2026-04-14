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

import java.util.ArrayList;
import java.util.List;

final class ResolvedArchiveObjectMetadata implements ArchiveObjectMetadata {

    private final String json;
    private final List<Format> archiveObjectMetadataList;

    ResolvedArchiveObjectMetadata(final String json) {
        this(json, List.of(new SyslogFormat(), new UnknownFormat()));
    }

    ResolvedArchiveObjectMetadata(final String json, final List<Format> archiveObjectMetadataList) {
        this.json = json;
        this.archiveObjectMetadataList = archiveObjectMetadataList;
    }

    private ArchiveObjectMetadata resolved() {
        final List<ArchiveObjectMetadata> validResults = new ArrayList<>();
        for (final Format format : archiveObjectMetadataList) {
            final ArchiveObjectMetadata result = format.parsed(json);
            if (!result.isStub()) {
                validResults.add(result);
            }
        }
        if (validResults.size() != 1) {
            throw new IllegalStateException(
                    "Expected one valid archive object metadata format but found <" + validResults.size() + ">"
            );
        }
        return validResults.get(0);
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public String format() {
        return resolved().format();
    }

    @Override
    public String bucket() {
        return resolved().bucket();
    }

    @Override
    public String path() {
        return resolved().path();
    }

    @Override
    public String partition() {
        return resolved().partition();
    }

    @Override
    public String epoch() {
        return resolved().epoch();
    }

    @Override
    public String rfc5424Timestamp() {
        return resolved().rfc5424Timestamp();
    }

    @Override
    public String pathExtracted() {
        return resolved().pathExtracted();
    }

    @Override
    public String pathExtractedPrecision() {
        return resolved().pathExtractedPrecision();
    }

    @Override
    public String source() {
        return resolved().source();
    }
}
