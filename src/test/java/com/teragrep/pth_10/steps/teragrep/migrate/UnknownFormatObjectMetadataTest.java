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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class UnknownFormatObjectMetadataTest {

    @Test
    public void testValues() {
        final String nonSyslogJsonString = "{\"epochMigration\":true,\"format\":\"unknown\",\"object\":{\"bucket\":\"bucket\",\"path\":\"path/to/file.gz\",\"partition\":\"part1\"},\"timestamp\":{\"path-extracted\":\"2023-09-05T09:00:00Z\",\"path-extracted-precision\":\"hourly\",\"source\":\"object-path\"}}";
        final ArchiveObjectMetadata event = new UnknownFormat().parsed(nonSyslogJsonString);
        Assertions.assertFalse(event.isStub());
        Assertions.assertEquals("unknown", event.format());
        Assertions.assertEquals("bucket", event.bucket());
        Assertions.assertEquals("path/to/file.gz", event.path());
        Assertions.assertEquals("part1", event.partition());
        Assertions.assertEquals("unknown", event.epoch());
        Assertions.assertEquals("unknown", event.rfc5424Timestamp());
        Assertions.assertEquals("2023-09-05T09:00:00Z", event.pathExtracted());
        Assertions.assertEquals("hourly", event.pathExtractedPrecision());
        Assertions.assertEquals("object-path", event.source());
    }

    @Test
    public void syslogFormatIsStub() {
        final String syslogJsonString = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"path/to/file.gz\",\"partition\":\"part1\"},\"timestamp\":{\"rfc5424timestamp\":\"2023-09-05T09:00:00Z\",\"epoch\":1693904400,\"path-extracted\":\"2023-09-05T09:00:00Z\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
        final ArchiveObjectMetadata event = new UnknownFormat().parsed(syslogJsonString);
        Assertions.assertTrue(event.isStub());
        Assertions.assertThrows(UnsupportedOperationException.class, event::format);
        Assertions.assertThrows(UnsupportedOperationException.class, event::bucket);
        Assertions.assertThrows(UnsupportedOperationException.class, event::path);
        Assertions.assertThrows(UnsupportedOperationException.class, event::partition);
        Assertions.assertThrows(UnsupportedOperationException.class, event::epoch);
        Assertions.assertThrows(UnsupportedOperationException.class, event::rfc5424Timestamp);
        Assertions.assertThrows(UnsupportedOperationException.class, event::pathExtracted);
        Assertions.assertThrows(UnsupportedOperationException.class, event::pathExtractedPrecision);
        Assertions.assertThrows(UnsupportedOperationException.class, event::source);
    }

    @Test
    public void testInvalidJson() {
        final ArchiveObjectMetadata event = new UnknownFormat().parsed("invalid");
        Assertions.assertTrue(event.isStub());
    }
}
