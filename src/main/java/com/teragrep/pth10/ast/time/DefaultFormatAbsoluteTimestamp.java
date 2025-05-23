/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
package com.teragrep.pth10.ast.time;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * Parser for the three default time formats that can be used: 1. MM/dd/yyyy:HH:mm:ss 2. ISO 8601 with timezone offset,
 * e.g., 2011-12-03T10:15:30+01:00 3 ISO 8601 without offset, e.g., 2011-12-03T10:15:30 When timezone is not specified,
 * uses the system default
 */
public final class DefaultFormatAbsoluteTimestamp implements DPLTimestamp {

    private final AbsoluteTimestamp[] timestamps;

    public DefaultFormatAbsoluteTimestamp(final String value) {
        this(value, TimeZone.getDefault().toZoneId());
    }

    public DefaultFormatAbsoluteTimestamp(final String value, final ZoneId zoneId) {
        this(new AbsoluteTimestamp[] {
                new AbsoluteTimestamp(value, "MM/dd/yyyy:HH:mm:ss", zoneId),
                new AbsoluteTimestamp(value, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", zoneId),
                new AbsoluteTimestamp(value, "yyyy-MM-dd'T'HH:mm:ss.SSS", zoneId),
                new AbsoluteTimestamp(value, "yyyy-MM-dd'T'HH:mm:ssXXX", zoneId),
                new AbsoluteTimestamp(value, "yyyy-MM-dd'T'HH:mm:ss", zoneId)
        });
    }

    public DefaultFormatAbsoluteTimestamp(final AbsoluteTimestamp[] timestamps) {
        this.timestamps = timestamps;
    }

    public ZonedDateTime zonedDateTime() {
        for (final DPLTimestamp timestamp : timestamps) {
            if (timestamp.isValid()) {
                return timestamp.zonedDateTime();
            }
        }
        throw new RuntimeException("TimeQualifier conversion error: can't be parsed using default formats.");
    }

    @Override
    public boolean isValid() {
        return Arrays.stream(timestamps).anyMatch(AbsoluteTimestamp::isValid);
    }
}
