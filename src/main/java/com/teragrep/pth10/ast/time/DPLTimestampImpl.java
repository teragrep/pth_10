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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Determines a point for time from input string, timeformat and a timezone id. Supports relative and absolute
 * timestamps, relative is used if possible to determine the offset
 */
public final class DPLTimestampImpl implements DPLTimestamp {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLTimestampImpl.class);
    private final String value;
    private final String timeformat;
    private final ZoneId zoneId;

    public DPLTimestampImpl(final String value) {
        this(value, "", TimeZone.getDefault().toZoneId());
    }

    public DPLTimestampImpl(final String value, final String timeformat) {
        this(value, timeformat, TimeZone.getDefault().toZoneId());
    }

    public DPLTimestampImpl(final String value, final String timeformat, final ZoneId zoneId) {
        this.value = value;
        this.timeformat = timeformat;
        this.zoneId = zoneId;
    }

    public ZonedDateTime zonedDateTime() {
        LOGGER.info("Incoming value <{}> to timestamp", value);
        final DPLTimestamp timestamp;
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp(value, zoneId);
        if (!relativeTimestamp.isStub()) {
            timestamp = relativeTimestamp;
        }
        else {
            timestamp = new AbsoluteTimestamp(value, timeformat, zoneId);
        }
        return timestamp.zonedDateTime();
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final DPLTimestampImpl that = (DPLTimestampImpl) o;
        return Objects.equals(value, that.value) && Objects.equals(timeformat, that.timeformat)
                && Objects.equals(zoneId, that.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timeformat, zoneId);
    }
}
