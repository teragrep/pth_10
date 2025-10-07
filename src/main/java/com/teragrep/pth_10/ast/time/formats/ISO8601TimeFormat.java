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
package com.teragrep.pth_10.ast.time.formats;

import com.teragrep.pth_10.ast.time.AbsoluteTimestamp;
import com.teragrep.pth_10.ast.time.DPLTimestamp;
import com.teragrep.pth_10.ast.time.StubTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Objects;

public final class ISO8601TimeFormat implements DPLTimeFormat {

    private final Logger LOGGER = LoggerFactory.getLogger(ISO8601TimeFormat.class);
    private final String timeFormat;
    private final ZoneId zoneId;

    public ISO8601TimeFormat(final ZoneId zoneId) {
        this("yyyy-MM-dd'T'HH:mm:ss", zoneId);
    }

    private ISO8601TimeFormat(final String timeFormat, final ZoneId zoneId) {
        this.timeFormat = timeFormat;
        this.zoneId = zoneId;
    }

    @Override
    public DPLTimestamp from(final String timestampString) {
        DPLTimestamp timestamp = new AbsoluteTimestamp(timestampString, timeFormat, zoneId);
        if (!timestamp.isValid()) {
            LOGGER.debug("timestamp <[{}]> was invalid", timestampString);
            timestamp = new StubTimestamp();
        }
        else {
            LOGGER.debug("timestamp <[{}]> matched", timestampString);
        }
        return timestamp;
    }

    @Override
    public DPLTimeFormat atZone(final ZoneId zoneId) {
        return new ISO8601TimeFormat(timeFormat, zoneId);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final ISO8601TimeFormat iso8601TimeFormat = (ISO8601TimeFormat) object;
        return Objects.equals(timeFormat, iso8601TimeFormat.timeFormat)
                && Objects.equals(zoneId, iso8601TimeFormat.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeFormat, zoneId);
    }
}
