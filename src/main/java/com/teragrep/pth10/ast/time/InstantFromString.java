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

import com.teragrep.pth10.ast.DPLTimeFormat;
import com.teragrep.pth10.ast.DefaultTimeFormat;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;

public final class InstantFromString {

    private final String value;
    private final String timeformat;

    public InstantFromString(final String value, final String timeformat) {
        this.value = value;
        this.timeformat = timeformat;
    }

    public Instant instant() {
        final String unquotedValue = new UnquotedText(new TextString(value)).read(); // erase the possible outer quotes
        final Instant timevalue;
        if (timeformat == null || timeformat.isEmpty()) {
            timevalue = new DefaultTimeFormat().parse(unquotedValue).toInstant();
        }
        else {
            // TODO: should be included in DPLTimeFormat
            if (timeformat.equals("%s")) {
                return Instant.ofEpochSecond(Long.parseLong(unquotedValue));
            }
            try {
                timevalue = new DPLTimeFormat(timeformat).instantOf(unquotedValue);
            }
            catch (ParseException e) {
                throw new RuntimeException("TimeQualifier conversion error: <" + unquotedValue + "> can't be parsed.");
            }
        }
        return timevalue;
    }

    public Instant instantAtTimezone(final ZoneId zoneId) {
        final String unquotedValue = new UnquotedText(new TextString(value)).read(); // erase the possible outer quotes
        final TimeZone timezone = TimeZone.getTimeZone(zoneId);
        final Instant timevalue;
        if (timeformat == null || timeformat.isEmpty()) {
            timevalue = new DefaultTimeFormat(timezone).parse(unquotedValue).toInstant();
        }
        else {
            // TODO: should be included in DPLTimeFormat
            if (timeformat.equals("%s")) {
                return Instant.ofEpochSecond(Long.parseLong(unquotedValue));
            }
            try {
                timevalue = new DPLTimeFormat(timeformat, timezone).instantOf(unquotedValue);
            }
            catch (ParseException e) {
                throw new RuntimeException("TimeQualifier conversion error: <" + unquotedValue + "> can't be parsed.");
            }
        }
        return timevalue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final InstantFromString that = (InstantFromString) o;
        return Objects.equals(value, that.value) && Objects.equals(timeformat, that.timeformat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timeformat);
    }
}
