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
package com.teragrep.pth_10.ast;

import java.util.Objects;

/** replaces dpl time units with java-compatible time units */
public final class DPLTimeFormatText implements Text {

    private final Text origin;

    public DPLTimeFormatText(final Text origin) {
        this.origin = origin;
    }

    @Override
    public String read() {
        String read = origin.read();
        if ("%s".equals(read)) {
            return read;
        }
        return read
                .replaceAll("%F", "yyyy-MM-dd") // ISO 8601 %Y-%m-%d
                .replaceAll("%y", "yy") // year without century (00-99)
                .replaceAll("%Y", "yyyy") // full year
                .replaceAll("%m", "MM") // month 1-12
                .replaceAll("%d", "dd") // day 1-31
                .replaceAll("%b", "MMM") // abbrv. month name
                .replaceAll("%B", "MMMM") // full month name
                .replaceAll("%A", "EEEE") // full weekday name, e.g. "sunday"
                .replaceAll("%a", "E") // abbrv. weekday name, e.g. "Sun"
                .replaceAll("%j", "D") // day of year, 001-366
                .replaceAll("%w", "e") // weekday as decimal 0=sun 6=sat
                // Time
                .replaceAll("%H", "HH") // hour 0-23
                .replaceAll("%k", "H") // hour without leading zeroes
                .replaceAll("%M", "mm") // minute 0-59
                .replaceAll("%S", "ss") // second 0-59
                .replaceAll("%I", "hh") // hour 1-12
                .replaceAll("%p", "a") // am/pm
                .replaceAll("%T", "HH:mm:ss") // hour:min:sec
                .replaceAll("%f", "SSS") // microsecs
                // Time zone
                .replaceAll("%Z", "zzz") // timezone abbreviation
                .replaceAll("%z", "XXX") // timezone offset +00:00
                // Other
                .replaceAll("%%", "%") // percent sign
                // remove unsupported
                .replaceAll("%c", "")
                .replaceAll("%x", "");
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
        final DPLTimeFormatText other = (DPLTimeFormatText) o;
        return Objects.equals(origin, other.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(origin);
    }
}
