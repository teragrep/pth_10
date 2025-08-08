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
package com.teragrep.pth10.ast.time;

import com.teragrep.pth10.ast.DPLTimeFormat;
import com.teragrep.pth10.ast.DefaultTimeFormat;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Objects;

public final class InstantTimestamp implements DPLTimestamp {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstantTimestamp.class);
    private final String value;
    private final String timeformat;

    public InstantTimestamp(final String value, final String timeformat) {
        this.value = value;
        this.timeformat = timeformat;
    }

    public Instant instant() {
        Instant rv;
        try {
            RelativeTimestamp relativeTimestamp = new RelativeTimeParser().parse(value);
            rv = relativeTimestamp.calculate(new Timestamp(System.currentTimeMillis()));
        }
        catch (NumberFormatException ne) {
            LOGGER.debug("Could not parse relative timestamp, trying default formats");
            rv = instantFromString(value, timeformat, ne);
        }

        return rv;
    }

    // Uses defaultTimeFormat if timeformat is null and DPLTimeFormat if timeformat isn't null (which means that the
    // timeformat= option was used).
    private Instant instantFromString(
            final String value,
            final String timeFormatString,
            final NumberFormatException cause
    ) {
        final String unquotedValue = new UnquotedText(new TextString(value)).read(); // erase the possible outer quotes
        final Instant timevalue;
        if (timeFormatString == null || timeFormatString.isEmpty()) {
            try {
                timevalue = new DefaultTimeFormat().parse(unquotedValue).toInstant();
            }
            catch (final DateTimeException ex) {
                throw new DateTimeException(
                        "Error parsing <" + unquotedValue + ">. " + ex.getMessage() + ". " + cause.getMessage() + ".",
                        cause
                );
            }
        }
        else {
            // TODO: should be included in DPLTimeFormat
            if (timeFormatString.equals("%s")) {
                return Instant.ofEpochSecond(Long.parseLong(unquotedValue));
            }
            try {
                timevalue = new DPLTimeFormat(timeFormatString).instantOf(unquotedValue);
            }
            catch (ParseException e) {
                throw new RuntimeException("TimeQualifier conversion error: <" + unquotedValue + "> can't be parsed.");
            }
        }
        return timevalue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InstantTimestamp that = (InstantTimestamp) o;
        return Objects.equals(value, that.value) && Objects.equals(timeformat, that.timeformat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timeformat);
    }
}
