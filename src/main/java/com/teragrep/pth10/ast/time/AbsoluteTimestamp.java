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

import com.teragrep.pth10.ast.DPLTimeFormatText;
import com.teragrep.pth10.ast.Text;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Objects;

public final class AbsoluteTimestamp implements DPLTimestamp {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbsoluteTimestamp.class);

    private final Text value;
    private final String timeformat;
    private final ZoneId zoneId;

    public AbsoluteTimestamp(final String value, final String timeformat, final ZoneId zoneId) {
        this(new UnquotedText(new TextString(value)), timeformat, zoneId);
    }

    public AbsoluteTimestamp(final Text value, final String timeformat, final ZoneId zoneId) {
        this.value = value;
        this.timeformat = timeformat;
        this.zoneId = zoneId;
    }

    @Override
    public ZonedDateTime zonedDateTime() throws DateTimeParseException {
        final ZonedDateTime zonedDateTime;
        final String unquotedValue = value.read();
        // default formats
        if (timeformat == null || timeformat.isEmpty()) {
            LOGGER.info("No timeformat provided for value <{}> using default formats", unquotedValue);
            final DefaultFormatAbsoluteTimestamp defaultFormatAbsoluteTimestamp = new DefaultFormatAbsoluteTimestamp(
                    unquotedValue,
                    zoneId
            );
            zonedDateTime = defaultFormatAbsoluteTimestamp.zonedDateTime();
        }
        // direct epoch from value
        else if (timeformat.equals("%s")) {
            LOGGER.info("Parsing value <{}> directly to epoch", unquotedValue);
            zonedDateTime = Instant.ofEpochSecond(Long.parseLong(unquotedValue)).atZone(zoneId);
        }
        // examine value with formatter using timeformat
        else {
            final String javaAcceptedTimeFormat = new DPLTimeFormatText(new UnquotedText(new TextString(timeformat)))
                    .read();
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(javaAcceptedTimeFormat);
            LOGGER.info("Parsing value <{}> with format <{}>", unquotedValue, javaAcceptedTimeFormat);
            final TemporalAccessor parseResult = dateTimeFormatter.parse(unquotedValue);
            // use zone from value if available
            if (parseResult.query(TemporalQueries.zone()) != null) {
                LOGGER.debug("Using time zone from parsed value");
                if (parseResult.isSupported(ChronoField.HOUR_OF_DAY)) {
                    zonedDateTime = ZonedDateTime.from(parseResult);
                }
                else {
                    zonedDateTime = LocalDate.from(parseResult).atStartOfDay(parseResult.query(TemporalQueries.zone()));
                }
            }
            // if no zone information uses class default
            else if (parseResult.isSupported(ChronoField.HOUR_OF_DAY)) {
                zonedDateTime = LocalDateTime.from(parseResult).atZone(zoneId);
            }
            else {
                zonedDateTime = LocalDate.from(parseResult).atStartOfDay(zoneId);
            }
        }
        return zonedDateTime;
    }

    @Override
    public boolean isValid() {
        boolean isValid = true;
        try {
            zonedDateTime();
        }
        catch (final DateTimeParseException exception) {
            isValid = false;
        }
        return isValid;
    }

    @Override
    public String toString() {
        return String.format("AbsoluteTimestamp value<%s> timeformat<%s>", value, timeformat);
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
        final AbsoluteTimestamp other = (AbsoluteTimestamp) o;
        return Objects.equals(value, other.value) && Objects.equals(timeformat, other.timeformat)
                && Objects.equals(zoneId, other.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timeformat, zoneId);
    }
}
