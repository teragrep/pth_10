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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.TimeZone;

/**
 * Parser for the three default timeformats that can be used: 1. MM/dd/yyyy:HH:mm:ss 2. ISO 8601 with timezone offset,
 * e.g. 2011-12-03T10:15:30+01:00 3. ISO 8601 without offset, e.g. 2011-12-03T10:15:30 When timezone is not specified,
 * uses the system default
 */
public final class DefaultFormatAbsoluteTimestamp implements DPLTimestamp {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFormatAbsoluteTimestamp.class);

    private final DateTimeFormatter[] formats;
    private final String value;
    private final ZoneId zoneId;

    public DefaultFormatAbsoluteTimestamp(final String value) {
        this(value, TimeZone.getDefault().toZoneId());
    }

    public DefaultFormatAbsoluteTimestamp(final String value, final ZoneId zoneId) {
        this(value, new DateTimeFormatter[] {
                DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
        }, zoneId);
    }

    public DefaultFormatAbsoluteTimestamp(String value, DateTimeFormatter[] formats, ZoneId zoneId) {
        this.value = value;
        this.formats = formats;
        this.zoneId = zoneId;
    }

    public ZonedDateTime zonedDateTime() {
        LOGGER.debug("Parsing value <{}> to default formats", value);
        for (final DateTimeFormatter dateTimeFormatter : formats) {
            LOGGER.debug("Trying format <{}>", dateTimeFormatter);
            try {
                // We parse the value with format and access the generic temporal object
                final TemporalAccessor parse = dateTimeFormatter.parse(value);
                if (parse.query(TemporalQueries.zone()) != null) {
                    // if a temporal object has zone information, we do not overwrite it
                    return ZonedDateTime.from(parse);
                }
                else if (parse.isSupported(ChronoField.HOUR_OF_DAY)) {
                    // if a temporal object has time information, we use date time at zone
                    return LocalDateTime.from(parse).atZone(zoneId);
                }
                else {
                    // date at zone
                    return LocalDate.from(parse).atStartOfDay(zoneId);
                }
            }
            catch (final DateTimeParseException e) {
                LOGGER.debug("Failed parse moving on to next formatter");
                // passthrough
            }
        }
        throw new RuntimeException("TimeQualifier conversion error: <" + value + "> can't be parsed using default formats.");
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
