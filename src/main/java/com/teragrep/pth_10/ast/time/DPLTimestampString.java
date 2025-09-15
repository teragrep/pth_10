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
package com.teragrep.pth_10.ast.time;

import com.teragrep.pth_10.ast.time.formats.DPLTimeFormat;
import com.teragrep.pth_10.ast.time.formats.DefaultTimeFormat;
import com.teragrep.pth_10.ast.time.formats.EpochSecondsTimeFormat;
import com.teragrep.pth_10.ast.time.formats.ISO8601TimeFormat;
import com.teragrep.pth_10.ast.time.formats.ISO8601TimeFormatWithMillis;
import com.teragrep.pth_10.ast.time.formats.ISO8601TimeFormatWithZone;
import com.teragrep.pth_10.ast.time.formats.ISO8601TimeFormatWithZoneAndMillis;
import com.teragrep.pth_10.ast.time.formats.RelativeTimeFormat;
import com.teragrep.pth_10.ast.time.formats.UserDefinedTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class DPLTimestampString {

    private final Logger LOGGER = LoggerFactory.getLogger(DPLTimestampString.class);
    private final DPLTimestamp stubTimestamp = new StubTimestamp();

    private final String timestampString;
    private final ZonedDateTime baseTime;
    private final List<DPLTimeFormat> formats;

    public DPLTimestampString(final String timestampString, final ZonedDateTime baseTime) {
        this(
                timestampString,
                baseTime,
                Arrays.asList(new DefaultTimeFormat(baseTime.getZone()), new EpochSecondsTimeFormat(baseTime.getZone()), new ISO8601TimeFormat(baseTime.getZone()), new ISO8601TimeFormatWithMillis(baseTime.getZone()), new ISO8601TimeFormatWithZone(baseTime.getZone()), new ISO8601TimeFormatWithZoneAndMillis(baseTime.getZone()), new RelativeTimeFormat(baseTime))
        );
    }

    /** Overwrites default time formats to use only the one defined in the constructor */
    public DPLTimestampString(final String timestampString, final ZonedDateTime baseTime, final String timeFormat) {
        this(
                timestampString,
                baseTime,
                Collections.singletonList(new UserDefinedTimeFormat(timeFormat, baseTime.getZone()))
        );
    }

    public DPLTimestampString(
            final String timestampString,
            final ZonedDateTime baseTime,
            final List<DPLTimeFormat> formats
    ) {
        this.timestampString = timestampString;
        this.baseTime = baseTime;
        this.formats = formats;
    }

    public DPLTimestamp asDPLTimestamp() {
        final List<DPLTimestamp> matchingTimestamps = formats
                .stream()
                .map(format -> format.atZone(baseTime.getZone())) // ensure formats are in base time zone
                .map(format -> format.from(timestampString))
                .filter(timestamp -> !timestamp.isStub())
                .collect(Collectors.toList());

        final DPLTimestamp dplTimestamp;
        if (matchingTimestamps.size() > 1) {
            LOGGER.debug("More than one match. Matching timestamps <{}>", matchingTimestamps);
            throw new IllegalArgumentException(
                    "Timestamp string <[" + timestampString + "]> matched with <" + matchingTimestamps.size()
                            + "> time formats"
            );
        }
        else if (matchingTimestamps.size() == 1) {
            dplTimestamp = matchingTimestamps.get(0);
        }
        else {
            dplTimestamp = stubTimestamp;
        }
        return dplTimestamp;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final DPLTimestampString dplTimestampString = (DPLTimestampString) object;
        return Objects.equals(timestampString, dplTimestampString.timestampString) && Objects
                .equals(baseTime, dplTimestampString.baseTime) && Objects.equals(formats, dplTimestampString.formats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampString, baseTime, formats);
    }
}
