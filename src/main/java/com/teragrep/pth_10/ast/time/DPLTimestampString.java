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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class DPLTimestampString {

    private final Logger LOGGER = LoggerFactory.getLogger(DPLTimestampString.class);

    private final String timestampString;
    private final ZonedDateTime baseTime;
    private final List<DPLTimeFormat> defaultFormats;
    private final List<DPLTimeFormat> customFormats;

    public DPLTimestampString(final String timestampString, ZonedDateTime baseTime) {
        this(
                timestampString,
                baseTime,
                Arrays
                        .asList(
                                new DefaultTimeFormat(), new EpochSecondsTimeFormat(), new ISO8601TimeFormat(),
                                new ISO8601TimeFormatWithMillis(), new ISO8601TimeFormatWithZone(),
                                new ISO8601TimeFormatWithZoneAndMillis(), new RelativeTimeFormat(baseTime)
                        ),
                Collections.emptyList()
        );
    }

    public DPLTimestampString(
            final String timestampString,
            final ZonedDateTime baseTime,
            final List<DPLTimeFormat> defaultFormats,
            final List<DPLTimeFormat> customFormats
    ) {
        this.timestampString = timestampString;
        this.baseTime = baseTime;
        this.defaultFormats = defaultFormats;
        this.customFormats = customFormats;
    }

    public DPLTimestampString withFormat(final DPLTimeFormat addedFormat) {
        final List<DPLTimeFormat> newCustomFormats = new ArrayList<>(customFormats);
        // ensure format is in the same zone as base time
        DPLTimeFormat addedFormatAtBaseTimeZone = addedFormat.atZone(baseTime.getZone());
        if (addedFormat.equals(addedFormatAtBaseTimeZone)) {
            LOGGER.debug("Overwrote <{}> zone with base time's zone", addedFormat);
        }
        newCustomFormats.add(addedFormatAtBaseTimeZone);
        return new DPLTimestampString(timestampString, baseTime, defaultFormats, newCustomFormats);
    }

    public DPLTimestampString withFormats(final List<DPLTimeFormat> addedFormatsList) {
        final List<DPLTimeFormat> newCustomFormats = new ArrayList<>(customFormats);
        // map added formats to the base time zone
        final List<DPLTimeFormat> addedFormatsInBaseTimeZone = addedFormatsList
                .stream()
                .map(f -> f.atZone(baseTime.getZone()))
                .collect(Collectors.toList());
        if (addedFormatsList.equals(addedFormatsInBaseTimeZone)) {
            LOGGER.debug("Overwrote <{}> formats with base time's zone", addedFormatsList);
        }
        newCustomFormats.addAll(addedFormatsInBaseTimeZone);
        return new DPLTimestampString(timestampString, baseTime, defaultFormats, newCustomFormats);
    }

    public DPLTimestamp asDPLTimestamp() {
        final List<DPLTimestamp> customMatchingTimestamps = customMatchingTimestamps();
        final List<DPLTimestamp> defaultMatchingTimestamps = defaultMatchingTimestamps();
        final DPLTimestamp dplTimestamp;

        // prioritize matching with custom timestamps
        // separated because certain custom formats can match with the provided default formats
        if (customMatchingTimestamps.size() > 1) {
            throw new IllegalStateException(
                    "String <" + timestampString + "> matched with multiple custom time formats"
            );
        }
        else if (customMatchingTimestamps.size() == 1) {
            dplTimestamp = customMatchingTimestamps.get(0);
        }
        else if (defaultMatchingTimestamps.size() > 1) {
            throw new IllegalStateException("String <" + timestampString + "> matched with multipel time formats");
        }
        else if (defaultMatchingTimestamps.size() == 1) {
            dplTimestamp = defaultMatchingTimestamps.get(0);
        }
        else {
            dplTimestamp = new StubTimestamp();
        }
        return dplTimestamp;
    }

    private List<DPLTimestamp> customMatchingTimestamps() {
        return customFormats
                .stream()
                .map(format -> format.from(timestampString))
                .filter(timestamp -> !timestamp.isStub())
                .collect(Collectors.toList());
    }

    private List<DPLTimestamp> defaultMatchingTimestamps() {
        return defaultFormats
                .stream()
                .map(format -> format.from(timestampString))
                .filter(timestamp -> !timestamp.isStub())
                .collect(Collectors.toList());
    }
}
