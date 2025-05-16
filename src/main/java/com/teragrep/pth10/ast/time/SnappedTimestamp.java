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

import com.teragrep.pth10.ast.TextString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Given a unit of time to snap to what is the resulting time from a start time, for example:
 * <p>
 * </p>
 * Seconds <b>17.30.01 = 17.30.00</b> or days <b>17.30.01 = 00.00</b>
 */
public final class SnappedTimestamp implements DPLTimestamp {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnappedTimestamp.class);

    public enum SnapUnit {
        SECONDS,
        MINUTES,
        HOURS,
        DAYS,
        MONDAY,
        TUESDAY,
        WEDNESDAY,
        THURSDAY,
        FRIDAY,
        SATURDAY,
        SUNDAY,
        MONTHS,
        QUARTER,
        YEARS
    }

    private final String timeStampString;
    private final ZonedDateTime startTime;
    private final Pattern subStringPattern = Pattern.compile("@([a-zA-Z])([+-]\\d+[a-zA-Z]+)");

    public SnappedTimestamp(final String snapUnitString, final DPLTimestamp dplTimestamp) {
        this(snapUnitString, dplTimestamp.zonedDateTime());
    }

    public SnappedTimestamp(final String timeStampString, final ZonedDateTime startTime) {
        this.timeStampString = timeStampString;
        this.startTime = startTime;
    }

    public ZonedDateTime zonedDateTime() {
        if (isStub()) {
            throw new UnsupportedOperationException("Object was stub, ZonedDateTime() method not supported");
        }
        LOGGER.info("Snap timestamp from input <{}>", timeStampString);
        final SnapUnit unit = snapUnit();
        final ZonedDateTime updatedTime;
        switch (unit) {
            case SECONDS:
                updatedTime = startTime.truncatedTo(ChronoUnit.SECONDS);
                break;
            case MINUTES:
                updatedTime = startTime.truncatedTo(ChronoUnit.MINUTES);
                break;
            case HOURS:
                updatedTime = startTime.truncatedTo(ChronoUnit.HOURS);
                break;
            case DAYS:
                updatedTime = startTime.truncatedTo(ChronoUnit.DAYS);
                break;
            case SUNDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
                break;
            case MONDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
                break;
            case TUESDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.TUESDAY));
                break;
            case WEDNESDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.WEDNESDAY));
                break;
            case THURSDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.THURSDAY));
                break;
            case FRIDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY));
                break;
            case SATURDAY:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.SATURDAY));
                break;
            case MONTHS:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.firstDayOfMonth());
                break;
            case QUARTER:
                final int year = startTime.getYear();
                final ZoneId zone = startTime.getZone();
                final ZonedDateTime q1 = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, zone);
                final ZonedDateTime q2 = ZonedDateTime.of(year, 4, 1, 0, 0, 0, 0, zone);
                final ZonedDateTime q3 = ZonedDateTime.of(year, 7, 1, 0, 0, 0, 0, zone);
                final ZonedDateTime q4 = ZonedDateTime.of(year, 10, 1, 0, 0, 0, 0, zone);

                if (q2.isAfter(startTime)) {
                    updatedTime = q1;
                }
                else if (q3.isAfter(startTime)) {
                    updatedTime = q2;
                }
                else if (q4.isAfter(startTime)) {
                    updatedTime = q3;
                }
                else {
                    updatedTime = q4;
                }
                break;
            case YEARS:
                updatedTime = startTime.with(LocalTime.MIN).with(TemporalAdjusters.firstDayOfYear());
                break;
            default:
                throw new RuntimeException("Unsupported snap unit <" + unit + ">");
        }

        // apply timestamp from value trail
        final ZonedDateTime trailTimestampIncluded;
        final ValidTrailingRelativeTimestampText validTrailingRelativeTimestampText = new ValidTrailingRelativeTimestampText(
                new TextString(timeStampString)
        );
        if (!validTrailingRelativeTimestampText.isStub()) {
            LOGGER.info("Has valid trailing timestamp");
            final OffsetTimestamp trailOffsetTimestamp = new OffsetTimestamp(
                    validTrailingRelativeTimestampText.read(),
                    updatedTime
            );
            trailTimestampIncluded = trailOffsetTimestamp.zonedDateTime();
        }
        else {
            trailTimestampIncluded = updatedTime;
        }
        return trailTimestampIncluded;
    }

    @Override
    public boolean isStub() {
        return !timeStampString.contains("@");
    }

    private SnapUnit snapUnit() {
        final String snapUnitSubstring;
        final Matcher matcher = subStringPattern.matcher(timeStampString);
        if (matcher.find()) {
            snapUnitSubstring = matcher.group(1);
        }
        else {
            snapUnitSubstring = timeStampString.substring(timeStampString.indexOf('@') + 1);
        }
        LOGGER.info("Snap unit string <{}>", snapUnitSubstring);
        final SnapUnit snapUnit;
        switch (snapUnitSubstring.toLowerCase()) {
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
                snapUnit = SnapUnit.SECONDS;
                break;
            case "m":
            case "min":
            case "minute":
            case "minutes":
                snapUnit = SnapUnit.MINUTES;
                break;
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
                snapUnit = SnapUnit.HOURS;
                break;
            case "d":
            case "day":
            case "days":
                snapUnit = SnapUnit.DAYS;
                break;
            case "w":
            case "w0":
            case "w7":
            case "week":
            case "weeks":
                snapUnit = SnapUnit.SUNDAY;
                break;
            case "w1":
                snapUnit = SnapUnit.MONDAY;
                break;
            case "w2":
                snapUnit = SnapUnit.TUESDAY;
                break;
            case "w3":
                snapUnit = SnapUnit.WEDNESDAY;
                break;
            case "w4":
                snapUnit = SnapUnit.THURSDAY;
                break;
            case "w5":
                snapUnit = SnapUnit.FRIDAY;
                break;
            case "w6":
                snapUnit = SnapUnit.SATURDAY;
                break;
            case "mon":
            case "month":
            case "months":
                snapUnit = SnapUnit.MONTHS;
                break;
            case "q":
            case "qtr":
            case "qtrs":
            case "quarter":
            case "quarters":
                snapUnit = SnapUnit.QUARTER;
                break;
            case "y":
            case "yr":
            case "yrs":
            case "year":
            case "years":
                snapUnit = SnapUnit.YEARS;
                break;
            default:
                throw new RuntimeException(
                        "Unsupported snap-to-time unit string <" + timeStampString.toLowerCase() + ">"
                );
        }
        LOGGER.info("Snapping to unit <{}>", snapUnit);
        return snapUnit;
    }
}
