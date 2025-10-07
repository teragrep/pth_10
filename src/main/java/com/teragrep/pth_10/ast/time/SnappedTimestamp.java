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

import com.teragrep.pth_10.ast.TextString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Objects;

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

    private final ValidSnapToTimeText validSnapToTimeText;
    private final ValidTrailingRelativeTimestampText validTrailingText;
    private final ZonedDateTime startTime;

    public SnappedTimestamp(final String snapUnitString, final DPLTimestamp dplTimestamp) {
        this(snapUnitString, dplTimestamp.zonedDateTime());
    }

    public SnappedTimestamp(final String timeStampString, final ZonedDateTime startTime) {
        this(
                new ValidSnapToTimeText(new TextString(timeStampString)),
                new ValidTrailingRelativeTimestampText(new TextString(timeStampString)),
                startTime
        );
    }

    public SnappedTimestamp(
            final ValidSnapToTimeText validSnapToTimeText,
            final ValidTrailingRelativeTimestampText validTrailingText,
            ZonedDateTime startTime
    ) {
        this.validTrailingText = validTrailingText;
        this.validSnapToTimeText = validSnapToTimeText;
        this.startTime = startTime;
    }

    public ZonedDateTime zonedDateTime() {
        if (!isValid()) {
            throw new UnsupportedOperationException("Timestamp did not contain '@' for a snap to time value");
        }
        final SnapUnit unit = snapUnit();
        LOGGER.debug("Snapping time <{}> to <{}>", startTime, unit);
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

        LOGGER.debug("Valid snap to time timestamp, snapping to <{}>", unit);
        // apply timestamp from value trail
        final ZonedDateTime trailTimestampIncluded;
        if (validTrailingText.isValid()) {
            final String trailingTimestampString = validTrailingText.read();
            LOGGER.debug("Adjusting snapped time with a valid trailing timestamp <{}>", trailingTimestampString);
            final OffsetTimestamp trailOffsetTimestamp = new OffsetTimestamp(trailingTimestampString, updatedTime);
            trailTimestampIncluded = trailOffsetTimestamp.zonedDateTime();
        }
        else {
            trailTimestampIncluded = updatedTime;
        }
        return trailTimestampIncluded;
    }

    @Override
    public boolean isValid() {
        return validSnapToTimeText.containsSnapCharacter();
    }

    @Override
    public boolean isStub() {
        return false;
    }

    private SnapUnit snapUnit() {
        final String snapUnitSubstring = validSnapToTimeText.read();
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
                throw new RuntimeException("Unsupported snap-to-time unit <" + snapUnitSubstring + ">");
        }
        LOGGER.debug("Snapping to unit <{}>", snapUnit);
        return snapUnit;
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
        final SnappedTimestamp other = (SnappedTimestamp) o;
        return Objects.equals(validSnapToTimeText, other.validSnapToTimeText) && Objects
                .equals(validTrailingText, other.validTrailingText) && Objects.equals(startTime, other.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validSnapToTimeText, validTrailingText, startTime);
    }
}
