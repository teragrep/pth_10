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
import com.teragrep.pth10.ast.UnquotedText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for relative timestamps in DPL language. Takes a String and parses it into a RelativeTimestamp -object.
 */
public class RelativeTimeParser {

    enum OffsetUnit {
        SECONDS, MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS
    }

    enum SnapUnit {
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

    private static final Logger LOGGER = LoggerFactory.getLogger(RelativeTimeParser.class);

    public RelativeTimeParser() {

    }

    /**
     * Parses the given String into a RelativeTimestamp object.
     * 
     * @param timestamp relative time as string, ex. -12h@day
     * @return relative time as object
     */
    public RelativeTimestamp parse(String timestamp) {
        timestamp = new UnquotedText(new TextString(timestamp)).read(); // strip quotes

        // regex that should match all types of relative timestamps but not normal timestamps
        Matcher relativeTimeMatcher = Pattern
                .compile("^((-|\\+)(\\d*[A-Za-z]+))?(@[A-Za-z]+(-|\\+)?[\\dA-Za-z]*)?")
                .matcher(timestamp);

        // no match and isn't keyword "now" -> assume it is a normal timestamp and use unixEpochFromString()
        if (!relativeTimeMatcher.matches() && !timestamp.equalsIgnoreCase("now")) {
            throw new NumberFormatException("Unknown relative time modifier string [" + timestamp + "]");
        }

        SnapToTime snap = null;
        RelativeOffset offset = null;
        // Check if @ is present, means snap-to-time is used if it is
        if (timestamp.indexOf('@') != -1) {
            // is there a Snap-to-time at all?
            // split the timestamp to offset (before @) and snap-to-time (after @)
            String offsetTimestamp = timestamp.substring(0, timestamp.indexOf("@"));
            String snapTimestamp = timestamp.substring(timestamp.indexOf('@') + 1);
            LOGGER.info("Snaptime=" + snapTimestamp);

            offset = parseRelativeOffset(offsetTimestamp);
            snap = parseSnapToTime(snapTimestamp);
        }
        else {
            // only offset present, or incorrect timestamp
            offset = parseRelativeOffset(timestamp);
        }

        // No relative time string, snap-to-time, nor "now" found
        if (offset == null && snap == null && !timestamp.equals("now")) {
            throw new NumberFormatException("Unknown relative time modifier string [" + timestamp + "]");
        }

        return new RelativeTimestamp(offset, snap);
    }

    // parses an offset (anything before @)
    private RelativeOffset parseRelativeOffset(String timestamp) {
        // Matches offset [+|-]d+ (leaves unit out). Can match an empty string or just a sign or just a digit
        Matcher matcher = Pattern.compile("^((-|\\+)?(\\d+)?)").matcher(timestamp);

        // [-|+]d+
        if (!matcher.find()) {
            // no relative offset found
            return null;
        }

        String valueString = matcher.group();

        long amount;
        if (valueString.equals("-")) {
            amount = -1L;
        }
        else if (valueString.equals("") || valueString.equals("+")) {
            amount = 1L;
        }
        else {
            amount = Long.parseLong(valueString);
        }

        // unit for relative offset (example: 3h, unit would be h)
        String unit = timestamp.substring(matcher.group().length());

        //LOGGER.info("v=" + v);
        //LOGGER.info("unit=" + unit);

        OffsetUnit OffsetUnit;
        // If unit exists and it is not 'now', add it to instant
        if (!unit.equals("") && !timestamp.equalsIgnoreCase("now")) {
            OffsetUnit = getOffsetUnit(unit);
        }
        // It wasn't a relative time after all if it has no unit or now
        else {
            return null;
        }

        return new RelativeOffset(amount, OffsetUnit);
    }

    // parses snap-to-time (anything after @)
    private SnapToTime parseSnapToTime(String timestamp) {
        // Check if snap-to-time has an offset at the end (example: @d+3h)
        int positiveOffsetIndex = timestamp.indexOf('+');
        int negativeOffsetIndex = timestamp.indexOf('-');
        boolean hasPositiveOffset = positiveOffsetIndex != -1;
        boolean hasNegativeOffset = negativeOffsetIndex != -1;

        RelativeOffset relativeOffset = null;
        // Separate offset from main part if it exists (example: @d+3h -> @d and +3h)
        if (hasPositiveOffset || hasNegativeOffset) {
            String offset = timestamp.substring(hasPositiveOffset ? positiveOffsetIndex : negativeOffsetIndex); // +3h
            timestamp = timestamp.substring(0, hasPositiveOffset ? positiveOffsetIndex : negativeOffsetIndex); // @d

            relativeOffset = parseRelativeOffset(offset);
        }

        SnapUnit snapUnit = getSnapUnit(timestamp);

        SnapToTime snapToTime;
        if (relativeOffset == null) {
            // snap without offset
            snapToTime = new SnapToTime(snapUnit);
        }
        else {
            // with offset
            snapToTime = new SnapToTime(snapUnit, relativeOffset);
        }

        return snapToTime;
    }

    // Change varying String unit representation into OffsetUnit Enum
    private OffsetUnit getOffsetUnit(String unit) {
        switch (unit.toLowerCase()) {
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
                return OffsetUnit.SECONDS;
            case "m":
            case "min":
            case "minute":
            case "minutes":
                return OffsetUnit.MINUTES;
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
                return OffsetUnit.HOURS;
            case "d":
            case "day":
            case "days":
                return OffsetUnit.DAYS;
            case "w":
            case "week":
            case "weeks":
                return OffsetUnit.WEEKS;
            case "mon":
            case "month":
            case "months":
                return OffsetUnit.MONTHS;
            case "y":
            case "yr":
            case "yrs":
            case "year":
            case "years":
                return OffsetUnit.YEARS;
        }

        throw new RuntimeException("Relative timestamp contained an invalid time unit");
    }

    // Change varying String unit representation into SnapUnit Enum
    private SnapUnit getSnapUnit(String unit) {
        switch (unit.toLowerCase()) {
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
                return SnapUnit.SECONDS;
            case "m":
            case "min":
            case "minute":
            case "minutes":
                return SnapUnit.MINUTES;
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
                return SnapUnit.HOURS;
            case "d":
            case "day":
            case "days":
                return SnapUnit.DAYS;
            case "w":
            case "w0":
            case "w7":
            case "week":
            case "weeks":
                // to latest sunday (beginning of week)
                return SnapUnit.SUNDAY;
            case "w1":
                // to latest monday
                return SnapUnit.MONDAY;
            case "w2":
                return SnapUnit.TUESDAY;
            case "w3":
                return SnapUnit.WEDNESDAY;
            case "w4":
                return SnapUnit.THURSDAY;
            case "w5":
                return SnapUnit.FRIDAY;
            case "w6":
                // to latest saturday
                return SnapUnit.SATURDAY;
            case "mon":
            case "month":
            case "months":
                // to beginning of month
                return SnapUnit.MONTHS;
            case "q":
            case "qtr":
            case "qtrs":
            case "quarter":
            case "quarters":
                // to most recent quarter (1.1., 1.4., 1.7. or 1.10.)
                return SnapUnit.QUARTER;
            case "y":
            case "yr":
            case "yrs":
            case "year":
            case "years":
                // to beginning of year
                return SnapUnit.YEARS;
            default:
                throw new RuntimeException("Relative timestamp contained an invalid snap-to-time time unit");
        }
    }
}
