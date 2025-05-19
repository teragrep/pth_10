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

import com.teragrep.pth10.ast.Text;
import com.teragrep.pth10.ast.TextString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

/**
 * Calculates an offset from a start time using a given number of time units, the number of units is extracted from a
 * class member field string
 */
public final class OffsetTimestamp implements DPLTimestamp {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetTimestamp.class);

    enum OffsetUnit {
        SECONDS, MINUTES, HOURS, DAYS, WEEKS, MONTHS, YEARS, NOW
    }

    private final Text offsetText;
    private final ZonedDateTime startTime;

    public OffsetTimestamp(final String offsetString, final ZonedDateTime startTime) {
        this(new TextString(offsetString), startTime);
    }

    public OffsetTimestamp(final Text offsetText, final ZonedDateTime startTime) {
        this.offsetText = offsetText;
        this.startTime = startTime;
    }

    @Override
    public ZonedDateTime zonedDateTime() {
        LOGGER.debug("start time <{}>", startTime);
        final OffsetUnit unit = offsetUnit();
        final long amount = offsetAmount();
        final ZonedDateTime updatedTime;
        // used "plus" methods also accept negative values
        switch (unit) {
            case NOW:
                updatedTime = ZonedDateTime.now(startTime.getZone());
                break;
            case SECONDS:
                updatedTime = startTime.plusSeconds(amount);
                break;
            case MINUTES:
                updatedTime = startTime.plusMinutes(amount);
                break;
            case HOURS:
                updatedTime = startTime.plusHours(amount);
                break;
            case DAYS:
                updatedTime = startTime.plusDays(amount);
                break;
            case WEEKS:
                updatedTime = startTime.plusWeeks(amount);
                break;
            case MONTHS:
                updatedTime = startTime.plusMonths(amount);
                break;
            case YEARS:
                updatedTime = startTime.plusYears(amount);
                break;
            default:
                throw new RuntimeException("Unsupported unit");
        }

        // ensure that year is between 1000-9999
        final long updatedYear = updatedTime.getYear();
        final ZonedDateTime updatedTimeWithYearBetweenRange;
        if (updatedYear > 9999) {
            updatedTimeWithYearBetweenRange = updatedTime.withYear(9999);
        }
        else if (updatedYear < 1000) {
            updatedTimeWithYearBetweenRange = updatedTime.withYear(1000);
        }
        else {
            updatedTimeWithYearBetweenRange = updatedTime;
        }

        LOGGER.debug("offset time <{}>", updatedTimeWithYearBetweenRange);
        return updatedTimeWithYearBetweenRange;
    }

    @Override
    public boolean isStub() {
        return false;
    }

    private long offsetAmount() {
        final String validAmountString = new ValidOffsetAmountText(offsetText).read();
        return Long.parseLong(validAmountString);
    }

    private OffsetUnit offsetUnit() {
        final String validUnitString = new ValidOffsetUnitText(offsetText).read();
        switch (validUnitString.toLowerCase()) {
            case "now":
                return OffsetUnit.NOW;
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
            default:
                throw new RuntimeException("Unsupported offset unit <" + validUnitString + "> used");
        }
    }
}
