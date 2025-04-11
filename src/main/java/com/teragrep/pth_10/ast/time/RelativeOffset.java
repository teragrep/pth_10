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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * Relative offset of time. Used to add or subtract time.
 */
public final class RelativeOffset {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelativeOffset.class);

    private final long amount;
    private final RelativeTimeParser.OffsetUnit offsetUnit;

    public RelativeOffset(long amount, RelativeTimeParser.OffsetUnit offsetUnit) {
        this.amount = amount;
        this.offsetUnit = offsetUnit;
    }

    public Instant addOffset(Instant time) {
        // Larger timeunits cant use Instant, change to LocalDateTime
        LocalDateTime ldt = LocalDateTime.ofInstant(time, ZoneOffset.systemDefault());

        try {
            switch (offsetUnit) {
                case SECONDS:
                    time = time.plusSeconds(amount);
                    ldt = ldt.plusSeconds(amount);
                    break;
                case MINUTES:
                    time = time.plus(amount, ChronoUnit.MINUTES);
                    ldt = ldt.plusMinutes(amount);
                    break;
                case HOURS:
                    time = time.plus(amount, ChronoUnit.HOURS);
                    ldt = ldt.plusHours(amount);
                    break;
                case DAYS:
                    time = time.plus(amount, ChronoUnit.DAYS);
                    ldt = ldt.plusDays(amount);
                    break;
                case WEEKS:
                    ldt = ldt.plusWeeks(amount);
                    time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                    break;
                case MONTHS:
                    ldt = ldt.plusMonths(amount);
                    time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                    break;
                case YEARS:
                    ldt = ldt.plusYears(amount);
                    time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                    break;
                default:
                    throw new RuntimeException("Relative timestamp contained an invalid time unit");
            }
        }
        catch (ArithmeticException ae) {
            // on overflow, check positivity/negativity and pin to max/min
            if (amount < 0) {
                time = Instant.ofEpochMilli(0);
            }
            else {
                time = Instant.ofEpochMilli(Long.MAX_VALUE);
            }
            ldt = time.atZone(ZoneId.systemDefault()).toLocalDateTime();
        }

        if (ldt.getYear() > 9999) {
            LOGGER.info("Epoch resulted in year over 9999, setting it to year 9999.");
            ldt = ldt.withYear(9999);
            time = ldt.atZone(ZoneId.systemDefault()).toInstant();
        }
        else if (ldt.getYear() < 1000) {
            LOGGER.info("Epoch resulted in year less than 1000, setting it to year 1000.");
            ldt = ldt.withYear(1000);
            time = ldt.atZone(ZoneId.systemDefault()).toInstant();
        }

        return time;
    }
}
