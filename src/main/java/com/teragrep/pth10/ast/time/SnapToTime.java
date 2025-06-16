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

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

/**
 * Truncates time to the given unit. Can contain an offset. Example: @d+3h snaps to 3AM of the same day.
 */
public final class SnapToTime {

    private final RelativeTimeParser.SnapUnit snapUnit;
    private final RelativeOffset offset;

    public SnapToTime(RelativeTimeParser.SnapUnit snapUnit, RelativeOffset offset) {
        this.snapUnit = snapUnit;
        this.offset = offset;
    }

    public SnapToTime(RelativeTimeParser.SnapUnit snapUnit) {
        this.snapUnit = snapUnit;
        this.offset = new RelativeOffset(0L, RelativeTimeParser.OffsetUnit.DAYS); // empty
    }

    /**
     * Truncate the time to the start of the SnapUnit and add the offset.
     * 
     * @param time original time
     * @return modified time
     */
    public Instant snap(Instant time) {
        time = snapToTime(time);
        time = offset.addOffset(time);
        return time;
    }

    // Snap time to given time unit
    private Instant snapToTime(Instant time) {
        // used for weeks and bigger units of time
        LocalDateTime ldt = LocalDateTime.ofInstant(time, ZoneOffset.systemDefault());
        ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneOffset.systemDefault());

        // snap to given time unit
        switch (snapUnit) {
            case SECONDS:
                // 17.30.01 -> 17.30.00
                time = zdt.truncatedTo(ChronoUnit.SECONDS).toInstant();
                break;
            case MINUTES:
                // 17.30.01 -> 17.30
                time = zdt.truncatedTo(ChronoUnit.MINUTES).toInstant();
                break;
            case HOURS:
                // 17.30.01 -> 17.00
                time = zdt.truncatedTo(ChronoUnit.HOURS).toInstant();
                break;
            case DAYS:
                // 17.30.01 -> 00.00
                time = zdt.truncatedTo(ChronoUnit.DAYS).toInstant();
                break;

            /* Week and larger units can't use truncate */
            case SUNDAY:
                // to latest sunday (beginning of week)
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case MONDAY:
                // to latest monday
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case TUESDAY:
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.TUESDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case WEDNESDAY:
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.WEDNESDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case THURSDAY:
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.THURSDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case FRIDAY:
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.FRIDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case SATURDAY:
                // to latest saturday
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.previousOrSame(DayOfWeek.SATURDAY));
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case MONTHS:
                // to beginning of month
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.firstDayOfMonth());
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            case QUARTER:
                // to most recent quarter (1.1., 1.4., 1.7. or 1.10.)
                int year = ldt.getYear();
                LocalDateTime q1_ldt = LocalDateTime.of(year, 1, 1, 0, 0);
                LocalDateTime q2_ldt = LocalDateTime.of(year, 4, 1, 0, 0);
                LocalDateTime q3_ldt = LocalDateTime.of(year, 7, 1, 0, 0);
                LocalDateTime q4_ldt = LocalDateTime.of(year, 10, 1, 0, 0);

                // Time between Q1-Q2
                if ((q1_ldt.isBefore(ldt) || q1_ldt.isEqual(ldt)) && q2_ldt.isAfter(ldt)) {
                    time = q1_ldt.atZone(ZoneId.systemDefault()).toInstant();
                }
                // Between Q2-Q3
                else if ((q2_ldt.isBefore(ldt) || q2_ldt.isEqual(ldt)) && q3_ldt.isAfter(ldt)) {
                    time = q2_ldt.atZone(ZoneId.systemDefault()).toInstant();
                }
                // Between Q3-Q4
                else if ((q3_ldt.isBefore(ldt) || q3_ldt.isEqual(ldt)) && q4_ldt.isAfter(ldt)) {
                    time = q3_ldt.atZone(ZoneId.systemDefault()).toInstant();
                }
                // After Q4
                else if ((q4_ldt.isBefore(ldt) || q4_ldt.isEqual(ldt))) {
                    time = q4_ldt.atZone(ZoneId.systemDefault()).toInstant();
                }
                else {
                    // Should not happen
                    throw new UnsupportedOperationException("Snap-to-time @q could not snap to closest quarter!");
                }

                break;
            case YEARS:
                // to beginning of year
                ldt = ldt.with(LocalTime.MIN).with(TemporalAdjusters.firstDayOfYear());
                time = ldt.atZone(ZoneId.systemDefault()).toInstant();
                break;
            default:
                throw new RuntimeException("Relative timestamp contained an invalid snap-to-time time unit");
        }

        return time;
    }
}
