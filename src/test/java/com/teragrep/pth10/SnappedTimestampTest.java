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
package com.teragrep.pth10;

import com.teragrep.pth10.ast.time.SnappedTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SnappedTimestampTest {

    private final ZoneId utcZone = ZoneId.of("UTC");
    private final ZonedDateTime originTimestamp = ZonedDateTime.of(2025, 5, 15, 14, 45, 15, 790, utcZone);

    @Test
    public void testFullTimestamp() {
        Assertions.assertEquals(790, originTimestamp.getNano());
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("+5d@minutes", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
    }

    @Test
    public void testSecond() {
        Assertions.assertEquals(790, originTimestamp.getNano());
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@seconds", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@s", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@sec", originTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@secs", originTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@second", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@minutes", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testMinute() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@minutes", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@m", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@min", originTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@minute", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testHour() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@hours", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@h", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@hr", originTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@hrs", originTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@hour", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testDay() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@days", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@d", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@day", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testWeekSnapToSunday() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@weeks", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(11, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(DayOfWeek.SUNDAY, zonedDateTime.getDayOfWeek());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@w", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@w0", originTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@w7", originTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@week", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testWeekDays() {
        final List<String> weekDays = Collections
                .unmodifiableList(Arrays.asList("@w1", "@w2", "@w3", "@w4", "@w5", "@w6"));
        int loops = 1;
        for (final String weekDayString : weekDays) {
            final SnappedTimestamp snappedTimestamp = new SnappedTimestamp(weekDayString, originTimestamp);
            final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
            Assertions.assertEquals(utcZone, zonedDateTime.getZone());
            Assertions.assertEquals(DayOfWeek.of(loops), zonedDateTime.getDayOfWeek()); // 1 = Monday, 2 = Tuesday etc.
            loops++;
        }
        Assertions.assertEquals(7, loops);
    }

    @Test
    public void testMonth() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@months", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(1, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@mon", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@month", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testYear() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@years", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(1, zonedDateTime.getMonthValue());
        Assertions.assertEquals(1, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@y", originTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@yr", originTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@yrs", originTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@year", originTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", originTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testFirstQuarter() {
        final ZonedDateTime firstQuarterTimestamp = ZonedDateTime.of(2025, 2, 15, 14, 45, 15, 790, utcZone);
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@quarters", firstQuarterTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(1, zonedDateTime.getMonthValue());
        Assertions.assertEquals(1, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@q", firstQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@qtr", firstQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@qtrs", firstQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@quarter", firstQuarterTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", firstQuarterTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testSecondQuarter() {
        final ZonedDateTime secondQuarterTimestamp = ZonedDateTime.of(2025, 5, 15, 14, 45, 15, 790, utcZone);
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@quarters", secondQuarterTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(4, zonedDateTime.getMonthValue());
        Assertions.assertEquals(1, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@q", secondQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@qtr", secondQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@qtrs", secondQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@quarter", secondQuarterTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", secondQuarterTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testThirdQuarter() {
        final ZonedDateTime thirdQuarterTimestamp = ZonedDateTime.of(2025, 8, 15, 14, 45, 15, 790, utcZone);
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@quarters", thirdQuarterTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(7, zonedDateTime.getMonthValue());
        Assertions.assertEquals(1, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@q", thirdQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@qtr", thirdQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@qtrs", thirdQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@quarter", thirdQuarterTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", thirdQuarterTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testFourthQuarter() {
        final ZonedDateTime fourthQuarterTimestamp = ZonedDateTime.of(2025, 11, 15, 14, 45, 15, 790, utcZone);
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@quarters", fourthQuarterTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        ;
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(10, zonedDateTime.getMonthValue());
        Assertions.assertEquals(1, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // test all possible snap unit strings
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@q", fourthQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp3 = new SnappedTimestamp("@qtr", fourthQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp4 = new SnappedTimestamp("@qtrs", fourthQuarterTimestamp);
        final SnappedTimestamp snappedTimestamp5 = new SnappedTimestamp("@quarter", fourthQuarterTimestamp);
        final SnappedTimestamp nonEqual = new SnappedTimestamp("@seconds", fourthQuarterTimestamp);
        Assertions.assertEquals(zonedDateTime, snappedTimestamp2.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp3.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp4.zonedDateTime());
        Assertions.assertEquals(zonedDateTime, snappedTimestamp5.zonedDateTime());
        Assertions.assertNotEquals(zonedDateTime, nonEqual.zonedDateTime());
    }

    @Test
    public void testUnsupportedSnapUnitString() {
        final SnappedTimestamp snappedTimestamp = new SnappedTimestamp("@test", originTimestamp);
        RuntimeException runtimeException = Assertions
                .assertThrows(RuntimeException.class, snappedTimestamp::zonedDateTime);
        String expectedMessage = "Unsupported snap-to-time unit string <@test>";
        Assertions.assertEquals(expectedMessage, runtimeException.getMessage());
    }

    @Test
    public void testStartTimeImmutability() {
        final SnappedTimestamp snappedTimestamp1 = new SnappedTimestamp("@days", originTimestamp);
        final SnappedTimestamp snappedTimestamp2 = new SnappedTimestamp("@days", originTimestamp);
        final ZonedDateTime zonedDateTime1 = snappedTimestamp1.zonedDateTime();
        final ZonedDateTime zonedDateTime2 = snappedTimestamp2.zonedDateTime();
        Assertions.assertEquals(zonedDateTime1, zonedDateTime2);
    }

    @Test
    public void testIsStub() {
        final SnappedTimestamp nonStubTimestamp = new SnappedTimestamp("@days", originTimestamp);
        final SnappedTimestamp stubTimestamp = new SnappedTimestamp("days", originTimestamp);
        Assertions.assertFalse(nonStubTimestamp.isStub());
        Assertions.assertTrue(stubTimestamp.isStub());
    }
}
