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
package com.teragrep.pth_10;

import com.teragrep.pth_10.ast.time.RelativeTimestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

public class RelativeTimestampTest {

    private final ZoneId utcZone = ZoneId.of("UTC");
    private final ZonedDateTime originTimestamp = ZonedDateTime.of(2025, 5, 15, 14, 45, 15, 790, utcZone);

    @Test
    public void testOffset() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+5s", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        // changed value
        Assertions.assertEquals(20, zonedDateTime.getSecond());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNegativeOffset() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("-5s", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(10, zonedDateTime.getSecond());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testOffsetWithSnap() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+5month@d", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(10, zonedDateTime.getMonthValue());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
    }

    @Test
    public void testOffsetWithSnapAndTrailOffset() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+5month@d+3h", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(10, zonedDateTime.getMonthValue());
        Assertions.assertEquals(3, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
    }

    @Test
    public void testOffsetWithSnapAndNegativeTrailOffset() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+5month@d-3months", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(7, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
    }

    @Test
    public void testNow() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("now", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        final ZonedDateTime now = ZonedDateTime.now(utcZone);
        Assertions.assertEquals(now.getZone(), zonedDateTime.getZone());
        Assertions.assertEquals(now.getYear(), zonedDateTime.getYear());
        Assertions.assertEquals(now.getMonthValue(), zonedDateTime.getMonthValue());
        Assertions.assertEquals(now.getDayOfMonth(), zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(now.getHour(), zonedDateTime.getHour());
        Assertions.assertEquals(now.getMinute(), zonedDateTime.getMinute());
    }

    @Test
    public void testNowWithTrailValue() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("now@h", originTimestamp);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, relativeTimestamp::zonedDateTime);
        String expectedMessage = "Timestamp did not contain a valid relative timestamp information";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testTimeZone() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+5s", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
    }

    @Test
    public void testMaxYearLimitedToFourDigits() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+99999year", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        Assertions.assertEquals(9999, zonedDateTime.getYear());
    }

    @Test
    public void testMinYearLimitedToFourDigits() {
        final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("-99999year", originTimestamp);
        final ZonedDateTime zonedDateTime = relativeTimestamp.zonedDateTime();
        Assertions.assertEquals(1000, zonedDateTime.getYear());
    }

    @Test
    public void testPlainNumbers() {
        final RelativeTimestamp timestamp = new RelativeTimestamp("10000", originTimestamp);
        final RelativeTimestamp timestampNegative = new RelativeTimestamp("-10000", originTimestamp);
        Assertions.assertFalse(timestamp.isValid());
        Assertions.assertFalse(timestampNegative.isValid());
    }

    @Test
    public void testEmpty() {
        final RelativeTimestamp timestamp = new RelativeTimestamp("", originTimestamp);
        Assertions.assertFalse(timestamp.isValid());
    }

    @Test
    public void testWhitespace() {
        final RelativeTimestamp timestamp = new RelativeTimestamp(" ", originTimestamp);
        Assertions.assertFalse(timestamp.isValid());
    }

    @Test
    public void testSecondUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("s", "sec", "secs", "second", "seconds");
        final int amount = 100;
        final long expected = startTime.plusSeconds(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(5, loops);
    }

    @Test
    public void testMinuteUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("m", "min", "minute", "minutes");
        final int amount = 100;
        final long expected = startTime.plusMinutes(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(4, loops);
    }

    @Test
    public void testHourUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("h", "hr", "hrs", "hour", "hours");
        final int amount = 100;
        final long expected = startTime.plusHours(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(5, loops);
    }

    @Test
    public void testDayUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("d", "day", "days");
        final int amount = 100;
        final long expected = startTime.plusDays(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(3, loops);
    }

    @Test
    public void testWeekUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("w", "week", "weeks");
        final int amount = 100;
        final long expected = startTime.plusWeeks(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(3, loops);
    }

    @Test
    public void testMonthUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("mon", "month", "months");
        final int amount = 100;
        final long expected = startTime.plusMonths(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(3, loops);
    }

    @Test
    public void testYearUnits() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final List<String> units = Arrays.asList("y", "yr", "yrs", "year", "years");
        final int amount = 100;
        final long expected = startTime.plusYears(100).toEpochSecond();
        int loops = 0;
        for (final String unit : units) {
            final String relativeTimestamp = "+" + amount + unit; //+100d etc.
            final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
            loops++;
        }
        Assertions.assertEquals(5, loops);
    }

    @Test
    public void testMaxYearLimitedTo9999() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = "+10000y";
        final long expected = startTime.withYear(9999).toEpochSecond();
        final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void testMinYearLimitedTo1000() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = "-1100y";
        final long expected = startTime.withYear(1000).toEpochSecond();
        final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void testInvalidUnit() {
        final String unsupportedUnit = "xyz";
        final RelativeTimestamp timestamp = new RelativeTimestamp(unsupportedUnit, ZonedDateTime.now());
        Assertions
                .assertThrows(
                        RuntimeException.class, timestamp::zonedDateTime,
                        "Relative timestamp contained an invalid time unit"
                );
    }

    @Test
    public void testOverflowLimitedToYear9999() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = String.format("+%sy", Long.MAX_VALUE);
        final long expected = startTime.withYear(9999).toEpochSecond();
        final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void TestNegativeOverflowLimitedToYear1000() {
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = String.format("%sy", Long.MIN_VALUE);
        final long expected = startTime.withYear(1000).toEpochSecond();
        final ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(RelativeTimestamp.class)
                .withNonnullFields("offsetString", "baseTime")
                .withIgnoredFields("LOGGER")
                .verify();
    }
}
