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

import com.teragrep.pth10.ast.time.OffsetTimestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class OffsetTimestampTest {

    private final ZoneId utcZone = ZoneId.of("UTC");
    private final ZonedDateTime originTimestamp = ZonedDateTime.of(2025, 5, 15, 14, 45, 15, 790, utcZone);

    @Test
    public void testSeconds() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("+5s", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
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
    public void testNegativeSeconds() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-5s", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
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
    public void testMinutes() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("+5m", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(50, zonedDateTime.getMinute());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNegativeMinutes() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-5m", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(40, zonedDateTime.getMinute());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testHours() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("+5h", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(19, zonedDateTime.getHour());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNegativeHours() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-5h", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(9, zonedDateTime.getHour());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testDays() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("+5d", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(20, zonedDateTime.getDayOfMonth());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNegativeDays() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-5d", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(10, zonedDateTime.getDayOfMonth());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testMonths() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("+1month", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(6, zonedDateTime.getMonthValue());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNegativeMonths() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-1month", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(4, zonedDateTime.getMonthValue());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testYears() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("+5year", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(2030, zonedDateTime.getYear());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNegativeYears() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-5year", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        // changed
        Assertions.assertEquals(2020, zonedDateTime.getYear());
        // unchanged
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(15, zonedDateTime.getSecond());
        Assertions.assertEquals(790, zonedDateTime.getNano());
    }

    @Test
    public void testNow() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("now", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        final ZonedDateTime now = ZonedDateTime.now(utcZone);
        Assertions.assertEquals(now.getZone(), zonedDateTime.getZone());
        Assertions.assertEquals(now.getYear(), zonedDateTime.getYear());
        Assertions.assertEquals(now.getMonthValue(), zonedDateTime.getMonthValue());
        Assertions.assertEquals(now.getDayOfMonth(), zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(now.getHour(), zonedDateTime.getHour());
        Assertions.assertEquals(now.getMinute(), zonedDateTime.getMinute());
        Assertions.assertEquals(now.getSecond(), zonedDateTime.getSecond());
    }

    @Test
    public void testMaxYears() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("99999year", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(9999, zonedDateTime.getYear());
    }

    @Test
    public void testMinYears() {
        final OffsetTimestamp timestamp = new OffsetTimestamp("-99999year", originTimestamp);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(1000, zonedDateTime.getYear());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(OffsetTimestamp.class)
                .withIgnoredFields("LOGGER")
                .withNonnullFields("offsetText", "startTime")
                .verify();
    }
}
