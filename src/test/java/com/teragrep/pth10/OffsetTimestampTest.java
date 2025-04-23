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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class OffsetTimestampTest {

    private final ZoneId utcZone = ZoneId.of("UTC");
    private final ZonedDateTime originTimestamp = ZonedDateTime.of(2025, 5, 15, 14, 45, 15, 790, utcZone);

    @Test
    public void testZone() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+5s", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
    }

    @Test
    public void testSeconds() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+5s", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(20, zonedDateTime.getSecond());
    }

    @Test
    public void testNegativeSeconds() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-5s", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(10, zonedDateTime.getSecond());
    }

    @Test
    public void testMinutes() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+5m", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(50, zonedDateTime.getMinute());
    }

    @Test
    public void testNegativeMinutes() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-5m", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(40, zonedDateTime.getMinute());
    }

    @Test
    public void testHours() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+5h", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(19, zonedDateTime.getHour());
    }

    @Test
    public void testNegativeHours() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-5h", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(9, zonedDateTime.getHour());
    }

    @Test
    public void testDays() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+5d", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(20, zonedDateTime.getDayOfMonth());
    }

    @Test
    public void testNegativeDays() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-5d", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(10, zonedDateTime.getDayOfMonth());
    }

    @Test
    public void testMonths() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+1month", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(6, zonedDateTime.getMonthValue());
    }

    @Test
    public void testNegativeMonths() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-1month", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(4, zonedDateTime.getMonthValue());
    }

    @Test
    public void testYears() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("+5year", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(2030, zonedDateTime.getYear());
    }

    @Test
    public void testNegativeYears() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-5year", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(2020, zonedDateTime.getYear());
    }

    @Test
    public void testNow() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("now", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        final ZonedDateTime now = ZonedDateTime.now(utcZone);
        Assertions.assertEquals(now.getZone(), zonedDateTime.getZone());
        Assertions.assertEquals(now.getYear(), zonedDateTime.getYear());
        Assertions.assertEquals(now.getMonthValue(), zonedDateTime.getMonthValue());
        Assertions.assertEquals(now.getDayOfMonth(), zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(now.getHour(), zonedDateTime.getHour());
        Assertions.assertEquals(now.getMinute(), zonedDateTime.getMinute());
    }

    @Test
    public void testMaxYears() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("99999year", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(9999, zonedDateTime.getYear());
    }

    @Test
    public void testMinYears() {
        final OffsetTimestamp snappedTimestamp = new OffsetTimestamp("-99999year", originTimestamp);
        final ZonedDateTime zonedDateTime = snappedTimestamp.zonedDateTime();
        Assertions.assertEquals(1000, zonedDateTime.getYear());
    }
}
