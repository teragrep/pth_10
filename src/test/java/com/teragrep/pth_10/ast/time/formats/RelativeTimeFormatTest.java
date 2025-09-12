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
package com.teragrep.pth_10.ast.time.formats;

import com.teragrep.pth_10.ast.time.DPLTimestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class RelativeTimeFormatTest {

    private final ZoneId utcZone = ZoneId.of("UTC");
    private final ZonedDateTime baseTime = ZonedDateTime.of(2020, 1, 2, 3, 4, 5, 6, utcZone);

    @Test
    public void testValidValue() {
        final DPLTimeFormat format = new RelativeTimeFormat(baseTime);
        final DPLTimestamp timestamp = format.from("-1y");
        Assertions.assertFalse(timestamp.isStub());
        ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(2019, zonedDateTime.getYear());
        Assertions.assertEquals(1, zonedDateTime.getMonthValue());
        Assertions.assertEquals(2, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(3, zonedDateTime.getHour());
        Assertions.assertEquals(4, zonedDateTime.getMinute());
        Assertions.assertEquals(5, zonedDateTime.getSecond());
        Assertions.assertEquals(6, zonedDateTime.getNano());
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
    }

    @Test
    public void testInvalidValue() {
        final DPLTimeFormat format = new RelativeTimeFormat(baseTime);
        final DPLTimestamp timestamp = format.from("2025/09/10T12:34:56");
        Assertions.assertTrue(timestamp.isStub());
    }

    @Test
    public void testAtZone() {
        ZoneId zone = ZoneId.of("+02:00");
        DPLTimeFormat format = new RelativeTimeFormat(baseTime).atZone(zone);
        DPLTimestamp timestamp = format.from("-1y");
        ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(zone, zonedDateTime.getZone());
        Assertions.assertEquals(2019, zonedDateTime.getYear());
        Assertions.assertEquals(1, zonedDateTime.getMonthValue());
        Assertions.assertEquals(2, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(5, zonedDateTime.getHour()); // + 2 hours
        Assertions.assertEquals(4, zonedDateTime.getMinute());
        Assertions.assertEquals(5, zonedDateTime.getSecond());
        Assertions.assertEquals(6, zonedDateTime.getNano());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(RelativeTimeFormat.class)
                .withIgnoredFields("LOGGER")
                .withNonnullFields("baseTime")
                .verify();
    }
}
