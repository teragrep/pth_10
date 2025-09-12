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

public final class DefaultTimeFormatTest {

    private final ZoneId utcZone = ZoneId.of("UTC");

    @Test
    public void testValidValue() {
        final DefaultTimeFormat format = new DefaultTimeFormat(utcZone);
        final DPLTimestamp timestamp = format.from("02/03/2020:12:13:14");
        Assertions.assertFalse(timestamp.isStub());
        ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(2020, zonedDateTime.getYear());
        Assertions.assertEquals(2, zonedDateTime.getMonthValue());
        Assertions.assertEquals(3, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(12, zonedDateTime.getHour());
        Assertions.assertEquals(13, zonedDateTime.getMinute());
        Assertions.assertEquals(14, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
    }

    @Test
    public void testInvalidValue() {
        final DefaultTimeFormat format = new DefaultTimeFormat(utcZone);
        final DPLTimestamp timestamp = format.from("02-03-2020:12:13:14");
        Assertions.assertTrue(timestamp.isStub());
    }

    @Test
    public void testAtZone() {
        ZoneId zone = ZoneId.of("Europe/Helsinki");
        DPLTimeFormat format = new DefaultTimeFormat(utcZone).atZone(zone);
        DPLTimestamp timestamp = format.from("02/03/2020:12:13:14");
        ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(zone, zonedDateTime.getZone());
        Assertions.assertEquals(2020, zonedDateTime.getYear());
        Assertions.assertEquals(2, zonedDateTime.getMonthValue());
        Assertions.assertEquals(3, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(12, zonedDateTime.getHour());
        Assertions.assertEquals(13, zonedDateTime.getMinute());
        Assertions.assertEquals(14, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(DefaultTimeFormat.class)
                .withIgnoredFields("LOGGER")
                .withNonnullFields("timeFormat", "zoneId")
                .verify();
    }
}
