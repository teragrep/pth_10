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

import com.teragrep.pth10.ast.time.AbsoluteTimestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class AbsoluteTimestampTest {

    private final ZoneId utcZone = ZoneId.of("UTC");

    @Test
    public void testValidDateTimeFormat() {
        final String value = "2025-05-15 14:45:00";
        final String timeformat = "yyyy-MM-dd HH:mm:ss";
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp(value, timeformat, utcZone);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(15, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(14, zonedDateTime.getHour());
        Assertions.assertEquals(45, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        Assertions.assertEquals(utcZone, zonedDateTime.getZone());
    }

    @Test
    public void testValidDateFormat() {
        final String value = "2025-05-15";
        final String format = "yyyy-MM-dd";
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp(value, format, utcZone);
        final ZonedDateTime result = timestamp.zonedDateTime();
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(5, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
        Assertions.assertEquals(0, result.getHour());
        Assertions.assertEquals(0, result.getMinute());
        Assertions.assertEquals(0, result.getSecond());
        Assertions.assertEquals(0, result.getNano());
        Assertions.assertEquals(utcZone, result.getZone());
    }

    @Test
    public void testInvalidValueForFormat() {
        final String value = "2025-05-15";
        final String format = "yyyy-dd-MM";
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp(value, format, utcZone);
        RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, timestamp::zonedDateTime);
        String expectedMessage = "Text '2025-05-15' could not be parsed: Invalid value for MonthOfYear (valid values 1 - 12): 15";
        Assertions.assertEquals(expectedMessage, runtimeException.getMessage());
    }

    @Test
    public void testUseValueZoneInformationForDateTime() {
        final String value = "2024-05-06T15:30:00+02:00";
        final String timeformat = "yyyy-MM-dd'T'HH:mm:ssXXX";
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp(value, timeformat, utcZone);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(2024, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(6, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(15, zonedDateTime.getHour());
        Assertions.assertEquals(30, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(ZoneId.of("+02:00"), zonedDateTime.getZone());
    }

    @Test
    public void testUseValueZoneInformationForDate() {
        final String value = "2024-05-06+02:00";
        final String timeformat = "yyyy-MM-ddXXX";
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp(value, timeformat, utcZone);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(2024, zonedDateTime.getYear());
        Assertions.assertEquals(5, zonedDateTime.getMonthValue());
        Assertions.assertEquals(6, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(0, zonedDateTime.getHour());
        Assertions.assertEquals(0, zonedDateTime.getMinute());
        Assertions.assertEquals(0, zonedDateTime.getSecond());
        Assertions.assertEquals(0, zonedDateTime.getNano());
        Assertions.assertEquals(ZoneId.of("+02:00"), zonedDateTime.getZone());
    }

    @Test
    public void testEpochFormatWithValidValue() {
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp("17000000", "%s", utcZone);
        final ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        final long expectedEpoch = 17000000L;
        Assertions.assertEquals(expectedEpoch, zonedDateTime.toEpochSecond());
    }

    @Test
    public void testEpochFormatWithInvalidValue() {
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp("2000-10-20", "%s", utcZone);
        final NumberFormatException numberFormatException = Assertions
                .assertThrows(NumberFormatException.class, timestamp::zonedDateTime);
        final String expected = "For input string: \"2000-10-20\"";
        Assertions.assertEquals(expected, numberFormatException.getMessage());
    }

    @Test
    public void testTriesDefaultFormatsWithEmptyTimeformat() {
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp("2001-10-20", "", utcZone);
        final RuntimeException runtimeException = Assertions
                .assertThrows(RuntimeException.class, timestamp::zonedDateTime);
        final String expected = "TimeQualifier conversion error: value <2001-10-20> couldn't be parsed using default formats.";
        Assertions.assertEquals(expected, runtimeException.getMessage());
    }

    @Test
    public void testTriesDefaultFormatsWithNullTimeformat() {
        final AbsoluteTimestamp timestamp = new AbsoluteTimestamp("2000-10-20", null, utcZone);
        final RuntimeException runtimeException = Assertions
                .assertThrows(RuntimeException.class, timestamp::zonedDateTime);
        final String expected = "TimeQualifier conversion error: value <2000-10-20> couldn't be parsed using default formats.";
        Assertions.assertEquals(expected, runtimeException.getMessage());
    }

    @Test
    public void testIsValid() {
        final AbsoluteTimestamp absoluteTimestamp = new AbsoluteTimestamp("2000-10-20", "yyyy-MM-dd", utcZone);
        final AbsoluteTimestamp invalidTimestamp = new AbsoluteTimestamp("2000-10-20", "123456", utcZone);
        Assertions.assertTrue(absoluteTimestamp.isValid());
        Assertions.assertFalse(invalidTimestamp.isValid());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(AbsoluteTimestamp.class).withNonnullFields("value", "timeformat", "zoneId").verify();
    }
}
