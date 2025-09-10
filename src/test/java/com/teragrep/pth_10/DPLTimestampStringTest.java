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

import com.teragrep.pth_10.ast.time.DPLTimestamp;
import com.teragrep.pth_10.ast.time.DPLTimestampString;
import com.teragrep.pth_10.ast.time.formats.UserDefinedTimeFormat;
import com.teragrep.pth_10.ast.time.formats.DPLTimeFormat;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

public final class DPLTimestampStringTest {

    private final ZonedDateTime startTime = ZonedDateTime.now(ZoneId.of("UTC"));

    @Test
    public void testUnsupportedTimestamp() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("invalid", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertTrue(timestamp.isStub());
    }

    @Test
    public void testDefaultFormat() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("04/16/2003:10:25:40", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testRelativeTimestamp() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("-1y", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testISO8601() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("2025-09-10T12:34:56", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testISO8601WithMillis() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("2025-09-10T12:34:56.789", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testISO8601WithZone() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("2025-09-10T12:34:56+02:00", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testISO8601WithZoneAndMillis() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("2025-09-10T12:34:56.789+02:00", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testEpochSeconds() {
        DPLTimestampString dplTimestampString = new DPLTimestampString("1694092800", startTime);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
    }

    @Test
    public void testUserDefinedFormatPrecedence() {
        DPLTimeFormat customFormat = new UserDefinedTimeFormat("yyyyMMdd");
        DPLTimestampString dplTimestampString = new DPLTimestampString("20250910", startTime).withFormat(customFormat);
        DPLTimestamp timestamp = dplTimestampString.asDPLTimestamp();
        Assertions.assertFalse(timestamp.isStub());
        ZonedDateTime zonedDateTime = timestamp.zonedDateTime();
        Assertions.assertEquals(2025, zonedDateTime.getYear());
        Assertions.assertEquals(9, zonedDateTime.getMonthValue());
        Assertions.assertEquals(10, zonedDateTime.getDayOfMonth());
        Assertions.assertEquals(ZoneId.of("UTC"), zonedDateTime.getZone());
    }

    @Test
    public void testMultipleMatchingUserDefinedFormats() {
        DPLTimeFormat userDefinedFormat1 = new UserDefinedTimeFormat("yyyyMMdd");
        DPLTimeFormat userDefinedFormat2 = new UserDefinedTimeFormat("yyyyMMdd");
        DPLTimestampString dplTimestampString = new DPLTimestampString("20250910", startTime)
                .withFormats(Arrays.asList(userDefinedFormat1, userDefinedFormat2));
        IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, dplTimestampString::asDPLTimestamp);
        String expectedMessage = "Timestamp string <20250910> matched with multiple user defined time formats";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(DPLTimestampString.class)
                .withIgnoredFields("LOGGER")
                .withNonnullFields("timestampString", "baseTime", "defaultFormats", "userDefinedFormats")
                .verify();
    }
}
