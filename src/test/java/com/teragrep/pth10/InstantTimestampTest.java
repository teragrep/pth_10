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

import com.teragrep.pth10.ast.time.InstantTimestamp;
import com.teragrep.pth10.ast.time.RoundedUpTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InstantTimestampTest {

    @Test
    public void testWithReadableTimeformat() {
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final Long expected = 1730325600L;
        InstantTimestamp et = new InstantTimestamp(value, timeformat);
        Assertions.assertEquals(expected, et.instant().getEpochSecond());
    }

    @Test
    public void testWithReadableTimeformatAndIsLatest() {
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final Long expected = 1730325600L;
        RoundedUpTimestamp et = new RoundedUpTimestamp(new InstantTimestamp(value, timeformat));
        Assertions.assertEquals(expected, et.instant().getEpochSecond());
    }

    @Test
    public void testWithUnixTimeformat() {
        final String value = "1730325600";
        final String timeformat = "%s";
        final Long expected = 1730325600L;
        InstantTimestamp et = new InstantTimestamp(value, timeformat);
        Assertions.assertEquals(expected, et.instant().getEpochSecond());
    }

    @Test
    public void testWithUnixTimeformatAndIsLatest() {
        final String value = "1730325600";
        final String timeformat = "%s";
        final Long expected = 1730325600L;
        RoundedUpTimestamp et = new RoundedUpTimestamp(new InstantTimestamp(value, timeformat));
        Assertions.assertEquals(expected, et.instant().getEpochSecond());
    }

    @Test
    public void testDefaultTimeformat() {
        final String value = "2024-10-31T10:10:10z";
        final String timeformat = "";
        final Long expected = 1730362210L;
        InstantTimestamp et = new InstantTimestamp(value, timeformat);
        Assertions.assertEquals(expected, et.instant().getEpochSecond());
    }

    @Test
    public void testDefaultTimeformatAndIsLatest() {
        final String value = "2024-10-31T10:10:10.001";
        final String timeformat = "";
        final Long expected = 1730362210L + 1L;
        RoundedUpTimestamp et = new RoundedUpTimestamp(new InstantTimestamp(value, timeformat));
        Assertions.assertEquals(expected, et.instant().getEpochSecond());
    }

    @Test
    public void testInvalidValue() {
        final String value = "xyz";
        final String timeformat = "%Y-%d-%m";
        RuntimeException e = Assertions
                .assertThrows(RuntimeException.class, () -> new InstantTimestamp(value, timeformat).instant());
        Assertions.assertEquals("TimeQualifier conversion error: <" + value + "> can't be parsed.", e.getMessage());
    }

    @Test
    public void testEquals() {
        final String value = "2024-10-31T10:10:10z";
        final String timeformat = "%Y-%d-%m";

        Assertions.assertEquals(new InstantTimestamp(value, timeformat), new InstantTimestamp(value, timeformat));
    }

    @Test
    public void testNotEquals() {
        final String value = "2024-10-31T10:10:10z";
        final String value2 = "2024-10-30T10:10:10z";
        final String timeformat = "%Y-%d-%m";

        Assertions.assertNotEquals(new InstantTimestamp(value, timeformat), new InstantTimestamp(value2, timeformat));
    }

}
