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
import com.teragrep.pth_10.ast.time.RoundedUpTimestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class RoundedUpTimestampTest {

    private final ZoneId utcZone = ZoneId.of("UTC");

    @Test
    void testWithoutNanosecond() {
        DPLTimestamp timestampFromString = new DPLTimestampString(
                "2024-01-01T00:00:00+00:00",
                ZonedDateTime.now(utcZone)
        ).asDPLTimestamp();
        DPLTimestamp timestamp = new RoundedUpTimestamp(timestampFromString);
        Assertions.assertEquals(1704067200L, timestamp.zonedDateTime().toInstant().getEpochSecond());
    }

    @Test
    void testWithNanoseconds() {
        DPLTimestamp timestampFromString = new DPLTimestampString(
                "2024-01-01T00:00:00.240+00:00",
                ZonedDateTime.now(utcZone)
        ).asDPLTimestamp();
        DPLTimestamp timestamp = new RoundedUpTimestamp(timestampFromString);
        Assertions.assertEquals(1704067200L + 1L, timestamp.zonedDateTime().toInstant().getEpochSecond());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(RoundedUpTimestamp.class).withNonnullFields("origin").verify();
    }
}
