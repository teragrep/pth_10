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
package com.teragrep.pth10;

import com.teragrep.pth10.ast.DefaultTimeFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.TimeZone;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DefaultTimeFormatTest {

    @BeforeEach
    void enforceTimeZone() {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
    }

    @Test
    void ISO8601_fractions_latest_Test() {
        String time = "2023-12-18T12:40:53.001+03:00"; // different timeformat than default (default would be +02:00)
        // No longer handled by DefaultTimeFormat, see RoundedUpTimestamp
        long expected = 1702892453L;
        long actual = new DefaultTimeFormat().getEpoch(time);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void ISO8601_fractions_earliest_Test() {
        String time = "2023-12-18T12:40:53.001+03:00"; // different timeformat than default (default would be +02:00)
        long expected = 1702892453L;
        long actual = new DefaultTimeFormat().getEpoch(time);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void ISO8601_offsetTest() {
        String time = "2023-12-18T12:40:53+03:00"; // different timeformat than default (default would be +02:00)
        long expected = 1702892453L;
        long actual = new DefaultTimeFormat().getEpoch(time);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void ISO8601_test() {
        String time = "2023-12-18T12:40:53";
        long expected = 1702896053L;
        long actual = new DefaultTimeFormat().getEpoch(time);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void defaultTimeFormatTest() {
        String time = "12/18/2023:12:40:53";
        long expected = 1702896053L;
        long actual = new DefaultTimeFormat().getEpoch(time);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void defaultTimeFormatTest_2() {
        String time = "04/16/2020:10:25:40";
        long expected = 1587021940L;
        long actual = new DefaultTimeFormat().getEpoch(time);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void invalidTimeformat() {
        String time = "12/34/2020:10:25:40";
        RuntimeException rte = Assertions
                .assertThrows(RuntimeException.class, () -> new DefaultTimeFormat().getEpoch(time));

        Assertions
                .assertEquals(
                        "Check that the timestamp or the relative time value is in the correct format (Supported timestamp formats: [MM/dd/yyyy:HH:mm:ss, yyyy-MM-dd'T'HH:mm:ss.SSSXXX, yyyy-MM-dd'T'HH:mm:ss.SSS, yyyy-MM-dd'T'HH:mm:ssXXX, yyyy-MM-dd'T'HH:mm:ss])",
                        rte.getMessage()
                );
    }
}
