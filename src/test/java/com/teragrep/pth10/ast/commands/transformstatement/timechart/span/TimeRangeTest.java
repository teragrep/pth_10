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
package com.teragrep.pth10.ast.commands.transformstatement.timechart.span;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeRangeTest {

    @Test
    public void testEqualsVerifier() {
        EqualsVerifier.forClass(TimeRange.class).withNonnullFields("duration").verify();
    }

    @Test
    public void testSeconds() {
        String duration = "5 seconds";
        TimeRange timeRange = new TimeRange(duration);

        long expectedSeconds = 5;
        Assertions.assertEquals(expectedSeconds, timeRange.asSeconds());
    }

    @Test
    public void testMinutes() {
        String duration = "5 minutes";
        TimeRange timeRange = new TimeRange(duration);

        long expectedSeconds = 5 * 60;
        Assertions.assertEquals(expectedSeconds, timeRange.asSeconds());
    }

    @Test
    public void testHours() {
        String duration = "5 hours";
        TimeRange timeRange = new TimeRange(duration);

        long expectedSeconds = 5 * 60 * 60;
        Assertions.assertEquals(expectedSeconds, timeRange.asSeconds());
    }

    @Test
    public void testDays() {
        String duration = "5 days";
        TimeRange timeRange = new TimeRange(duration);

        long expectedSeconds = 5 * 60 * 60 * 24;
        Assertions.assertEquals(expectedSeconds, timeRange.asSeconds());
    }

    @Test
    public void testMonths() {
        String duration = "5 months";
        TimeRange timeRange = new TimeRange(duration);

        long expectedSeconds = 5 * 60 * 60 * 24 * 30;
        Assertions.assertEquals(expectedSeconds, timeRange.asSeconds());
    }

    @Test
    public void testYears() {
        String duration = "5 years";
        TimeRange timeRange = new TimeRange(duration);

        String expectedErrorMessage = "| timechart 'span' parameter only accepts seconds, minutes, hours, days, weeks and months. Got 'years' instead.";
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, timeRange::asSeconds);
        Assertions.assertEquals(expectedErrorMessage, e.getMessage());
    }
}
