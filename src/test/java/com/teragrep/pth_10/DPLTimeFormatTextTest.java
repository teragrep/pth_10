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

import com.teragrep.pth_10.ast.DPLTimeFormatText;
import com.teragrep.pth_10.ast.Text;
import com.teragrep.pth_10.ast.TextString;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class DPLTimeFormatTextTest {

    @Test
    public void testReadUnchanged() {
        Text s = new TextString("%s");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("%s", dplTimeFormatText.read());
    }

    @Test
    public void testReadFullYear() {
        Text s = new TextString("%Y");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("yyyy", dplTimeFormatText.read());
    }

    @Test
    public void testReadYearWithoutCentury() {
        Text s = new TextString("%y");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("yy", dplTimeFormatText.read());
    }

    @Test
    public void testReadMonth() {
        Text s = new TextString("%m");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("MM", dplTimeFormatText.read());
    }

    @Test
    public void testReadDay() {
        Text s = new TextString("%d");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("dd", dplTimeFormatText.read());
    }

    @Test
    public void testReadAbbreviatedMonth() {
        Text s = new TextString("%b");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("MMM", dplTimeFormatText.read());
    }

    @Test
    public void testReadFullMonth() {
        Text s = new TextString("%B");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("MMMM", dplTimeFormatText.read());
    }

    @Test
    public void testReadFullWeekdayName() {
        Text s = new TextString("%A");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("EEEE", dplTimeFormatText.read());
    }

    @Test
    public void testReadAbbreviatedWeekdayName() {
        Text s = new TextString("%a");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("E", dplTimeFormatText.read());
    }

    @Test
    public void testReadDayOfYear() {
        Text s = new TextString("%j");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("D", dplTimeFormatText.read());
    }

    @Test
    public void testReadWeekdayAsDecimal() {
        Text s = new TextString("%w");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("e", dplTimeFormatText.read());
    }

    @Test
    public void testReadHour24() {
        Text s = new TextString("%H");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("HH", dplTimeFormatText.read());
    }

    @Test
    public void testReadHourWithoutLeadingZeroes() {
        Text s = new TextString("%k");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("H", dplTimeFormatText.read());
    }

    @Test
    public void testReadMinute() {
        Text s = new TextString("%M");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("mm", dplTimeFormatText.read());
    }

    @Test
    public void testReadSecond() {
        Text s = new TextString("%S");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("ss", dplTimeFormatText.read());
    }

    @Test
    public void testReadHour12() {
        Text s = new TextString("%I");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("hh", dplTimeFormatText.read());
    }

    @Test
    public void testReadAMPM() {
        Text s = new TextString("%p");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("a", dplTimeFormatText.read());
    }

    @Test
    public void testReadTimeHHMMSS() {
        Text s = new TextString("%T");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("HH:mm:ss", dplTimeFormatText.read());
    }

    @Test
    public void testReadMicroseconds() {
        Text s = new TextString("%f");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("SSS", dplTimeFormatText.read());
    }

    @Test
    public void testReadTimezoneAbbreviation() {
        Text s = new TextString("%Z");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("zzz", dplTimeFormatText.read());
    }

    @Test
    public void testReadTimezoneOffset() {
        Text s = new TextString("%z");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("XXX", dplTimeFormatText.read());
    }

    @Test
    public void testReadPercentSign() {
        Text s = new TextString("%%");
        Text dplTimeFormatText = new DPLTimeFormatText(s);
        Assertions.assertEquals("%", dplTimeFormatText.read());
    }

    @Test
    public void testReadUnsupportedSymbols() {
        Text cString = new TextString("%c");
        Text cFormatted = new DPLTimeFormatText(cString);
        Text xString = new TextString("%x");
        Text xFormatted = new DPLTimeFormatText(xString);
        Assertions.assertEquals("", xFormatted.read());
        Assertions.assertEquals("", cFormatted.read());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(DPLTimeFormatText.class).withNonnullFields("origin").verify();
    }
}
