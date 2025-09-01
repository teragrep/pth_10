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

import com.teragrep.pth_10.ast.Text;
import com.teragrep.pth_10.ast.TextString;
import com.teragrep.pth_10.ast.time.ValidRelativeTimestampText;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ValidRelativeTimestampTextTest {

    @Test
    public void testPlainValue() {
        final Text input = new TextString("+5d");
        final ValidRelativeTimestampText offset = new ValidRelativeTimestampText(input);
        Assertions.assertEquals("+5d", offset.read());
    }

    @Test
    public void testWithSnapValue() {
        final Text input = new TextString("+5d@hours+3");
        final ValidRelativeTimestampText offset = new ValidRelativeTimestampText(input);
        Assertions.assertEquals("+5d@hours+3", offset.read());
    }

    @Test
    public void testOnlySnapValue() {
        final Text input = new TextString("@hours+3");
        final ValidRelativeTimestampText offset = new ValidRelativeTimestampText(input);
        Assertions.assertEquals("@hours+3", offset.read());
    }

    @Test
    public void testNow() {
        final ValidRelativeTimestampText offset = new ValidRelativeTimestampText(new TextString("now"));
        final ValidRelativeTimestampText offset2 = new ValidRelativeTimestampText(new TextString("NOW"));
        Assertions.assertEquals("now", offset.read());
        Assertions.assertEquals("NOW", offset2.read());
    }

    @Test
    public void testInvalidInput() {
        final Text input = new TextString("invalid");
        final ValidRelativeTimestampText offset = new ValidRelativeTimestampText(input);
        final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, offset::read);
        final String expectedMessage = "Unknown relative time modifier string <invalid>";
        Assertions.assertEquals(expectedMessage, runtimeException.getMessage());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(ValidRelativeTimestampText.class).withNonnullFields("origin").verify();
    }
}
