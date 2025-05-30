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

import com.teragrep.pth10.ast.Text;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.time.ValidOffsetAmountText;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ValidOffsetAmountTextTest {

    @Test
    public void testFullTimestamp() {
        final Text input = new TextString("5d@hours+3h");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("5", offset.read());
    }

    @Test
    public void testNegativeFullTimestamp() {
        final Text input = new TextString("-5d@hours+3h");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("-5", offset.read());
    }

    @Test
    public void testJustUnit() {
        final Text input = new TextString("+d@hours+3h");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("1", offset.read());
    }

    @Test
    public void testJustNegativeUnit() {
        final Text input = new TextString("-d@hours+3h");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("-1", offset.read());
    }

    @Test
    public void testPositiveValue() {
        final Text input = new TextString("+5");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("+5", offset.read());
    }

    @Test
    public void testNegativeValue() {
        final Text input = new TextString("-5");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("-5", offset.read());
    }

    @Test
    public void testIgnoreTrail() {
        final Text input = new TextString("-5test");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("-5", offset.read());
    }

    @Test
    public void testPositiveAmount() {
        final Text input = new TextString("+");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("1", offset.read());
    }

    @Test
    public void testNegativeAmount() {
        final Text input = new TextString("-");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("-1", offset.read());
    }

    @Test
    public void testEmpty() {
        final Text input = new TextString("@d");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        Assertions.assertEquals("0", offset.read());
    }

    @Test
    public void testNonNumericValue() {
        final Text input = new TextString("invalid5@d");
        final ValidOffsetAmountText offset = new ValidOffsetAmountText(input);
        final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, offset::read);
        final String expectedMessage = "Matcher could not find a valid offset amount from <invalid5>";
        Assertions.assertEquals(expectedMessage, runtimeException.getMessage());
    }

    @Test
    public void testNowValue() {
        // "now" inputs means that no offset amount is used when calculating a timestamp
        final ValidOffsetAmountText offset1 = new ValidOffsetAmountText(new TextString("now"));
        final ValidOffsetAmountText offset2 = new ValidOffsetAmountText(new TextString("NOW"));
        Assertions.assertEquals("0", offset1.read());
        Assertions.assertEquals("0", offset2.read());
    }

    @Test
    public void testMaxOverflowFromInputValue() {
        final ValidOffsetAmountText offset1 = new ValidOffsetAmountText(
                new TextString("+" + Long.MAX_VALUE + "y@hours+3h")
        );
        Assertions.assertEquals("999999999", offset1.read());
    }

    @Test
    public void testMinValueOverflow() {
        final ValidOffsetAmountText offset1 = new ValidOffsetAmountText(new TextString(Long.MIN_VALUE + "y@hours+3h"));
        Assertions.assertEquals("-999999999", offset1.read());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(ValidOffsetAmountText.class)
                .withIgnoredFields("LOGGER")
                .withNonnullFields("origin", "pattern")
                .verify();
    }
}
