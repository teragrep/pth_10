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

import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnquotedTextTest {

    // test with single quotes
    @Test
    public void unquoteSingleQuotesTest() {
        String original = "'foo'";
        String expected = "foo";

        String actual = new UnquotedText(new TextString(original)).read();
        Assertions.assertEquals(expected, actual);
    }

    // test with single quotes
    @Test
    public void unquoteSingleQuotesProvidedInConstructorTest() {
        String original = "'foo'";
        String expected = "foo";

        String actual = new UnquotedText(new TextString(original), "'").read();
        Assertions.assertEquals(expected, actual);
    }

    // test with single quotes
    @Test
    public void unquoteDifferentQuotesProvidedInConstructorTest() {
        String original = "'foo'";
        String expected = "'foo'";

        String actual = new UnquotedText(new TextString(original), "`", "\"").read();
        Assertions.assertEquals(expected, actual);
    }

    // test with empty quotes
    @Test
    public void unquoteEmptyQuotesTest() {
        String original = "''";
        String expected = "";

        String actual = new UnquotedText(new TextString(original)).read();
        Assertions.assertEquals(expected, actual);
    }

    // test with backtick quotes
    @Test
    public void unquoteBacktickQuotesTest() {
        String original = "`foo`";
        String expected = "foo";

        String actual = new UnquotedText(new TextString(original)).read();
        Assertions.assertEquals(expected, actual);
    }

    // test with double quotes
    @Test
    public void unquoteDoubleQuotesTest() {
        String original = "\"foo\"";
        String expected = "foo";

        String actual = new UnquotedText(new TextString(original)).read();
        Assertions.assertEquals(expected, actual);
    }

    // test with no quotes, should return the same String back
    @Test
    public void unquoteWithNoQuotesTest() {
        String original = "foo";

        String actual = new UnquotedText(new TextString(original)).read();
        Assertions.assertEquals(original, actual);
    }

    @Test
    public void testEquals() {
        UnquotedText ut1 = new UnquotedText(new TextString("foo"));
        UnquotedText ut2 = new UnquotedText(new TextString("foo"));
        Assertions.assertEquals(ut1, ut2);
    }

    @Test
    public void testNotEquals() {
        UnquotedText ut1 = new UnquotedText(new TextString("foo"));
        UnquotedText ut2 = new UnquotedText(new TextString("bar"));
        Assertions.assertNotEquals(ut1, ut2);
    }
}
