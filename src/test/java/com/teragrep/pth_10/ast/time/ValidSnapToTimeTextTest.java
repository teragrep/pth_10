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
package com.teragrep.pth_10.ast.time;

import com.teragrep.pth_10.ast.TextString;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidSnapToTimeTextTest {

    @Test
    public void testSingleLetter() {
        ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours@d"));
        String result = assertDoesNotThrow(validSnapToTimeText::read);
        Assertions.assertEquals("d", result);
    }

    @Test
    public void testSingleLetterWithTrail() {
        ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours@d+10seconds"));
        String result = assertDoesNotThrow(validSnapToTimeText::read);
        Assertions.assertEquals("d", result);
    }

    @Test
    public void testWord() {
        ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours@day"));
        String result = assertDoesNotThrow(validSnapToTimeText::read);
        Assertions.assertEquals("day", result);
    }

    @Test
    public void testWordWithTrail() {
        ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours@day+10hours"));
        String result = assertDoesNotThrow(validSnapToTimeText::read);
        Assertions.assertEquals("day", result);
    }

    @Test
    public void testMissingSnapSymbol() {
        // no @
        ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours"));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, validSnapToTimeText::read);
        String expectedMessage = "Invalid snap to time text <+10hours>";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testValidWeekDays() {
        int i = 0;
        while (i < 8) {
            // w0-w7
            String input = "w" + i;
            ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours@" + input));
            String result = assertDoesNotThrow(validSnapToTimeText::read);
            Assertions.assertEquals("w" + i, result);
            i++;
        }
        Assertions.assertEquals(8, i);
    }

    @Test
    public void testInvalidWeekdays() {
        ValidSnapToTimeText validSnapToTimeText = new ValidSnapToTimeText(new TextString("+10hours@w8"));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, validSnapToTimeText::read);
        String expectedMessage = "Invalid snap to time text <+10hours@w8>";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(ValidSnapToTimeText.class).withNonnullFields("snapPattern", "origin").verify();
    }
}
