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

import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.time.ValidTrailingRelativeTimestampText;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

public class ValidTrailingRelativeTimestampTextTest {

    @Test
    public void testValidTrailText() {
        final String read = new ValidTrailingRelativeTimestampText(new TextString("+10hours@d+3h")).read();
        final String expected = "+3h";
        Assertions.assertEquals(expected, read);
    }

    @Test
    public void testValidTrailAfterWeekWithDigit() {
        final String read = new ValidTrailingRelativeTimestampText(new TextString("+10hours@w0+3h")).read();
        final String expected = "+3h";
        Assertions.assertEquals(expected, read);
    }

    @Test
    public void testNoSnapToTime() {
        final ValidTrailingRelativeTimestampText validTrailingRelativeTimestampText = new ValidTrailingRelativeTimestampText(
                new TextString("+10hours")
        );
        Assertions.assertThrows(RuntimeException.class, validTrailingRelativeTimestampText::read);
    }

    @Test
    public void testInvalidTrailText() {
        final ValidTrailingRelativeTimestampText validTrailingRelativeTimestampText = new ValidTrailingRelativeTimestampText(
                new TextString("@d")
        );
        final IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, validTrailingRelativeTimestampText::read);
        final String expectedMessage = "Could not find a valid trailing offset after '@' for value <@d>";
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    @Disabled("EqualsVerifier verify gets stuck in a Recursive datastructure even with suggested prefab values")
    public void testContract() {
        final Pattern prefabPattern1 = Pattern.compile("pattern1");
        final Pattern prefabPattern2 = Pattern.compile("pattern2");
        Assertions.assertNotEquals(prefabPattern1, prefabPattern2);
        // this happens even if the Pattern class was not included in the equals and hashcode methods.
        // the only working solution was to make the pattern a static final field.
        EqualsVerifier
                .forClass(ValidTrailingRelativeTimestampText.class)
                .withPrefabValues(Pattern.class, prefabPattern1, prefabPattern2)
                .withNonnullFields("origin", "validPattern")
                .verify();
    }
}
