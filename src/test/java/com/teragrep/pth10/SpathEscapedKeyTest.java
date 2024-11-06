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

import com.teragrep.pth10.steps.spath.SpathEscapedKey;
import com.teragrep.pth10.steps.spath.SpathKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpathEscapedKeyTest {

    @Test
    public void testUnscapedKey() {
        final String spathKey = "`hello.world`";
        final SpathKey expected = new SpathKey("hello.world");
        SpathEscapedKey key = new SpathEscapedKey(spathKey);
        Assertions.assertEquals(expected, key.unescaped());
    }

    @Test
    public void testEmptyStringFail() {
        final String spathKey = "";
        IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, () -> new SpathEscapedKey(spathKey).unescaped());
        Assertions.assertEquals("SpathKey cannot be null or empty!", exception.getMessage());
    }

    @Test
    public void testKeyWithNoBackticks() {
        final String spathKey = "hello.world";
        IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, () -> new SpathEscapedKey(spathKey).unescaped());
        Assertions
                .assertEquals(
                        "SpathKey must be wrapped in backticks, but it was not!" + spathKey, exception.getMessage()
                );
    }

    @Test
    public void testKeyWithOneBacktick() {
        final String spathKey = "hello.world`";
        IllegalArgumentException exception = Assertions
                .assertThrows(IllegalArgumentException.class, () -> new SpathEscapedKey(spathKey).unescaped());
        Assertions
                .assertEquals(
                        "SpathKey must be wrapped in backticks, but it was not!" + spathKey, exception.getMessage()
                );
    }

    @Test
    void testEquality() {
        final String source = "`hello.world`";
        final SpathEscapedKey key = new SpathEscapedKey(source);
        final SpathEscapedKey key2 = new SpathEscapedKey(source);
        Assertions.assertEquals(key, key2);
    }

    @Test
    void testNonEquality() {
        final String source = "`hello.world`";
        final String source2 = "`hello2.world`";
        final SpathEscapedKey key = new SpathEscapedKey(source);
        final SpathEscapedKey key2 = new SpathEscapedKey(source2);
        Assertions.assertNotEquals(key, key2);
    }
}
