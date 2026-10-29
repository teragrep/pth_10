/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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

import com.teragrep.pth_10.ast.DoubleValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DoubleValueTest {

    @Test
    public void testConvertLongToDouble() {
        final Object inputValue = 111111L;
        final Double expectedValue = 111111.0;

        final Double actualValue = new DoubleValue(inputValue).value();
        Assertions.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testConvertIntegerToDouble() {
        final Object inputValue = 123;
        final Double expectedValue = 123.0;

        final Double actualValue = new DoubleValue(inputValue).value();
        Assertions.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testConvertFloatToDouble() {
        final Object inputValue = 123f;
        final Double expectedValue = 123.0;

        final Double actualValue = new DoubleValue(inputValue).value();
        Assertions.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testConvertDoubleToDouble() {
        final Object inputValue = 123.4d;
        final Double expectedValue = 123.4;

        final Double actualValue = new DoubleValue(inputValue).value();
        Assertions.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testConvertStringToDouble() {
        final Object inputValue = "123";
        final Double expectedValue = 123.0;

        final Double actualValue = new DoubleValue(inputValue).value();
        Assertions.assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testConvertNonNumericalStringThrowsException() {
        final Object inputValue = "string";
        final DoubleValue actualValue = new DoubleValue(inputValue);
        final IllegalArgumentException iae = Assertions
                .assertThrows(IllegalArgumentException.class, actualValue::value);
        Assertions
                .assertEquals(
                        "Non-numerical strings cannot be converted to Double. <For input string: \"string\" >",
                        iae.getMessage()
                );
    }
}
