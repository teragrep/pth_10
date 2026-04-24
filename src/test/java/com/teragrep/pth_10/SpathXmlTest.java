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

import com.teragrep.pth_10.ast.commands.evalstatement.UDFs.SpathXml;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class SpathXmlTest {

    @Test
    void testSpathXmlInput() {
        final String input = "<main><sub1><item1>Hello</item1><item2>Hello2</item2></sub1><sub2><item3 id=\"30\">1</item3></sub2></main>";
        final String spathExpression = null;
        final String inputColumn = "_raw";
        final String outputColumn = "a";
        final Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put("`main.sub1.item1`", "Hello");
        expectedResult.put("`main.sub1.item2`", "Hello2");
        expectedResult.put("`main.sub2.item3`", "1");
        final Map<String, String> actualResult = new SpathXml(input, spathExpression, inputColumn, outputColumn)
                .asMap();
        Assertions.assertEquals(expectedResult, actualResult);

    }

    @Test
    void testSpathXmlInputNullPath() {
        final String input = "<main><item>Hello</item></main>";
        final String spathExpression = null;
        final String inputColumn = "_raw";
        final String outputColumn = "a";
        final Map<String, String> ExpectedResult = new HashMap<>();
        ExpectedResult.put("`main.item`", "Hello");
        final Map<String, String> ActualResult = new SpathXml(input, spathExpression, inputColumn, outputColumn)
                .asMap();
        Assertions.assertEquals(ExpectedResult, ActualResult);

    }

    @Test
    void testSpathInvalidXmlInput() {
        final String input = "main><sub>Hello</sub><sub>World</sub>main>";
        final String spathExpression = "main.sub";
        final String inputColumn = "_raw";
        final String outputColumn = "a";
        final Map<String, String> result = new SpathXml(input, spathExpression, inputColumn, outputColumn).asMap();
        Assertions.assertTrue(result.isEmpty());

    }

    @Disabled
    @Test
    void testSpathXmlInputDuplicatekeysWithPath() {
        final String input = "<main><sub><item>Hello</item><item>Hello2</item></sub><sub><item id=\"30\">1</item></sub></main>";
        final String spathExpression = "main.sub.item";
        final String inputColumn = "_raw";
        final String outputColumn = "a";
        final Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put("`main.sub.item`", "Hello\nHello2\n1");
        final Map<String, String> actualResult = new SpathXml(input, spathExpression, inputColumn, outputColumn)
                .asMap();
        Assertions.assertEquals(expectedResult, actualResult);

    }

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(SpathXml.class).withIgnoredFields("LOGGER").verify();
    }
}
