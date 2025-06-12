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

import com.teragrep.pth10.ast.time.DecreasedEpochXmlElementValue;
import com.teragrep.pth10.ast.time.TimeQualifier;
import com.teragrep.pth10.ast.time.TimeQualifierImpl;
import com.teragrep.pth_03.antlr.DPLLexer;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;

public class DecreasedEpochXmlElementValueTest {

    @Test
    public void testDecreasedEpochValueDecorator() {
        final long decreaseAmount = 1000L;
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int earliestType = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        final TimeQualifier timeQualifier = new DecreasedEpochXmlElementValue(
                new TimeQualifierImpl(value, timeformat, earliestType, doc),
                decreaseAmount
        );
        final long expectedEpoch = 1730325600L - decreaseAmount;
        final Element expectedElement = doc.createElement("earliest");
        expectedElement.setAttribute("operation", "GE");
        expectedElement.setAttribute("value", Long.toString(expectedEpoch));
        Assertions.assertTrue(timeQualifier.isStartTime());
        Assertions.assertEquals(expectedElement.toString(), timeQualifier.xmlElement().toString());
        // epoch() value should remain unchanged
        Assertions.assertEquals(expectedEpoch + decreaseAmount, timeQualifier.epoch());
    }

    @Test
    public void testEquality() {
        final long decreaseAmount = 1000L;
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int earliestType = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        TimeQualifierImpl origin = new TimeQualifierImpl(value, timeformat, earliestType, doc);
        final TimeQualifier decreased1 = new DecreasedEpochXmlElementValue(origin, decreaseAmount);
        final TimeQualifier decreased2 = new DecreasedEpochXmlElementValue(origin, decreaseAmount);

        Assertions.assertEquals(decreased1, decreased2);
    }

    @Test
    public void testNonEquality() {
        final long decreaseAmount = 1000L;
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int earliestType = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        TimeQualifierImpl origin = new TimeQualifierImpl(value, timeformat, earliestType, doc);
        final TimeQualifier decreased1 = new DecreasedEpochXmlElementValue(origin, decreaseAmount);
        final TimeQualifier decreased2 = new DecreasedEpochXmlElementValue(origin, decreaseAmount - 1000);

        Assertions.assertNotEquals(decreased1, decreased2);
    }

    @Test
    public void testHashCode() {
        final long decreaseAmount = 1000L;
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int earliestType = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        TimeQualifierImpl origin = new TimeQualifierImpl(value, timeformat, earliestType, doc);
        final TimeQualifier decreased1 = new DecreasedEpochXmlElementValue(origin, decreaseAmount);
        final TimeQualifier decreased2 = new DecreasedEpochXmlElementValue(origin, decreaseAmount);
        final TimeQualifier decreasedNotEq = new DecreasedEpochXmlElementValue(origin, decreaseAmount - 1000);
        Assertions.assertEquals(decreased1.hashCode(), decreased2.hashCode());
        Assertions.assertNotEquals(decreased1.hashCode(), decreasedNotEq.hashCode());
    }

    @Test
    public void testContract() {
        EqualsVerifier
                .forClass(DecreasedEpochXmlElementValue.class)
                .withNonnullFields("origin")
                .withNonnullFields("decreaseAmount")
                .verify();
    }
}
