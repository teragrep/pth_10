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

import com.teragrep.pth10.ast.time.TimeQualifier;
import com.teragrep.pth_03.antlr.DPLLexer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;

public class TimeQualifierTest {

    @Test
    public void testEarliest() {
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int type = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testEarliestWithFractions() {
        final String value = "2024-01-01T10:00:00.001Z";
        final int type = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, "", type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1704103200L)));
        Element el = doc.createElement("earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1704103200L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testEarliestWithoutFractions() {
        final String value = "2024-01-01T10:00:00.000Z";
        final int type = DPLLexer.EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, "", type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1704103200L)));
        Element el = doc.createElement("earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1704103200L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testLatestWithFractions() {
        final String value = "2024-01-01T10:00:00.001Z";
        final int type = DPLLexer.LATEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, "", type, doc);
        Column expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1704103200L + 1L)));
        Element el = doc.createElement("latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1704103200L + 1L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testLatestWithoutFractions() {
        final String value = "2024-01-01T10:00:00.000Z";
        final int type = DPLLexer.LATEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, "", type, doc);
        Column expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1704103200L)));
        Element el = doc.createElement("latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1704103200L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testLatest() {
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int type = DPLLexer.LATEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testIndexEarliest() {
        final String value = "2024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.INDEX_EARLIEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("index_earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testIndexLatest() {
        final String value = "2024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.INDEX_LATEST;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("index_latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testStarttimeU() {
        final String value = "1730325600";
        final String timeformat = "";
        final int type = DPLLexer.STARTTIMEU;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testEndtimeU() {
        final String value = "1730325600";
        final String timeformat = "";
        final int type = DPLLexer.ENDTIMEU;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testInvalidToken() {
        final String value = "024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.IN;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        RuntimeException e = Assertions
                .assertThrows(RuntimeException.class, () -> new TimeQualifier(value, timeformat, type, doc).column());
        Assertions.assertEquals("TimeQualifier <" + type + "> not implemented yet.", e.getMessage());
        RuntimeException exml = Assertions
                .assertThrows(RuntimeException.class, () -> new TimeQualifier(value, timeformat, type, doc).xmlElement());
        Assertions.assertEquals("TimeQualifier <" + type + "> not implemented yet.", exml.getMessage());
    }

    @Test
    public void testEquals() {
        final String value = "2024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.ENDTIMEU;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        Assertions
                .assertEquals(new TimeQualifier(value, timeformat, type, doc), new TimeQualifier(value, timeformat, type, doc));

    }

    @Test
    public void testNotEquals() {
        final String value = "2024-10-31:00:00:00";
        final String value2 = "2024-10-30:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.ENDTIMEU;
        final Document doc = Assertions
                .assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        Assertions
                .assertNotEquals(new TimeQualifier(value, timeformat, type, doc), new TimeQualifier(value2, timeformat, type, doc));

    }
}
