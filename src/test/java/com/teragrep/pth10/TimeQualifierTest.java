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
    public void testEarliest(){
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int type = DPLLexer.EARLIEST;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testLatest(){
        final String value = "2024-31-10";
        final String timeformat = "%Y-%d-%m";
        final int type = DPLLexer.LATEST;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testIndexEarliest(){
        final String value = "2024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.INDEX_EARLIEST;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("index_earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testIndexLatest(){
        final String value = "2024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.INDEX_LATEST;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column  expected = new Column("`_time`").lt(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("index_latest");
        el.setAttribute("operation", "LE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testStarttimeU(){
        final String value = "1730325600";
        final String timeformat = "";
        final int type = DPLLexer.STARTTIMEU;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
        TimeQualifier tq = new TimeQualifier(value, timeformat, type, doc);
        Column expected = new Column("`_time`").geq(functions.from_unixtime(functions.lit(1730325600L)));
        Element el = doc.createElement("earliest");
        el.setAttribute("operation", "GE");
        el.setAttribute("value", Long.toString(1730325600L));

        Assertions.assertEquals(expected, tq.column());
        Assertions.assertEquals(el.toString(), tq.xmlElement().toString());
    }

    @Test
    public void testEndtimeU(){
        final String value = "1730325600";
        final String timeformat = "";
        final int type = DPLLexer.ENDTIMEU;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());
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
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> new TimeQualifier(value, timeformat, type, doc).column());
        Assertions.assertEquals("TimeQualifier <" + type + "> not implemented yet.", e.getMessage());
        RuntimeException exml = Assertions.assertThrows(RuntimeException.class, () -> new TimeQualifier(value, timeformat, type, doc).xmlElement());
        Assertions.assertEquals("TimeQualifier <" + type + "> not implemented yet.", exml.getMessage());
    }

    @Test
    public void testEquals(){
        final String value = "2024-10-31:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.ENDTIMEU;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        Assertions.assertEquals(new TimeQualifier(value, timeformat, type, doc), new TimeQualifier(value, timeformat, type, doc));

    }

    @Test
    public void testNotEquals(){
        final String value = "2024-10-31:00:00:00";
        final String value2 = "2024-10-30:00:00:00";
        final String timeformat = "%Y-%m-%d:HH:mm:ss";
        final int type = DPLLexer.ENDTIMEU;
        final Document doc = Assertions.assertDoesNotThrow(() -> DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument());

        Assertions.assertNotEquals(new TimeQualifier(value, timeformat, type, doc), new TimeQualifier(value2, timeformat, type, doc));

    }
}
