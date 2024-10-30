package com.teragrep.pth10.ast.time;

import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.Token;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public final class TimeQualifier {
    private final Token token;
    private final String value;
    private final String timeformat;
    private final Document doc;

    public TimeQualifier(final String value, final String timeformat, final Token type, final Document doc) {
        this.token = type;
        this.value = value;
        this.timeformat = timeformat;
        this.doc = doc;
    }

    public long epoch() {
        if (isUnixEpoch()) {
            try {
                return Long.parseLong(value);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid unix epoch: <[" + value + "]>", e);
            }
        }
        return new EpochTimestamp(value, timeformat).epoch();
    }

    public Column column() {
        Column col = new Column("`_time`");

        switch (token.getType()) {
            case DPLLexer.EARLIEST:
            case DPLLexer.INDEX_EARLIEST:
            case DPLLexer.STARTTIMEU: {
                col = col.geq(functions.from_unixtime(functions.lit(epoch())));
                break;
            }
            case DPLLexer.LATEST:
            case DPLLexer.INDEX_LATEST:
            case DPLLexer.ENDTIMEU:{
                col = col.lt(functions.from_unixtime(functions.lit(epoch())));
                break;
            }
            default: {
                throw new RuntimeException("TimeQualifier <" + token.getText() + "> not implemented yet.");
            }
        }

        return col;
    }

    public Element xmlElement() {
        // Handle date calculations
        Element el;
        switch (token.getType()) {
            case DPLLexer.EARLIEST:
            case DPLLexer.STARTTIMEU: {
                el = doc.createElement("earliest");
                el.setAttribute("operation", "GE");
                break;
            }
            case DPLLexer.INDEX_EARLIEST: {
                el = doc.createElement("index_earliest");
                el.setAttribute("operation", "GE");
                break;
            }
            case DPLLexer.LATEST:
            case DPLLexer.ENDTIMEU: {
                el = doc.createElement("latest");
                el.setAttribute("operation", "LE");
                break;
            }
            case DPLLexer.INDEX_LATEST: {
                el = doc.createElement("index_latest");
                el.setAttribute("operation", "LE");
                break;
            }
            default: {
                throw new RuntimeException("TimeQualifier <" + token.getText() + "> not implemented yet.");
            }
        }

        el.setAttribute("value", Long.toString(epoch()));
        return el;
    }

    public boolean isStartTime() {
        return token.getType() == DPLLexer.EARLIEST || token.getType() == DPLLexer.INDEX_EARLIEST || token.getType() == DPLLexer.STARTTIMEU;
    }

    public boolean isEndTime() {
        return token.getType() == DPLLexer.LATEST || token.getType() == DPLLexer.INDEX_LATEST || token.getType() == DPLLexer.ENDTIMEU;
    }

    public boolean isUnixEpoch() {
        return token.getType() == DPLLexer.STARTTIMEU || token.getType() == DPLLexer.ENDTIMEU;
    }
}
