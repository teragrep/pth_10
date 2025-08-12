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
package com.teragrep.pth10.ast.time;

import com.teragrep.pth_03.antlr.DPLLexer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Objects;

public final class TimeQualifier {

    private final int token;
    private final String value;
    private final String timeformat;
    private final Document doc;

    public TimeQualifier(final String value, final String timeformat, final int type, final Document doc) {
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

        if (isEndTime()) {
            return new RoundedUpTimestamp(new DPLTimestampImpl(value, timeformat)).zonedDateTime().toEpochSecond();
        }
        return new DPLTimestampImpl(value, timeformat).zonedDateTime().toEpochSecond();
    }

    public Column column() {
        Column col = new Column("`_time`");

        switch (token) {
            case DPLLexer.EARLIEST:
            case DPLLexer.INDEX_EARLIEST:
            case DPLLexer.STARTTIMEU: {
                col = col.geq(functions.from_unixtime(functions.lit(epoch())));
                break;
            }
            case DPLLexer.LATEST:
            case DPLLexer.INDEX_LATEST:
            case DPLLexer.ENDTIMEU: {
                col = col.lt(functions.from_unixtime(functions.lit(epoch())));
                break;
            }
            default: {
                throw new RuntimeException("TimeQualifier <" + token + "> not implemented yet.");
            }
        }

        return col;
    }

    public Element xmlElement() {
        // Handle date calculations
        Element el;
        switch (token) {
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
                throw new RuntimeException("TimeQualifier <" + token + "> not implemented yet.");
            }
        }

        el.setAttribute("value", Long.toString(epoch()));
        return el;
    }

    public boolean isStartTime() {
        return token == DPLLexer.EARLIEST || token == DPLLexer.INDEX_EARLIEST || token == DPLLexer.STARTTIMEU;
    }

    public boolean isEndTime() {
        return token == DPLLexer.LATEST || token == DPLLexer.INDEX_LATEST || token == DPLLexer.ENDTIMEU;
    }

    public boolean isUnixEpoch() {
        return token == DPLLexer.STARTTIMEU || token == DPLLexer.ENDTIMEU;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TimeQualifier that = (TimeQualifier) o;
        return Objects.equals(token, that.token) && Objects.equals(value, that.value)
                && Objects.equals(timeformat, that.timeformat) && Objects.equals(doc, that.doc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, value, timeformat, doc);
    }
}
