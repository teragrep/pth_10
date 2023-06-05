/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

package com.teragrep.pth10.ast.commands.logicalstatement;

import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.EmitMode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.sql.Timestamp;
import java.util.Stack;

import static com.teragrep.pth10.ast.TimestampToEpochConversion.unixEpochFromString;
import static com.teragrep.pth10.ast.Util.relativeTimeModifier;

/**
 * <p>A subrule of logicalStatement, used for statements of time such as
 * earliest, latest, and et cetera.</p>
 */
public class TimeStatement extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeStatement.class);

    private Document doc;
    private final EmitMode.mode mode;
    // Time calculation specifics
    private Stack<String> timeFormatStack = new Stack<>();

    private Long startTime = null;
    private Long endTime = null;

    // return query start timestamp or null if not set
    public Long getStartTime() {
        return startTime;
    }

    // return query end timestamp or null if not set
    public Long getEndTime() {
        return endTime;
    }

    public TimeStatement(Document doc, Stack<String> timeFormatStack) {
        this.doc = doc;
        mode = EmitMode.mode.XML;
    }

    public TimeStatement(Stack<String> timeFormatStack) {
        this.doc = null;
        this.mode = EmitMode.mode.CATALYST;
    }

    /**
     * timeStatement : timeFormatQualifier? timeQualifier ;
     */
    @Override
    public Node visitTimeStatement(DPLParser.TimeStatementContext ctx) {
        if (ctx.getChildCount() == 2) {
            // Optional format part. Push it into the stack and use it in the next phase
            timeFormatStack.push(visit(ctx.getChild(0)).toString());
            return visit(ctx.getChild(1));
        } else {
            return visit(ctx.getChild(0));
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.
     * </p>
     * timeFormatQualifier : TIMEFORMAT EQ stringType //FIXME implement Time
     * Properties ;
     */
    @Override
    public Node visitTimeFormatQualifier(DPLParser.TimeFormatQualifierContext ctx) {
        LOGGER.info("visitTimeFormatQualifier incoming:"+ctx.getText());
        return new StringNode(new Token(Type.TIMEFORMAT_STATEMENT, ctx.getChild(1).getText()));
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.
     * </p>
     *
     * <pre>
     * timeQualifier
     * : EARLIEST EQ stringType
     * | INDEX_EARLIEST EQ stringType
     * | STARTTIME EQ stringType
     * | STARTDAYSAGO EQ integerType
     * | STARTMINUTESAGO EQ integerType
     * | STARTHOURSAGO EQ integerType
     * | STARTMONTHSAGO EQ integerType
     * | STARTTIMEU EQ integerType
     * | LATEST EQ stringType
     * | INDEX_LATEST EQ stringType
     * | ENDTIME EQ stringType
     * | ENDDAYSAGO EQ integerType
     * | ENDMINUTESAGO EQ integerType
     * | ENDHOURSAGO EQ integerType
     * | ENDMONTHSAGO EQ integerType
     * | ENDTIMEU EQ integerType
     * | SEARCHTIMESPANHOURS EQ integerType
     * | SEARCHTIMESPANMINUTES EQ integerType
     * | SEARCHTIMESPANDAYS EQ integerType
     * | SEARCHTIMESPANMONHTS EQ integerType
     * | DAYSAGO EQ integerType
     * | MINUTESAGO EQ integerType
     * | HOURSAGO EQ integerType
     * | MONTHSAGO EQ integerType
     * ;
     * </pre>
     */
    @Override
    public Node visitTimeQualifier(DPLParser.TimeQualifierContext ctx) {
        Node rv = null;
        //LOGGER.info("Visit timeQualifier mode:"+mode+" incoming:"+ctx.getText());
        switch(mode){
            case XML:{
                rv = timeQualifierEmitXml(ctx);
                break;
            }
            case CATALYST:{
                rv = timeQualifierEmitCatalyst(ctx);
                break;
            }
        }
        return rv;
    }

    /**
     * Returns an ElementNode with {@literal LE(<=) or GE(>=)} of unix time used
     * to restrict search results to certain timeframe. <br>
     * Supports <code>EARLIEST, INDEX_EARLIEST, LATEST, INDEX_LATEST</code>
     * @param ctx
     * @return ElementNode(XML) with LE/GE unixtime
     */
    private ElementNode timeQualifierEmitXml(DPLParser.TimeQualifierContext ctx) {
        String op = null;
        String timeFormatString = null;
        long timevalue = 0;
        String value = null;
        boolean isRelativeTime = false;
        Token comparisonToken;

        // If defined earlier, get it from stack
        if (!timeFormatStack.isEmpty()) {
            timeFormatString = timeFormatStack.pop();
        }

        // Get specifier. We know that 2 first childs are terminals
        // 'earliest = '
        TerminalNode node = (TerminalNode) ctx.getChild(0);
        value = ctx.getChild(1).getText();
        Timestamp now = new Timestamp(System.currentTimeMillis());
        // Is time given as absolute
        // Try to check if it is relative and  catch exception
        try {
            timevalue = relativeTimeModifier(now, value);
            isRelativeTime = true;
        } catch (NumberFormatException ne) {
        }
//		LOGGER.info("visitTimeQualifier value:"+value+" isRelative:"+isRelativeTime);

        // Handle date calculations
        switch (node.getSymbol().getType()) {
            case DPLLexer.EARLIEST: {
                op = "earliest";
                if (isRelativeTime) {
                    // Relational date like -12h from current timestamp
                    timevalue = relativeTimeModifier(now, value);
                } else {
                    // Absolute date given
                    timevalue = unixEpochFromString(value, timeFormatString);
                }
                comparisonToken = new Token(Type.GE);

                startTime = timevalue;
                break;
            }
            case DPLLexer.INDEX_EARLIEST: {
                op = "index_earliest";
                if (isRelativeTime) {
                    // Relational date like -12h from current timestamp
                    timevalue = relativeTimeModifier(now, value);
                } else {
                    // Absolute date given
                    timevalue = unixEpochFromString(value, timeFormatString);
                }
                comparisonToken = new Token(Type.GE);
                startTime = timevalue;
                break;
            }
            case DPLLexer.LATEST: {
                op = "latest";
                if (isRelativeTime) {
                    // Relational date like -12h from current timestamp
                    timevalue = relativeTimeModifier(now, value);
                } else {
                    // Absolute time
                    timevalue = unixEpochFromString(value, timeFormatString);
                }
                comparisonToken = new Token(Type.LE);
                endTime = timevalue;
                break;
            }
            case DPLLexer.INDEX_LATEST: {
                op = "index_latest";
                if (isRelativeTime) {
                    // Relational date like -12h from current timestamp
                    timevalue = relativeTimeModifier(now, value);
                } else {
                    // Absolute time
                    timevalue = unixEpochFromString(value, timeFormatString);
                }
                comparisonToken = new Token(Type.LE);
                endTime = timevalue;
                break;
            }
            default: {
                throw new RuntimeException("TimeQualifier <" + node.getSymbol().getText() + "> not implemented yet.");
            }
        }

        // Instant instant = Instant.ofEpochSecond( timevalue );
        // Use only date part and forget actual time
        //LocalDate localDate = LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
        // Use only date, not excact time
        Element el = doc.createElement(op);
        el.setAttribute("operation", comparisonToken.toString());
        el.setAttribute("value", Long.toString(timevalue));
        return new ElementNode(el);
    }

    /**
     * Returns a ColumnNode containing a column with {@literal LEQ(<=) or GEQ(>=)} of unix time used
     * to restrict search results to certain timeframe. <br>
     * Supports <code>EARLIEST, INDEX_EARLIEST, LATEST, INDEX_LATEST</code>
     * @param ctx
     * @return ColumnNode with leq/geq unixtime
     */
    private ColumnNode timeQualifierEmitCatalyst(DPLParser.TimeQualifierContext ctx) {
        String op = null;
        Column rv = null;
        String timeFormatString = null;
        long timevalue = 0;
        String value = null;
        boolean isRelativeTime = false;
        Token comparisonToken;

        // If defined earlier, get it from stack
        if (!timeFormatStack.isEmpty()) {
            timeFormatString = timeFormatStack.pop();
        }

        // Get specifier. We know that 2 first childs are terminals
        // 'earliest = '
        TerminalNode node = (TerminalNode) ctx.getChild(0);
        value = ctx.getChild(1).getText();
        Timestamp now = new Timestamp(System.currentTimeMillis());
        // Is time given as absolute
        // Try to check if it is relative and  catch exception
        try {
            timevalue = relativeTimeModifier(now, value);
            isRelativeTime = true;
        } catch (NumberFormatException ne) {
        }

        Column col = new Column("`_time`");
        // Handle date calculations
        switch (node.getSymbol().getType()) {
            case DPLLexer.EARLIEST:
            case DPLLexer.INDEX_EARLIEST: {
                if (isRelativeTime) {
                    // Relational date like -12h from current timestamp
                    timevalue = relativeTimeModifier(now, value);
                } else {
                    // Absolute date given
                    timevalue = unixEpochFromString(value, timeFormatString);
                }
                startTime = timevalue;

                //java.sql.Timestamp tt = new Timestamp(timevalue*1000);
                //rv = col.geq(tt);
                rv = col.geq(functions.from_unixtime(functions.lit(timevalue)));
                break;
            }
            case DPLLexer.LATEST:
            case DPLLexer.INDEX_LATEST: {
                if (isRelativeTime) {
                    // Relational date like -12h from current timestamp
                    timevalue = relativeTimeModifier(now, value);
                } else {
                    // Absolute time
                    timevalue = unixEpochFromString(value, timeFormatString);
                }
                endTime = timevalue;
                rv = col.leq(functions.from_unixtime(functions.lit(timevalue)));
                break;
            }
            default: {
                throw new RuntimeException("TimeQualifier <" + node.getSymbol().getText() + "> not implemented yet.");
            }
        }

        // Instant instant = Instant.ofEpochSecond( timevalue );
        // Use only date part and forget actual time
        //LocalDate localDate = LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
        //LOGGER.info("visitTimeQualifier(catalyst) return value:" + rv+ " SQL:"+rv.expr().sql() +" asString:"+rv.toString());

        return new ColumnNode(rv);
    }



}
