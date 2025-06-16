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
package com.teragrep.pth10.ast.commands.transformstatement;

import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.steps.timechart.TimechartStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that contains the visitor methods for the <code>timechart</code> command<br>
 * Provides a pivoted dataset, making it easier to form time-field graphs in the UI
 * <pre>Dataset.groupBy("_time").pivot(aggregateField).sum(fieldname)</pre>
 */
public class TimechartTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimechartTransformation.class);
    private DPLParserCatalystContext catCtx = null;
    private DPLParserCatalystVisitor catVisitor;
    private Document doc;

    EvalTransformation evalTransformation;
    AggregateFunction aggregateFunction;
    private String aggregateField = null;

    public TimechartStep timechartStep = null;

    public TimechartTransformation(DPLParserCatalystContext catCtx, DPLParserCatalystVisitor catVisitor) {
        this.doc = null;
        this.catCtx = catCtx;
        this.catVisitor = catVisitor;
        this.evalTransformation = new EvalTransformation(catCtx);
        this.aggregateFunction = new AggregateFunction(catCtx);
    }

    public String getAggregateField() {
        return this.aggregateField;
    }

    /**
     * timechartTransformation : COMMAND_MODE_TIMECHART (t_timechart_sepParameter)? (t_timechart_formatParameter)?
     * (t_timechart_fixedrangeParameter)? (t_timechart_partialParameter)? (t_timechart_contParameter)?
     * (t_timechart_limitParameter)? (t_timechart_binOptParameter)*
     * (t_timechart_singleAggregation|t_timechart_intervalInstruction|EVAL_LANGUAGE_MODE_PARENTHESIS_L evalStatement
     * EVAL_LANGUAGE_MODE_PARENTHESIS_R)+ (t_timechart_divideByInstruction) ;
     */
    @Override
    public Node visitTimechartTransformation(DPLParser.TimechartTransformationContext ctx) {
        return timechartTransformationEmitCatalyst(ctx);
    }

    private Node timechartTransformationEmitCatalyst(DPLParser.TimechartTransformationContext ctx) {
        this.timechartStep = new TimechartStep();

        Column span = null;

        if (ctx.t_timechart_binOptParameter() != null && !ctx.t_timechart_binOptParameter().isEmpty()) {
            LOGGER.info("Timechart Optional parameters: <[{}]>", ctx.t_timechart_binOptParameter().get(0).getText());

            ColumnNode spanNode = (ColumnNode) visit(ctx.t_timechart_binOptParameter().get(0));
            if (spanNode != null) {
                span = spanNode.getColumn();
            }
        }

        Column funCol = null;
        List<Column> listOfAggFunCols = new ArrayList<>();
        List<String> listOfDivideByInst = new ArrayList<>();
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);

            if (child instanceof DPLParser.AggregateFunctionContext) {
                // go through each agg. function
                DPLParser.AggregateFunctionContext aggFunCtx = (DPLParser.AggregateFunctionContext) child;
                Node funNode = visit(aggFunCtx);
                if (funNode != null) {
                    if (funCol != null) {
                        listOfAggFunCols.add(funCol);
                    }
                    funCol = ((ColumnNode) funNode).getColumn();
                }
            }
            else if (child instanceof DPLParser.T_timechart_divideByInstructionContext) {
                String divByInst = ((StringNode) visitT_timechart_divideByInstruction(
                        (DPLParser.T_timechart_divideByInstructionContext) child
                )).toString();
                listOfDivideByInst.add(divByInst);
            }
            else if (child instanceof DPLParser.T_timechart_fieldRenameInstructionContext) {
                if (funCol != null) {
                    funCol = funCol.as(visit(child).toString());
                }
            }
        }
        listOfAggFunCols.add(funCol); // need to add last one; for loop above only adds if there's a new one coming

        if (span == null) {
            span = createDefaultSpan();
        }

        timechartStep.setHdfsPath(this.catVisitor.getHdfsPath());
        timechartStep.setCatCtx(catCtx);
        timechartStep.setSpan(span);
        timechartStep.setAggCols(listOfAggFunCols);
        timechartStep.setDivByInsts(listOfDivideByInst);

        // span
        this.catCtx.setTimeChartSpanSeconds(getSpanSeconds(span));

        LOGGER.debug("span= <[{}]>", timechartStep.getSpan().toString());
        LOGGER.debug("aggcols= <[{}]>", Arrays.toString(timechartStep.getAggCols().toArray()));
        LOGGER.debug("divby= <[{}]>", Arrays.toString(timechartStep.getDivByInsts().toArray()));

        return new StepNode(timechartStep);

        //throw new RuntimeException("Chart transformation operation not supported yet");
    }

    /**
     * Convert span of type Column to the span length in seconds
     * 
     * @param span span of type column
     * @return span length in seconds
     */
    private long getSpanSeconds(Column span) {
        // span column is of type 'timewindow(_time, 60000000, 60000000, 0)'
        // get second parameter and convert from microseconds to seconds
        // yes, this is terrible but it works so is it really?
        char[] spanChars = span.toString().toCharArray();
        boolean isWithinNumber = false;
        StringBuilder num = new StringBuilder();
        for (char spanChar : spanChars) {
            if (spanChar == ',') {
                isWithinNumber = !isWithinNumber;
                if (!isWithinNumber) {
                    break;
                }
            }
            else if (isWithinNumber && spanChar != ' ') {
                num.append(spanChar);
            }
        }

        try {
            return Long.parseLong(num.toString()) / 1_000_000L;
        }
        catch (NumberFormatException nfe) {
            throw new RuntimeException("Error converting span column into seconds");
        }
    }

    @Override
    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        Node rv = aggregateFunction.visitAggregateFunction(ctx);
        if (aggregateField == null)
            aggregateField = aggregateFunction.getAggregateField();
        return aggregateFunction.visitAggregateFunction(ctx);
    }

    @Override
    public Node visitT_timechart_divideByInstruction(DPLParser.T_timechart_divideByInstructionContext ctx) {
        //        LOGGER.info(ctx.getChildCount()+"--visitT_chart_divideByInstruction incoming{}", ctx.getText());
        if (ctx.getChildCount() == 0) {
            return null;
        }
        String target = ctx.getChild(1).getChild(0).toString();

        if (doc != null) {
            Element el = doc.createElement("divideBy");
            el.setAttribute("field", target);
            return new ElementNode(el);
        }
        else {
            return new StringNode(new Token(Type.STRING, target));
        }
    }

    @Override
    public Node visitT_timechart_fieldRenameInstruction(DPLParser.T_timechart_fieldRenameInstructionContext ctx) {
        String field = ctx.getChild(1).getText();
        if (doc != null) {
            Element el = doc.createElement("fieldRename");
            el.setAttribute("field", field);
            return new ElementNode(el);
        }
        else {
            return new StringNode(new Token(Type.STRING, field));
        }
    }

    @Override
    public Node visitT_timechart_binOptParameter(DPLParser.T_timechart_binOptParameterContext ctx) {
        LOGGER.info("visitT_timechart_binOptParameter:<{}>", ctx.getText());
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_binSpanParameter(DPLParser.T_timechart_binSpanParameterContext ctx) {
        LOGGER.info("visitT_timechart_binSpanParameter:<{}>", ctx.getText());
        CalendarInterval ival = getSpanLength(ctx.getChild(1).getText());
        Column col = new Column("_time");
        Column span = functions.window(col, String.valueOf(ival));

        return new ColumnNode(span);
    }

    /**
     * Creates a column with default span of one hour
     * 
     * @return
     */
    private Column createDefaultSpan() {
        long sec = 0;
        TimeRange tr = TimeRange.ONE_HOUR;
        String duration = "1 days"; // Default duration
        //        LOGGER.info("createDefaultSpan="+catCtx.getTimeRange());
        DPLParserConfig pConf = catCtx.getParserConfig();
        if (pConf != null) {
            tr = pConf.getTimeRange();
        }
        switch (tr) {
            case TEN_SECONDS: {
                sec = 10;
                duration = "10 seconds";
                break;
            }
            case ONE_MINUTE: {
                sec = 60;
                duration = "1 minutes";
                break;
            }
            case FIVE_MINUTES: {
                sec = 5 * 60;
                duration = "5 minutes";
                break;
            }
            case THIRTY_MINUTES: {
                sec = 30 * 60;
                duration = "30 minutes";
                break;
            }
            case ONE_HOUR: {
                sec = 3600;
                duration = "1 hours";
                break;
            }
            case ONE_DAY: {
                sec = 24 * 3600;
                duration = "1 days";
                break;
            }
            case ONE_MONTH: {
                sec = 30 * 24 * 3600;
                duration = "30 days";
                break;
            }
            default: {
                throw new RuntimeException("timechart span duration greater that month is not supported");
            }
        }
        CalendarInterval ival = new CalendarInterval(0, 0, sec * 1000 * 1000);
        return functions.window(new Column("_time"), String.valueOf(ival), duration, "0 minutes");
    }

    /**
     * Gets the CalendarInterval of string form span
     * 
     * @param value span as string
     * @return CalendarInterval
     */
    private CalendarInterval getSpanLength(String value) {
        // incoming span-length consist of
        // <int>[<timescale>]
        // default timescale is sec
        String timescale = "sec";
        int numericalValue;
        int month = 0;
        long sec = 0;
        Pattern p = Pattern.compile("\\d+");
        Matcher m = p.matcher(value);
        if (m.lookingAt()) {
            numericalValue = Integer.parseInt(m.group());
            String[] parts = value.split(m.group());
            if (parts.length > 1)
                timescale = parts[1].trim();
        }
        else {
            LOGGER.error("Span length error: missing numerical value:<{}>", value);
            throw new RuntimeException("getSpanLength, missing numerical value:" + value);
        }
        // Calculate value
        switch (timescale) {
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
            case "S": {
                sec = numericalValue;
                break;
            }
            case "m":
            case "min":
            case "mins":
            case "minute":
            case "minutes":
            case "M": {
                sec = numericalValue * 60L;
                break;
            }
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
            case "H": {
                sec = numericalValue * 3600L;
                break;
            }
            case "d":
            case "day":
            case "days":
            case "D": {
                sec = numericalValue * 3600L * 24;
                break;
            }
            case "w":
            case "week":
            case "weeks":
            case "W": {
                sec = numericalValue * 3600L * 24 * 7;
                break;
            }
            case "mon":
            case "month":
            case "months":
            case "MON": {
                //month = numericalValue;
                // month is not  supported as such, it needs to be changed seconds
                // use 30 as default month length
                sec = (long) numericalValue * 30 * 24 * 3600;
                break;
            }
        }
        return new CalendarInterval(month, 0, sec * 1000 * 1000L);
    }

    @Override
    public Node visitT_timechart_binsParameter(DPLParser.T_timechart_binsParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_binStartEndParameter(DPLParser.T_timechart_binStartEndParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_contParameter(DPLParser.T_timechart_contParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_fixedrangeParameter(DPLParser.T_timechart_fixedrangeParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_formatParameter(DPLParser.T_timechart_formatParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_limitParameter(DPLParser.T_timechart_limitParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_partialParameter(DPLParser.T_timechart_partialParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_sepParameter(DPLParser.T_timechart_sepParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_binMinspanParameter(DPLParser.T_timechart_binMinspanParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_binAligntimeParameter(DPLParser.T_timechart_binAligntimeParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_whereInstruction(DPLParser.T_timechart_whereInstructionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_nullstrParameter(DPLParser.T_timechart_nullstrParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_otherstrParameter(DPLParser.T_timechart_otherstrParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_usenullParameter(DPLParser.T_timechart_usenullParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_useotherParameter(DPLParser.T_timechart_useotherParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_evaledField(DPLParser.T_timechart_evaledFieldContext ctx) {
        return visitChildren(ctx);
    }

    /*@Override public Node visitT_timechart_singleAggregation(DPLParser.T_timechart_singleAggregationContext ctx) {
        String oper = ctx.getText();
        String defaultField="*";
        Node rv = null;
        Column col = null;
        if(oper.equalsIgnoreCase("count") || oper.equalsIgnoreCase("c")) {
            aggregateField = "count"; // use default name
            col = org.apache.spark.sql.functions.count(defaultField);
    //            LOGGER.info("T_timechart_singleAggregation (Catalyst):{}", col.expr().sql()+" default field="+defaultField);
            traceBuffer.add("Visit AggregateMethodCount(Catalyst):{}", col.expr().sql());
            rv = new ColumnNode(col);
        }else {
            rv = this.aggregateFunction.visitAggregateFunction(ctx.aggregateFunction());
            this.aggregateField = aggregateFunction.getAggregateField();
        }
        // Check whether field needs to be renamed
        if(ctx.t_timechart_fieldRenameInstruction() != null){
            Node renameCmd = visitT_timechart_fieldRenameInstruction(ctx.t_timechart_fieldRenameInstruction());
            aggregateField = renameCmd.toString();
    //            rv = new ColumnNode(((ColumnNode) rv).getColumn().as(renameCmd.toString()));
        }
        return rv;
    }*/

    @Override
    public Node visitSpanType(DPLParser.SpanTypeContext ctx) {
        //        LOGGER.info("visitSpanType:"+ctx.getText());
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_aggParameter(DPLParser.T_timechart_aggParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_dedupSplitParameter(DPLParser.T_timechart_dedupSplitParameterContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Node visitT_timechart_tcOpt(DPLParser.T_timechart_tcOptContext ctx) {
        return visitChildren(ctx);
    }
}
