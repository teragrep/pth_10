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
package com.teragrep.pth10.ast.commands.transformstatement;

import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.ast.commands.transformstatement.timechart.DivByInstContextValue;
import com.teragrep.pth10.ast.commands.transformstatement.timechart.SpanContextValue;
import com.teragrep.pth10.steps.timechart.TimechartStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class that contains the visitor methods for the <code>timechart</code> command<br>
 */
public final class TimechartTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimechartTransformation.class);
    private final DPLParserCatalystContext catCtx;

    private final AggregateFunction aggregateFunction;

    // fields set in visit functions
    private final ArrayList<Column> aggFunCols = new ArrayList<>();

    public TimechartTransformation(final DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
        this.aggregateFunction = new AggregateFunction(catCtx);
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
        final Column span = new SpanContextValue(ctx.t_timechart_binOptParameter(), catCtx).value();
        final List<String> divByInsts = new DivByInstContextValue(ctx.t_timechart_divideByInstruction()).value();

        visitChildren(ctx); // visit all the parameters

        TimechartStep timechartStep = new TimechartStep(aggFunCols, divByInsts, span);

        LOGGER.debug("span= <[{}]>", span);
        LOGGER.debug("aggcols= <[{}]>", aggFunCols);
        LOGGER.debug("divby= <[{}]>", divByInsts);

        return new StepNode(timechartStep);
    }

    @Override
    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        ColumnNode aggCol = (ColumnNode) aggregateFunction.visitAggregateFunction(ctx);
        aggFunCols.add(aggCol.getColumn());
        return new NullNode();
    }

    @Override
    public Node visitT_timechart_fieldRenameInstruction(DPLParser.T_timechart_fieldRenameInstructionContext ctx) {
        String rename = ctx.getChild(1).getText();
        if (!aggFunCols.isEmpty()) {
            Column latestAgg = aggFunCols.remove(aggFunCols.size() - 1);
            aggFunCols.add(latestAgg.as(rename)); // rename the newest visited aggregation column
        }

        return new NullNode();
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

    @Override
    public Node visitSpanType(DPLParser.SpanTypeContext ctx) {
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
