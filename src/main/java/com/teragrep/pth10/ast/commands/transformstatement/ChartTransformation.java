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

package com.teragrep.pth10.ast.commands.transformstatement;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.steps.chart.ChartStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.RuleContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base visitor class for the chart command
 */
public class ChartTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChartTransformation.class);
    private final List<String> traceBuffer;
    DPLParserCatalystContext catCtx;

    ProcessingStack processingPipe;

    EvalTransformation evalTransformation;
    AggregateFunction aggregateFunction;
    private boolean aggregatesUsed = false;
    private String aggregateField = null;
    public ChartStep chartStep = null;

    /**
     * Initialize the class to use in TransformStatement
     * @param catCtx catalyst context
     * @param processingPipe processing stack
     * @param buf tracebuffer, for debugging
     */
    public ChartTransformation(DPLParserCatalystContext catCtx, ProcessingStack processingPipe, List<String> buf) {
        this.traceBuffer = buf;
        this.catCtx = catCtx;
        this.processingPipe = processingPipe;
        this.evalTransformation = new EvalTransformation(catCtx, processingPipe, buf);
        this.aggregateFunction = new AggregateFunction(processingPipe, buf);
        traceBuffer.add("Init chartTransformation stack:" + processingPipe);
    }
    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }
    
    public void setAggregatesUsed(boolean newValue) {
    	this.aggregatesUsed = newValue;
    }

    public String getAggregateField() {return this.aggregateField;}

    /**
     * chartTransformation : CHART
     * (sepChartParameter|formatParameter|contParameter|limitParameter|aggParameter)*
     * (aggregationInstruction|sparklineAggregationInstruction|PARENTHESIS_L *
     * evalStatement PARENTHESIS_R)+
     * ((overInstruction(divideByInstruction)?)|(divideByInstruction))? ;
     */

    public Node visitChartTransformation(DPLParser.ChartTransformationContext ctx) {
        Node rv;

        rv = visitChartTransformationEmitCatalyst(ctx);
        traceBuffer.add("visitChartTransformation returns");

        return rv;
    }

    /**
     * Goes through all the chart command's parameters and performs the aggregation via the stack
     * @param ctx
     * @return
     */
    private Node visitChartTransformationEmitCatalyst(DPLParser.ChartTransformationContext ctx) {
    	Node rv;
    	String divByInst;

        Dataset<Row> ds = null;
        if (!this.processingPipe.isEmpty()) {
            ds = processingPipe.pop();
        }
        this.chartStep = new ChartStep(ds);
    	
    	LOGGER.info("::::: ChartTransformation incoming: " + ctx.getText());

    	
    	// aggregate function and its field renaming instruction
    	for (DPLParser.T_chart_aggregationInstructionContext c : ctx.t_chart_aggregationInstruction()) {    		
    		// Visit aggregation function
    		Node aggFunction = visit(c.getChild(0));
    		Column aggCol = ((ColumnNode) aggFunction).getColumn();
    		
    		// Get field rename instruction if exists, and apply the rename to aggCol
    		if (c.t_chart_fieldRenameInstruction() != null) {
    			String fieldName = visit(c.t_chart_fieldRenameInstruction()).toString();
    			aggCol = aggCol.as(fieldName);
    		}
    		
    		// add to list of expressions
    		chartStep.addAggregateExpressionOperation(aggCol);
    	}
    	
    	// groupBy given column
    	if (ctx.t_chart_by_column_rowOptions() != null && !ctx.t_chart_by_column_rowOptions().isEmpty()) {
            final List<String> listOfGroupBys = new ArrayList<>();
            ctx.t_chart_by_column_rowOptions().forEach(opt -> {
                if (opt.t_column_Parameter() != null && opt.t_column_Parameter().fieldType() != null) {
                    listOfGroupBys.add(opt.t_column_Parameter().fieldType().getText());
                }

                if (opt.t_row_Parameter() != null && !opt.t_row_Parameter().fieldType().isEmpty()) {
                    listOfGroupBys.addAll(opt.t_row_Parameter().fieldType().stream().map(RuleContext::getText).collect(Collectors.toList()));
                }
            });

            listOfGroupBys.forEach(gb -> this.chartStep.addGroupByOperation(functions.col(gb)));
    	}

    	this.aggregatesUsed = true;
    	

    	ds = chartStep.get();
    	
    	// push to stack, where the actual aggregation will occur
    	processingPipe.push(ds, processingPipe.getStackMode(),
                this.aggregatesUsed);

    	rv = new CatalystNode(ds);
    	return rv;
    }

    @Override
    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        Node rv = aggregateFunction.visitAggregateFunction(ctx);
        if(aggregateField == null)
            aggregateField = aggregateFunction.getAggregateField();
        traceBuffer.add("---AggregateFunc="+ctx.getText()+ " AggregateField="+aggregateField+"\n");
        return rv;
    }

    @Override public Node visitT_row_Parameter(DPLParser.T_row_ParameterContext ctx) {
        String target = ctx.getText();
        traceBuffer.add("-- visitT_row_parameter:" + target);

        return new StringNode(new Token(Type.STRING, target));
    }

    @Override public Node visitT_column_Parameter(DPLParser.T_column_ParameterContext ctx) {
        String target = ctx.getText();
        traceBuffer.add("-- visitT_column_parameter:" + target);
        return new StringNode(new Token(Type.STRING, target));
    }

    public Node visitT_chart_by_column_rowOptions(List<DPLParser.T_chart_by_column_rowOptionsContext> ctxList) {
        String target = ctxList.toString();

        List<String> divInsts = new ArrayList<>();
        ctxList.forEach(c -> {
            // grammar: t_row_Parameter? t_column_Parameter?
            String f= null;
            Node rn = null;
            // Check row-parameter
            if(c.t_row_Parameter() != null) {
                rn = visit(c.t_row_Parameter());
                // Node n = visitT_chart_divideByInstruction(c);
                f = rn.toString();
                divInsts.add(f);
            }
            // Check also optional column-parameter
            if(c.t_column_Parameter() != null) {
                rn = visit(c.t_column_Parameter());
                if (rn != null) {
                    f = rn.toString();
                    divInsts.add(f);
                }
            }
            traceBuffer.add("field-list:" + f);
        });


        if (divInsts.size() != 0) {

            String divCmd = String.join(",", divInsts);
            traceBuffer.add("--visitT_chart_divideByInstruction fields=" + divCmd);
            return new StringNode(new Token(Type.STRING, divCmd));

        } else {
            traceBuffer.add("--visitT_chart_divideByInstruction list=:" + ctxList);
        }
        return null;
    }



    @Override
    public Node visitT_chart_fieldRenameInstruction(DPLParser.T_chart_fieldRenameInstructionContext ctx) {
        String field = ctx.getChild(1).getText();
        traceBuffer.add("fieldRename:" + field);
        return new StringNode(new Token(Type.STRING, field));

    }
}
