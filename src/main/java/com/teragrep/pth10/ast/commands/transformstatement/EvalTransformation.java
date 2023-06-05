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
import com.teragrep.pth10.ast.bo.ElementNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.commands.evalstatement.EvalStatement;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.*;


/**
 * Base transformation for evaluation commands, actual implementations of
 * the commands can be found in {@link com.teragrep.pth10.ast.commands.evalstatement.EvalStatement EvalStatement}
 */
public class EvalTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EvalTransformation.class);

    private boolean debugEnabled = true;

    private List<String> traceBuffer = null;
    private Document doc = null;
    private Dataset<Row> ds = null;
    private ProcessingStack processingPipe = null;
    private DPLParserCatalystContext catCtx = null;
    private List<String> dplHosts = new ArrayList<>();
    private List<String> dplSourceTypes = new ArrayList<>();
    private boolean aggregatesUsed = false;
    private String aggregateField = null;

    private Stack<String> timeFormatStack = new Stack<>();

    // Symbol table for mapping DPL default column names
    private Map<String, Object> symbolTable = new HashMap<>();

    // Eval-statement
    public EvalStatement evalStatement = null;


    public EvalTransformation(DPLParserCatalystContext catCtx, ProcessingStack processingPipe, List<String> buf) {
        this.doc = null;
        this.ds = null;
        this.catCtx = null;
        this.processingPipe = processingPipe;
        this.traceBuffer = buf;
        // Eval returns Columns so no need to pass Dataset<Row>
        evalStatement = new EvalStatement(catCtx, processingPipe, buf);
    }

    // Parameters for Groupmapper queries

    // List of found hosts
    public List<String> getHosts() {
        return this.dplHosts;
    }

    // List of found sourcetypes
    public List<String> getSourcetypes() {
        return this.dplSourceTypes;
    }

    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    public String getAggregateField() {
        return this.aggregateField;
    }

    public Node visitEvalTransformation(DPLParser.EvalTransformationContext ctx) {
        LOGGER.info("VisitEvalTransformations incoming:"+ctx.getText());
        Node rv = evalTransformationEmitCatalyst(ctx);
        traceBuffer.add("visitEvalTransformation returns:" + rv.toString());

        return rv;
    }

    private Node evalTransformationEmitCatalyst(DPLParser.EvalTransformationContext ctx) {
        if(debugEnabled)
            LOGGER.info(ctx.getChildCount() + " visitEvalTransformation(Catalyst) " + ctx.getText() + " type="
                    + ctx.getChild(0).getClass().getName()+" left oper="+ctx.getChild(0).getText() );
        Node rv = null;
        // Left=eval, actual operation is on right
        Node left = visit(ctx.getChild(0));
        //if(LOGGER.isDebugEnabled())
        traceBuffer.add(" visitEvalStatement" + ctx.getChildCount() + " incoming logical expression:" + ctx.getText());
        if (ctx.getChildCount() == 1) {
            // leaf
            //traceBuffer.add(" visitEvalStatement Catalyst leaf:" + left + " type:" + left.getClass().getName());
            rv = left;
        } else if (ctx.getChildCount() == 2) {
            Node right = visit(ctx.getChild(1));
            traceBuffer.add(" visitEvalStatement Catalyst leaf:" + right + " type:" + right.getClass().getName());
            rv = right;

        } else if (ctx.getChildCount() == 3) {
            // logical operation xxx AND/OR/XOR xxx
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Node right = visit(ctx.getChild(2));
            Element el = doc.createElement(operation.getText().toUpperCase());
            el.appendChild(((ElementNode) left).getElement());
            el.appendChild(((ElementNode) right).getElement());
            ElementNode eln = new ElementNode(el);
            traceBuffer.add("visitEvalStatement with operation:" + eln.toString());
            rv = eln;
        }
        return rv;
    }

    public Node visitEvalFunctionStatement(DPLParser.EvalFunctionStatementContext ctx) {
        LOGGER.info("visitEvalFunctionStatement incoming:"+ctx.getText() );
        return  evalStatement.visitEvalFunctionStatement(ctx);
    }

    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        return evalStatement.visitL_evalStatement_evalCompareStatement(ctx);
    }

    public Node visitL_evalStatement_evalLogicStatement(DPLParser.L_evalStatement_evalLogicStatementContext ctx) {
        return evalStatement.visitL_evalStatement_evalLogicStatement(ctx);
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        return evalStatement.visitFieldType(ctx);
    }

    public Node visitL_evalStatement_subEvalStatement(DPLParser.L_evalStatement_subEvalStatementContext ctx) {
        return evalStatement.visitL_evalStatement_subEvalStatement(ctx);
    }

    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        return evalStatement.visitAggregateFunction(ctx);
    }

    @Override
    public Node visitAggregateMethodCount(DPLParser.AggregateMethodCountContext ctx) {
        return evalStatement.visitAggregateMethodCount(ctx);
    }

    @Override
    public Node visitT_eval_evalParameter(DPLParser.T_eval_evalParameterContext ctx) {
        return evalStatement.visitT_eval_evalParameter(ctx);
    }

    public Node visitEvalMethodIf(DPLParser.EvalMethodIfContext ctx) {
        return evalStatement.visitEvalMethodIf(ctx);
    }

    public Node visitEvalMethodSubstr(DPLParser.EvalMethodSubstrContext ctx) {
        return evalStatement.visitEvalMethodSubstr(ctx);
    }

    public Node visitEvalMethodTrue(DPLParser.EvalMethodTrueContext ctx) {
        return evalStatement.visitEvalMethodTrue(ctx);
    }

    public Node visitEvalMethodFalse(DPLParser.EvalMethodFalseContext ctx) {
        return evalStatement.visitEvalMethodFalse(ctx);
    }

    public Node visitEvalMethodNull(DPLParser.EvalMethodNullContext ctx) {
        return evalStatement.visitEvalMethodNull(ctx);
    }

    public Node visitEvalMethodNow(DPLParser.EvalMethodNowContext ctx) {
        return evalStatement.visitEvalMethodNow(ctx);
    }

    public Node visitEvalMethodLen(DPLParser.EvalMethodLenContext ctx) {
        LOGGER.info("-visitEvalMethodLen");
        return evalStatement.visitEvalMethodLen(ctx);
    }

    public Node visitEvalMethodSplit(DPLParser.EvalMethodSplitContext ctx) {
        return evalStatement.visitEvalMethodSplit(ctx);
    }

    public Node visitEvalMethodStrftime(DPLParser.EvalMethodStrftimeContext ctx) {
        return evalStatement.visitEvalMethodStrftime(ctx);
    }

    public Node visitEvalMethodStrptime(DPLParser.EvalMethodStrptimeContext ctx) {
        return evalStatement.visitEvalMethodStrptime(ctx);
    }

    public Node visitEvalFieldType(DPLParser.EvalFieldTypeContext ctx) {
        return evalStatement.visitEvalFieldType(ctx);
    }

    public Node visitEval_integerType(DPLParser.EvalIntegerTypeContext ctx) {
        return evalStatement.visitEvalIntegerType(ctx);
    }

    public Node visitEvalStringType(DPLParser.EvalStringTypeContext ctx) {
        return evalStatement.visitEvalStringType(ctx);
    }

    public Node visitL_evalStatement_evalCalculateStatement_multipliers(DPLParser.L_evalStatement_evalCalculateStatement_multipliersContext ctx){
        return evalStatement.visitL_evalStatement_evalCalculateStatement_multipliers(ctx);
    }

    public Node visitL_evalStatement_evalCalculateStatement_minus_plus(DPLParser.L_evalStatement_evalCalculateStatement_minus_plusContext ctx){
        return evalStatement.visitL_evalStatement_evalCalculateStatement_minus_plus(ctx);
    }

    public Node visitL_evalStatement_evalConcatenateStatement(DPLParser.L_evalStatement_evalConcatenateStatementContext ctx) {
        return evalStatement.visitL_evalStatement_evalConcatenateStatement(ctx);
    }
}
