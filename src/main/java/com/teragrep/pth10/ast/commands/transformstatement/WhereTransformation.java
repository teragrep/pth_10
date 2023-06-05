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
import com.teragrep.pth10.ast.bo.ColumnNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.commands.evalstatement.EvalStatement;
import com.teragrep.pth10.steps.where.WhereStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.util.List;

/**
 * Class containing all the visitor methods for the <code>where</code> command<br>
 * Can be piped like <code>{@literal ... | where col > 1}</code> to limit the results to only
 * the values where the statement is true
 */
public class WhereTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WhereTransformation.class);
    private boolean debugEnabled = false;
    private List<String> traceBuffer = null;
    private Document doc;
    private ProcessingStack transformPipe = null;
    DPLParserCatalystContext catCtx = null;
    private boolean aggregatesUsed = false;
    private String aggregateField = null;

    public WhereStep whereStep = null;

    // eval-transformation support
    EvalStatement evalStatement = null;

    // emit catalyst
    public WhereTransformation(DPLParserCatalystContext catCtx, ProcessingStack transformPipe, List<String> buf) {
        this.traceBuffer = buf;
        this.doc = null;
        this.catCtx = catCtx;
        this.transformPipe = transformPipe;
        this.evalStatement = new EvalStatement(catCtx, transformPipe, buf);
    }

    // Parameters for Groupmapper queries

    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    public String getAggregateField() {
        return this.aggregateField;
    }


    /**
     * whereTransformation : WHERE evalStatement ;
     */
    @Override
    public Node visitWhereTransformation(DPLParser.WhereTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!this.transformPipe.isEmpty()) {
            ds = this.transformPipe.pop();
        }
        this.whereStep = new WhereStep(ds);

        ColumnNode cn = (ColumnNode)whereTransformationEmitCatalyst(ctx);

        this.whereStep.setWhereColumn(cn.getColumn());
        LOGGER.info("Set whereStep column to: " + cn.getColumn().expr().sql());

        this.transformPipe.push(this.whereStep.get());
        return cn;
    }

    private Node whereTransformationEmitCatalyst(DPLParser.WhereTransformationContext ctx) {
        traceBuffer.add(ctx.getChildCount() + " WhereTransformation(Catalyst) " + ctx.getText()+ " stackIsEmpty:"+transformPipe.isEmpty());

        boolean isNot = false;
        // where NOT like(field, something)
        if (ctx.getChild(1).getChild(0) instanceof TerminalNode) {
            TerminalNode term = (TerminalNode) ctx.getChild(1).getChild(0);
            if (term.getSymbol().getType() == DPLLexer.EVAL_LANGUAGE_MODE_NOT) {
                isNot = true;
            }
        }

        Node n = evalStatement.visit(ctx.getChild(1));
        String sql = null;
        if (n instanceof ColumnNode) {
            Column whereCol = ((ColumnNode)n).getColumn();
            // apply NOT if it was present
            if (isNot) {
                n = new ColumnNode(functions.not(whereCol));
            }
            sql = whereCol.expr().sql();
            traceBuffer.add(ctx.getChildCount() + " WhereTransformation(Catalyst) out:" + sql);
            LOGGER.info(ctx.getChildCount() + " WhereTransformation(Catalyst) out:" + sql);
        } else {
            if (n != null)
                throw new RuntimeException(
                        "Where transformation operation not supported for type:" + n.getClass().getName()+" value="+n.toString());
            else
                throw new RuntimeException(
                        "Where transformation operation not supported for type:" + n);
        }
        return n;
    }

    @Override
    public Node visitT_eval_evalParameter(DPLParser.T_eval_evalParameterContext ctx) {
        return evalStatement.visitT_eval_evalParameter(ctx);
    }

    public Node visitEvalMethodIf(DPLParser.EvalMethodIfContext ctx) {
        return evalStatement.visitEvalMethodIf(ctx);
    }

    @Override
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

    public Node visitEvalIntegerType(DPLParser.EvalIntegerTypeContext ctx) {
        return evalStatement.visitEvalIntegerType(ctx);
    }

    public Node visitEvalStringType(DPLParser.EvalStringTypeContext ctx) {
        return evalStatement.visitEvalStringType(ctx);
    }

    public Node visitSubEvalStatement(DPLParser.SubEvalStatementContext ctx) {
        return evalStatement.visitSubEvalStatement(ctx);
    }
}
