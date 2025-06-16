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

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.steps.eventstats.EventstatsStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EventstatsTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventstatsTransformation.class);
    private DPLParserCatalystContext catCtx;
    private final String hdfsPath;
    private final List<Column> listOfAggregations = new ArrayList<>();
    public EventstatsStep eventstatsStep = null;

    public EventstatsTransformation(String hdfsPath, DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
        this.hdfsPath = hdfsPath;
    }

    @Override
    public Node visitEventstatsTransformation(DPLParser.EventstatsTransformationContext ctx) {
        LOGGER.debug("Visiting eventstats transformation: text=<{}>", ctx.getText());
        // eventstats [allnum=bool] stats-agg-term [by-clause]
        this.eventstatsStep = new EventstatsStep();

        // BY instruction
        String byInst = null;

        // FIXME implement allnum=bool parameter
        if (ctx.t_eventstats_allnumParameter() != null) {
            LOGGER.warn("Detected allnum parameter; however, it is not yet implemented and will be skipped.");
            Node allNumParamNode = visit(ctx.t_eventstats_allnumParameter());
        }

        // go through stats-agg-term, by clause and as clause
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);

            if (child instanceof DPLParser.T_eventstats_aggregationInstructionContext) {
                LOGGER.debug("Detected aggregation instruction");
                visit(child);
            }
            else if (child instanceof DPLParser.T_eventstats_byInstructionContext) {
                LOGGER.debug("Detected BY instruction");
                byInst = ((StringNode) visit(child)).toString();
            }
            else if (child instanceof DPLParser.T_eventstats_fieldRenameInstructionContext) {
                LOGGER.debug("Detected field rename (AS) instruction");
                visit(child);
            }
        }

        eventstatsStep.setByInstruction(byInst);
        eventstatsStep.setHdfsPath(hdfsPath);
        eventstatsStep.setCatCtx(catCtx);
        eventstatsStep.setListOfAggregations(listOfAggregations);

        return new StepNode(eventstatsStep);
    }

    @Override
    public Node visitT_eventstats_aggregationInstruction(DPLParser.T_eventstats_aggregationInstructionContext ctx) {
        ParseTree cmd = ctx.getChild(0);
        AggregateFunction aggFunction = new AggregateFunction(catCtx);
        Column aggCol = null;

        if (cmd instanceof TerminalNode) {
            /* if (((TerminalNode)cmd).getSymbol().getType() == DPLLexer.COMMAND_EVENTSTATS_MODE_COUNT) {
                LOGGER.info("Implied wildcard COUNT mode - count({}", ds.columns()[0] + ")");
                aggCol = functions.count(ds.columns()[0]).as("count");
            }*/
        }
        else if (cmd instanceof DPLParser.AggregateFunctionContext) {
            // visit agg function
            LOGGER.debug("Aggregate function: text=<{}>", cmd.getText());
            Node aggNode = aggFunction.visit((DPLParser.AggregateFunctionContext) cmd);
            aggCol = ((ColumnNode) aggNode).getColumn();
        }

        ParseTree fieldRenameInst = ctx.getChild(1);
        if (fieldRenameInst instanceof DPLParser.T_eventstats_fieldRenameInstructionContext) {
            LOGGER.debug("Field rename instruction: text=<{}>", fieldRenameInst.getText());
            // AS new-fieldname
            aggCol = aggCol.as(fieldRenameInst.getChild(1).getText());
        }

        listOfAggregations.add(aggCol);
        return null;
    }

    @Override
    public Node visitT_eventstats_byInstruction(DPLParser.T_eventstats_byInstructionContext ctx) {
        String byInst = ctx.getChild(1).getText();

        LOGGER.info("byInst: text=<{}>", byInst);

        return new StringNode(new Token(Token.Type.STRING, byInst));
    }

    /**
     * Doesn't seem to get used, goes through aggregationInstruction-&gt;fieldRenameInstruction
     * 
     * @param ctx T_eventstats_fieldRenameInstructionContext
     * @return StringNode with rename field
     */
    @Override
    public Node visitT_eventstats_fieldRenameInstruction(DPLParser.T_eventstats_fieldRenameInstructionContext ctx) {
        String renameInst = ctx.getChild(1).getText();

        LOGGER.info("renameInst: text=<{}>", renameInst);

        return new StringNode(new Token(Token.Type.STRING, renameInst));
    }

}
