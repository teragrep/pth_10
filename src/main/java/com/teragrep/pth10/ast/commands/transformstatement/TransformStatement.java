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
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepListNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.ast.commands.transformstatement.accum.AccumTransformation;
import com.teragrep.pth10.ast.commands.transformstatement.rex4j.Rex4jTransformation;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base statement for all transformation commands, for example statistics (stats) command, evaluation (eval) command and
 * Teragrep system commands.
 */
public class TransformStatement extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformStatement.class);
    private final DPLParserCatalystContext catCtx;
    private final DPLParserCatalystVisitor catVisitor;

    /**
     * Constructor for the TransformStatement. Initializes various classes for different transform commands.
     * 
     * @param catCtx     Catalyst context object containing objects like the Zeppelin config.
     * @param catVisitor Catalyst visitor object used for walking the parse tree.
     */
    public TransformStatement(DPLParserCatalystContext catCtx, DPLParserCatalystVisitor catVisitor) {
        this.catVisitor = catVisitor;
        this.catCtx = catCtx;
        LOGGER.info("Initializing TransformStatement with catCtx=<{}> catVisitor=<{}>", catCtx, catVisitor);
    }

    @Override
    public Node visitTransformStatement(DPLParser.TransformStatementContext ctx) {
        LOGGER.info("visitTransformStatement incoming: text=<{}>", ctx.getText());
        return transformStatementEmitCatalyst(ctx);
    }

    /**
     * Goes through the transform statement, visiting the given transform commands in the statement.
     * 
     * @param ctx TransformStatement context
     * @return node generated during the walk
     */
    private Node transformStatementEmitCatalyst(DPLParser.TransformStatementContext ctx) {
        if (ctx.EOF() != null) {
            LOGGER.info("TransformStatement <EOF>, return null");
            return null;
        }

        // Proceed leaves
        Node left;
        ParseTree leftTree = ctx.getChild(0);
        ParseTree rightTree = ctx.getChild(2);

        // Logging
        if (leftTree != null) {
            LOGGER.info("-> Left tree: text=<{}>", leftTree.getText());
        }
        else {
            LOGGER.info("-> Left tree NULL");
        }

        if (rightTree != null) {
            LOGGER.info("-> Right tree: text=<{}>", rightTree.getText());
        }
        else {
            LOGGER.info("-> Right tree NULL");
        }

        // Visit command transformations
        left = visit(leftTree);

        if (left != null) {
            if (left instanceof StepNode) {
                LOGGER.debug("Add step to list");
                this.catVisitor.getStepList().add(((StepNode) left).get());
            }
            else if (left instanceof StepListNode) {
                LOGGER.debug("Add multiple steps to list");
                ((StepListNode) left).asList().forEach(step -> this.catVisitor.getStepList().add(step));
            }
            else {
                LOGGER
                        .error(
                                "visit of leftTree did not return Step(List)Node, instead got: class=<{}>",
                                left.getClass().getName()
                        );
            }
            // Add right branch
            if (rightTree != null) {
                Node right = visit(rightTree);
                if (right != null) {
                    LOGGER.debug("Right side was not null: <{}>", right);
                    left = right;
                }
                else {
                    LOGGER.debug("transformStatement <EOF>");
                }
            }
            else { // EOF, return only left
                LOGGER.debug("transformStatement <EOF> return only left transformation");
            }

            return left;
        }
        else {
            // If null is returned, the command is not implemented.
            // All implemented commands return a StepNode or a StepListNode.
            throw new IllegalArgumentException("The provided command '" + ctx.getText() + "' is not yet implemented.");
        }
    }

    @Override
    public Node visitMakeresultsTransformation(DPLParser.MakeresultsTransformationContext ctx) {
        // makeresults command
        return new MakeresultsTransformation(catCtx).visitMakeresultsTransformation(ctx);
    }

    @Override
    public Node visitChartTransformation(DPLParser.ChartTransformationContext ctx) {
        // chart command
        return new ChartTransformation(catCtx).visitChartTransformation(ctx);
    }

    @Override
    public Node visitTimechartTransformation(DPLParser.TimechartTransformationContext ctx) {
        // timechart command
        return new TimechartTransformation(catCtx, catVisitor).visitTimechartTransformation(ctx);
    }

    @Override
    public Node visitFieldsTransformation(DPLParser.FieldsTransformationContext ctx) {
        // fields command
        return new FieldsTransformation(catCtx).visitFieldsTransformation(ctx);
    }

    @Override
    public Node visitWhereTransformation(DPLParser.WhereTransformationContext ctx) {
        // where command
        return new WhereTransformation(catCtx).visitWhereTransformation(ctx);
    }

    @Override
    public Node visitEvalTransformation(DPLParser.EvalTransformationContext ctx) {
        // eval command
        // Note: eval handles _time as bigint internally
        return new EvalTransformation(catCtx).visitEvalTransformation(ctx);
    }

    @Override
    public Node visitRexTransformation(DPLParser.RexTransformationContext ctx) {
        // rex command
        return new RexTransformation(catCtx).visitRexTransformation(ctx);
    }

    @Override
    public Node visitRex4jTransformation(DPLParser.Rex4jTransformationContext ctx) {
        // rex4j command
        return new Rex4jTransformation(catCtx).visitRex4jTransformation(ctx);
    }

    @Override
    public Node visitStrcatTransformation(DPLParser.StrcatTransformationContext ctx) {
        // strcat command
        return new StrcatTransformation(catCtx.nullValue).visitStrcatTransformation(ctx);
    }

    @Override
    public Node visitStatsTransformation(DPLParser.StatsTransformationContext ctx) {
        // stats command
        return new StatsTransformation(catCtx).visitStatsTransformation(ctx);
    }

    @Override
    public Node visitSearchTransformation(DPLParser.SearchTransformationContext ctx) {
        // search command
        return new SearchTransformation(catCtx).visitSearchTransformation(ctx);
    }

    @Override
    public Node visitTopTransformation(DPLParser.TopTransformationContext ctx) {
        // top command
        return new TopTransformation(catCtx).visitTopTransformation(ctx);
    }

    @Override
    public Node visitExplainTransformation(DPLParser.ExplainTransformationContext ctx) {
        // explain command
        return new ExplainTransformation(catCtx).visitExplainTransformation(ctx);
    }

    @Override
    public Node visitTeragrepTransformation(DPLParser.TeragrepTransformationContext ctx) {
        // teragrep command
        return new TeragrepTransformation(catCtx, this.catVisitor).visitTeragrepTransformation(ctx);
    }

    @Override
    public Node visitDplTransformation(DPLParser.DplTransformationContext ctx) {
        // dpl command
        return new DPLTransformation(catCtx).visitDplTransformation(ctx);
    }

    @Override
    public Node visitJoinTransformation(DPLParser.JoinTransformationContext ctx) {
        // join command
        return new JoinTransformation(catVisitor, catCtx).visitJoinTransformation(ctx);
    }

    @Override
    public Node visitSendemailTransformation(DPLParser.SendemailTransformationContext ctx) {
        // sendemail command
        return new SendemailTransformation(catCtx).visitSendemailTransformation(ctx);
    }

    @Override
    public Node visitConvertTransformation(DPLParser.ConvertTransformationContext ctx) {
        // convert command
        return new ConvertTransformation().visitConvertTransformation(ctx);
    }

    @Override
    public Node visitTableTransformation(DPLParser.TableTransformationContext ctx) {
        // table command
        return new TableTransformation().visitTableTransformation(ctx);
    }

    @Override
    public Node visitSortTransformation(DPLParser.SortTransformationContext ctx) {
        // sort command
        return new SortTransformation(catCtx, this.catVisitor).visitSortTransformation(ctx);
    }

    @Override
    public Node visitSpathTransformation(DPLParser.SpathTransformationContext ctx) {
        // spath command
        return new SpathTransformation(catCtx).visitSpathTransformation(ctx);
    }

    @Override
    public Node visitRegexTransformation(DPLParser.RegexTransformationContext ctx) {
        // regex command
        return new RegexTransformation().visitRegexTransformation(ctx);
    }

    @Override
    public Node visitRenameTransformation(DPLParser.RenameTransformationContext ctx) {
        // rename command
        return new RenameTransformation().visitRenameTransformation(ctx);
    }

    @Override
    public Node visitReplaceTransformation(DPLParser.ReplaceTransformationContext ctx) {
        // replace command
        return new ReplaceTransformation().visitReplaceTransformation(ctx);
    }

    @Override
    public Node visitEventstatsTransformation(DPLParser.EventstatsTransformationContext ctx) {
        // eventstats command
        return new EventstatsTransformation(catVisitor.getHdfsPath(), catCtx).visitEventstatsTransformation(ctx);
    }

    @Override
    public Node visitDedupTransformation(DPLParser.DedupTransformationContext ctx) {
        // dedup command
        return new DedupTransformation(catCtx).visitDedupTransformation(ctx);
    }

    @Override
    public Node visitIplocationTransformation(DPLParser.IplocationTransformationContext ctx) {
        // iplocation command
        return new IplocationTransformation(catCtx, this.catVisitor).visitIplocationTransformation(ctx);
    }

    @Override
    public Node visitPredictTransformation(DPLParser.PredictTransformationContext ctx) {
        // predict command
        return new PredictTransformation().visitPredictTransformation(ctx);
    }

    @Override
    public Node visitFormatTransformation(DPLParser.FormatTransformationContext ctx) {
        // format command
        return new FormatTransformation().visitFormatTransformation(ctx);
    }

    @Override
    public Node visitXmlkvTransformation(DPLParser.XmlkvTransformationContext ctx) {
        // xmlkv command
        return new XmlkvTransformation(catCtx).visitXmlkvTransformation(ctx);
    }

    @Override
    public Node visitRangemapTransformation(DPLParser.RangemapTransformationContext ctx) {
        // rangemap command
        return new RangemapTransformation().visitRangemapTransformation(ctx);
    }

    @Override
    public Node visitFillnullTransformation(DPLParser.FillnullTransformationContext ctx) {
        // fillnull command
        return new FillnullTransformation(catCtx.nullValue).visitFillnullTransformation(ctx);
    }

    @Override
    public Node visitAccumTransformation(DPLParser.AccumTransformationContext ctx) {
        // accum command
        return new AccumTransformation(catCtx).visitAccumTransformation(ctx);
    }

    @Override
    public Node visitAddtotalsTransformation(DPLParser.AddtotalsTransformationContext ctx) {
        // addtotals command
        return new AddtotalsTransformation(catCtx).visitAddtotalsTransformation(ctx);
    }
}
