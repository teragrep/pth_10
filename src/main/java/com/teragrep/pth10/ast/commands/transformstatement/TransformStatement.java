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
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.ColumnNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.commands.transformstatement.accum.AccumTransformation;
import com.teragrep.pth10.ast.commands.transformstatement.rex4j.Rex4jTransformation;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.util.*;


/**
 * Base statement for all transformation commands, for example
 * statistics (stats) command, evaluation (eval) command and Teragrep system commands.
 */
public class TransformStatement extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformStatement.class);

    private boolean debugEnabled = false;
    private DPLParserCatalystContext catCtx = null;

    private List<String> traceBuffer = null;
    private Document doc;
    private Dataset<Row> ds = null;
    private ProcessingStack processingStack = null;
    // transformation commands
    ChartTransformation chartTransformation = null;
    TimechartTransformation timechartTransformation = null;
    WhereTransformation whereTransformation = null;
    AccumTransformation accumTransformation = null;
    SearchTransformation searchTransformation = null;

    private List<String> dplHosts = new ArrayList<>();
    private List<String> dplSourceTypes = new ArrayList<>();
    private boolean aggregatesUsed = false;
    private String aggregateField = null;

    // display limit times
    String earliestLimit = "-10y";
    String latestLimit = null;

    private Stack<String> timeFormatStack = new Stack<>();

    // list of transformation fragments which are added around logical block as
    // nested SELECTS
    private List<Node> transformPipeline = new ArrayList<>();

    // Symbol table for mapping DPL default column names
    private Map<String, Object> symbolTable = new HashMap<>();

    /**
     * Constructor for the TransformStatement.
     * Initializes various classes for different transform commands.
     * @param catCtx Catalyst context object containing objects like the Zeppelin config.
     * @param transformPipe ProcessingStack, containing the dataset being worked on
     * @param tBuf Tracebuffer for debugging purposes
     * @param symbolTable -
     */
    public TransformStatement(DPLParserCatalystContext catCtx, ProcessingStack transformPipe, List<String> tBuf, Map<String,Object> symbolTable) {
        this.ds = null;
        this.processingStack = transformPipe;
        this.traceBuffer = tBuf;
        this.symbolTable = symbolTable;
        chartTransformation = new ChartTransformation(catCtx, transformPipe, tBuf);
        timechartTransformation = new TimechartTransformation(catCtx, transformPipe, tBuf);
        whereTransformation = new WhereTransformation(catCtx, transformPipe, tBuf);
        this.catCtx = catCtx;
        accumTransformation = new AccumTransformation(tBuf, transformPipe);
        searchTransformation = new SearchTransformation(symbolTable, tBuf, transformPipe);
        traceBuffer.add("Init transform statement(Catalyst) Stack\n");
        LOGGER.info("Init transform statement(Catalyst) Stack:"+transformPipe+ " ctx:"+catCtx);
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
    public void setAggregatesUsed(boolean newValue) {
        this.aggregatesUsed = newValue;
    }

    public String getAggregateField() {
        return this.aggregateField;
    }

    public List<Node> getTransformPipeline() {
        return transformPipeline;
    }

    /**
     * Syntactic transform statement <blockquote>
     *
     * <pre>
     * transformStatement
     * : PIPE transform_XXX_Operation
     * ;
     * </pre>
     *
     * </blockquote>
     */
    @Override
    public Node visitTransformStatement(DPLParser.TransformStatementContext ctx) {
        Node rv = null;
        LOGGER.info("visitTransformStatement incoming:"+ctx.getText());
        rv = transformStatementEmitCatalyst(ctx);
        return rv;
    }

    /**
     * Goes through the transform statement, visiting the given transform commands in the statement.
     * @param ctx TransformStatement context
     * @return node generated during the walk
     */
    private Node transformStatementEmitCatalyst(DPLParser.TransformStatementContext ctx) {
        Node left = null;
        traceBuffer.add(ctx.getChildCount() + " transformStatementEmitCatalyst:" + ctx.getText() + " stackIsEmpty:" + processingStack.isEmpty());

        if (ctx.getChildCount() == 1) {
            LOGGER.info("TransfromStatement child count 1, stack empty: " + processingStack.isEmpty());
            traceBuffer.add("EOF stackIsEmpty:" + processingStack.isEmpty() + " Stack:" + processingStack);
            traceBuffer.add("transformStatement Right = <EOF>");
            return null;
        }
        // Proceed leaves
        ParseTree leftTree = ctx.getChild(1);
        ParseTree rightTree = ctx.getChild(2);

        if (ctx.getChild(1) != null) LOGGER.info("xxxx Left tree = " + ctx.getChild(1).getText()); else LOGGER.info("xxxx Left tree NULL");
        if (ctx.getChild(2) != null) LOGGER.info("xxxx Right tree = " + ctx.getChild(2).getText()); else LOGGER.info("xxxx Right tree NULL");

        LOGGER.info("Sequential stack size: " + processingStack.size(ProcessingStack.StackMode.SEQUENTIAL));
        LOGGER.info("Parallel stack size: " + processingStack.size(ProcessingStack.StackMode.PARALLEL));
        LOGGER.info("Stack mode: " + processingStack.getStackMode());
        LOGGER.info("Aggregates used: " + this.aggregatesUsed);

        
        /* 
         * SequentialOpsStack is empty here only when within forEachBatch 
         * 
         * The right tree (= continuation of the parse tree) will be saved into the processingStack,
         * so the parsing can be continued in the BatchProcessor's forEachBatch.
         * This allows multiple back-to-back aggregations.
         * */

        if (aggregatesUsed && processingStack.getStackMode() == ProcessingStack.StackMode.PARALLEL) {
            // TODO check chart etc aggregations for unneeded code after this
            LOGGER.info("Switched to SEQUENTIAL MODE with " +
                    "parallelOpsStack: " + processingStack.getParallelOpsStack());
            processingStack.setParseTree(ctx);
            processingStack.push("hackism: to foreachbatch conversion",
                    processingStack.pop(),
                    ProcessingStack.StackMode.SEQUENTIAL,
                    null,
                    null,
                    this.aggregatesUsed
            );
            left = null;
            rightTree = null;
            return null;
        }


        if (leftTree instanceof DPLParser.MakeresultsTransformationContext) {
            // makeresults command
            MakeresultsTransformation makeresultsTransformation = new MakeresultsTransformation(processingStack, catCtx);
            left = (CatalystNode) makeresultsTransformation.visitMakeresultsTransformation((DPLParser.MakeresultsTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.ChartTransformationContext) {
        	chartTransformation.setAggregatesUsed(this.aggregatesUsed);
            left = chartTransformation.visitChartTransformation((DPLParser.ChartTransformationContext) leftTree);
            traceBuffer.add(" --- transformStatementEmitCatalyst after chart:" + left + " Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty());
            // pass fields forward
            this.aggregatesUsed = chartTransformation.getAggregatesUsed();
            this.aggregateField = chartTransformation.getAggregateField();

        } else if (leftTree instanceof DPLParser.TimechartTransformationContext) {
            left = timechartTransformation.visitTimechartTransformation((DPLParser.TimechartTransformationContext) leftTree);
            traceBuffer.add(" --- transformStatementEmitCatalyst after timechart:" + left + " Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty());
            // pass fields forward
            this.aggregatesUsed = timechartTransformation.getAggregatesUsed();
            this.aggregateField = timechartTransformation.getAggregateField();
        } else if (leftTree instanceof DPLParser.FieldsTransformationContext) {
            FieldsTransformation fieldsTransformation = new FieldsTransformation(catCtx, symbolTable, traceBuffer, processingStack);
            left = (CatalystNode) fieldsTransformation.visitFieldsTransformation((DPLParser.FieldsTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.WhereTransformationContext) {
            left = whereTransformation.visitWhereTransformation((DPLParser.WhereTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.EvalTransformationContext) {
            // eval handles _time as bigint internally
            LOGGER.info("transformStatement calling eval with " + ctx.getText());
            EvalTransformation evalTransformation = new EvalTransformation(catCtx, processingStack, traceBuffer);
            left = (CatalystNode) evalTransformation.visitEvalTransformation((DPLParser.EvalTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.RexTransformationContext) {
            // rex command
            RexTransformation rexTransformation = new RexTransformation(processingStack, catCtx);
            left = (CatalystNode) rexTransformation.visitRexTransformation((DPLParser.RexTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.Rex4jTransformationContext) {
            // rex4j command
            Rex4jTransformation rex4jTransformation = new Rex4jTransformation(symbolTable, traceBuffer, processingStack);
            left = (CatalystNode) rex4jTransformation.visitRex4jTransformation(((DPLParser.Rex4jTransformationContext) leftTree));
        } else if (leftTree instanceof DPLParser.StrcatTransformationContext) {
        	// strcat command
        	LOGGER.info(" -- leftTree is an instance of StrcatTransformationCtx");
        	StrcatTransformation strcatTransformation = new StrcatTransformation(symbolTable, this.traceBuffer, this.processingStack);
        	left = (CatalystNode) strcatTransformation.visitStrcatTransformation((DPLParser.StrcatTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.AccumTransformationContext) {
        	// TODO accumTransformation
        	LOGGER.info(" -- leftTree is an instance of AccumTransformationCtx");
        	//AccumTransformation accumTransformation = new AccumTransformation(symbolTable, this.traceBuffer, this.processingPipe, this.mode);
        	accumTransformation.setAggregatesUsed(this.aggregatesUsed);
        	left = (CatalystNode) accumTransformation.visitAccumTransformation((DPLParser.AccumTransformationContext) leftTree);
        	this.aggregatesUsed = accumTransformation.getAggregatesUsed();
        	//this.aggregateField = accumTransformation.getAggregateField();
        } else if (leftTree instanceof DPLParser.StatsTransformationContext) {
        	LOGGER.info("-- leftTree is an instance of StatsTransformationCtx");
        	StatsTransformation statsTransformation = new StatsTransformation(symbolTable, this.traceBuffer, this.processingStack);
        	statsTransformation.setAggregatesUsed(aggregatesUsed);
        	left = (CatalystNode) statsTransformation.visitStatsTransformation((DPLParser.StatsTransformationContext) leftTree);
        	this.aggregatesUsed = statsTransformation.getAggregatesUsed();
        } else if (leftTree instanceof DPLParser.SearchTransformationContext) {
            left = (CatalystNode) searchTransformation.visitSearchTransformation((DPLParser.SearchTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.TopTransformationContext) {
            TopTransformation topTransformation = new TopTransformation(catCtx, processingStack, traceBuffer);
            left = (CatalystNode) topTransformation.visitTopTransformation((DPLParser.TopTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.ExplainTransformationContext) {
            ExplainTransformation explainTransformation = new ExplainTransformation(catCtx, processingStack, traceBuffer);
            left = (CatalystNode) explainTransformation.visitExplainTransformation((DPLParser.ExplainTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.TeragrepTransformationContext) {
            TeragrepTransformation teragrepTransformation = new TeragrepTransformation(catCtx, processingStack, traceBuffer, this.aggregatesUsed);
            left = (CatalystNode) teragrepTransformation.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.DplTransformationContext) {
            DPLTransformation dplTransformation = new DPLTransformation(catCtx, processingStack, traceBuffer, symbolTable);
            left = (CatalystNode) dplTransformation.visitDplTransformation((DPLParser.DplTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.JoinTransformationContext) {
        	// join command
        	JoinTransformation joinTransformation = new JoinTransformation(traceBuffer, processingStack, catCtx);
        	joinTransformation.setAggregatesUsed(aggregatesUsed);
        	left = (CatalystNode) joinTransformation.visitJoinTransformation((DPLParser.JoinTransformationContext) leftTree);
        	this.aggregatesUsed = joinTransformation.getAggregatesUsed();
        } else if (leftTree instanceof DPLParser.SendemailTransformationContext) {
        	// sendemail command
            // Make sure sendemail is in SEQUENTIAL mode at all times
            // If it is not, push the current dataset into sequential stack, and re-visit in sequential stack mode.
            if (processingStack.getStackMode() == ProcessingStack.StackMode.SEQUENTIAL) {
                SendemailTransformation sendEmailTransformation = new SendemailTransformation(traceBuffer, processingStack, catCtx);
                sendEmailTransformation.setAggregatesUsed(aggregatesUsed);
                left = (CatalystNode) sendEmailTransformation.visitSendemailTransformation((DPLParser.SendemailTransformationContext) leftTree);
                this.aggregatesUsed = sendEmailTransformation.getAggregatesUsed();
            }
            else {
                processingStack.setParseTree(ctx);
                processingStack.push("Sendemail: to foreachbatch conversion", processingStack.pop(), ProcessingStack.StackMode.SEQUENTIAL, null, null, this.aggregatesUsed);
                left = null;
                rightTree = null;
            }

        } else if (leftTree instanceof DPLParser.ConvertTransformationContext) {
            // convert command
            ConvertTransformation convertTransformation = new ConvertTransformation(traceBuffer, processingStack, catCtx);
            convertTransformation.setAggregatesUsed(aggregatesUsed);
            left = (CatalystNode) convertTransformation.visitConvertTransformation((DPLParser.ConvertTransformationContext) leftTree);
            this.aggregatesUsed = convertTransformation.getAggregatesUsed();
        } else if (leftTree instanceof DPLParser.TableTransformationContext) {
            // table command
            TableTransformation tableTransformation = new TableTransformation(processingStack, catCtx);
            tableTransformation.setAggregatesUsed(aggregatesUsed);
            left = (CatalystNode) tableTransformation.visitTableTransformation((DPLParser.TableTransformationContext) leftTree);
            this.aggregatesUsed = tableTransformation.getAggregatesUsed();
        } else if (leftTree instanceof DPLParser.SortTransformationContext) {
            // sort command
            // Make sure sort is in SEQUENTIAL mode at all times
            // If it is not, push the current dataset into sequential stack, and re-visit in sequential stack mode.
            processingStack.setSorted(true);
            if (processingStack.getStackMode() == ProcessingStack.StackMode.SEQUENTIAL) {
                LOGGER.info("ProcessingStack was in SEQUENTIAL mode; proceeding to sort");
                LOGGER.info("aggs used before creating sort: " + this.aggregatesUsed);
                SortTransformation sortTransformation = new SortTransformation(processingStack, catCtx);
                sortTransformation.setAggregatesUsed(aggregatesUsed);
                left = (CatalystNode) sortTransformation.visitSortTransformation((DPLParser.SortTransformationContext) leftTree);
                this.aggregatesUsed = sortTransformation.getAggregatesUsed();
            }
            else {
                LOGGER.info("ProcessingStack was in PARALLEL mode, converting to SEQUENTIAL and re-visiting sort");
                processingStack.setParseTree(ctx);
                LOGGER.info("Mode conversion, agg used= " + aggregatesUsed);
                processingStack.push("Sort: to foreachbatch conversion", processingStack.pop(), ProcessingStack.StackMode.SEQUENTIAL, null, null, this.aggregatesUsed);
                left = null;
                rightTree = null;
            }
        } else if (leftTree instanceof DPLParser.SpathTransformationContext) {
            LOGGER.info("Left tree is instanceof SpathTransformationCtx");
            // spath command
            SpathTransformation spathTransformation = new SpathTransformation(processingStack, catCtx);
            left = (CatalystNode) spathTransformation.visitSpathTransformation((DPLParser.SpathTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.RegexTransformationContext) {
            // regex command
            RegexTransformation regexTransformation = new RegexTransformation(processingStack, catCtx);
            left = (CatalystNode) regexTransformation.visitRegexTransformation((DPLParser.RegexTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.RenameTransformationContext) {
            // rename command
            RenameTransformation renameTransformation = new RenameTransformation(processingStack, catCtx);
            left = (CatalystNode) renameTransformation.visitRenameTransformation((DPLParser.RenameTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.ReplaceTransformationContext) {
            // replace command
            ReplaceTransformation replaceTransformation = new ReplaceTransformation(processingStack, catCtx);
            left = (CatalystNode) replaceTransformation.visitReplaceTransformation((DPLParser.ReplaceTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.EventstatsTransformationContext) {
            // eventstats command
            EventstatsTransformation eventstatsTransformation = new EventstatsTransformation(processingStack, catCtx);
            eventstatsTransformation.setAggregatesUsed(this.aggregatesUsed);
            left = (CatalystNode) eventstatsTransformation.visitEventstatsTransformation((DPLParser.EventstatsTransformationContext) leftTree);
            this.aggregatesUsed = eventstatsTransformation.getAggregatesUsed();
        } else if (leftTree instanceof DPLParser.DedupTransformationContext) {
            // dedup command
            // sequential mode should be always on
            //processingStack.setSorted(true); // ignore sorting
            if (processingStack.getStackMode() == ProcessingStack.StackMode.SEQUENTIAL) {
                LOGGER.info("In sequential mode, proceed");
                DedupTransformation dedupTransformation = new DedupTransformation(processingStack, catCtx);
                dedupTransformation.setAggregatesUsed(this.aggregatesUsed);
                left = (CatalystNode) dedupTransformation.visitDedupTransformation((DPLParser.DedupTransformationContext) leftTree);
                // no need to getAggregatesUsed() since this is not an aggregating command
            }
            else {
                LOGGER.info("In parallel mode, set parse tree: " + ctx.getText() + " and convert to sequential");
                processingStack.setParseTree(ctx);
                processingStack.push("Dedup: to foreachbatch conversion", processingStack.pop(), ProcessingStack.StackMode.SEQUENTIAL, null, null, this.aggregatesUsed);
                left = null;
                rightTree = null;
            }
        } else if (leftTree instanceof DPLParser.IplocationTransformationContext) {
            // iplocation command
            IplocationTransformation iplocationTransformation = new IplocationTransformation(processingStack, catCtx);
            left = (CatalystNode) iplocationTransformation.visitIplocationTransformation((DPLParser.IplocationTransformationContext) leftTree);
        } else if (leftTree instanceof DPLParser.PredictTransformationContext) {
            // predict command
            if (!this.aggregatesUsed) {
                throw new IllegalStateException("Predict cannot be used without pre-existing aggregations. Make sure that 'timechart' is the preciding command.");
            }
            PredictTransformation predictTransformation = new PredictTransformation(processingStack, catCtx);
            left = (CatalystNode) predictTransformation.visitPredictTransformation((DPLParser.PredictTransformationContext) leftTree);
        } else {
            left = visit(leftTree);
        }

        if (left != null) {
            traceBuffer.add(" --- transformStatementEmitCatalyst Before right branch visit Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty() + " Stack:" + processingStack);
            Dataset<Row> leftTransformation = processingStack.pop("transformStatementEmitCatalyst");
            processingStack.push("TransformStatementEmit(Catalyst)", leftTransformation);
            // Add right branch
            if (rightTree != null) {
                Node right = visit(rightTree);
                if (right != null) {
                    traceBuffer.add("transformStatement right=" + right + " value=" + right.toString() + " type:" + right.getClass().getName());
                    if (right instanceof ColumnNode) {
                        Column col = ((ColumnNode) right).getColumn();
                        traceBuffer.add("  transformStatementEmitCatalyst right branch(column) Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty());
                        //processingPipe.pop("Replace previous dataset from stack");
                        traceBuffer.add("  transformStatementEmitCatalyst after right clean Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty());
                        //processingPipe.push("transformStatementEmitCatalyst 2",leftTransformation.where(col));
                        // Use right as return value
                        left = right;
                    } else {
                        // right is full dataset, use it
                        CatalystNode tsn = (CatalystNode) right;
                        traceBuffer.add("  transformStatementEmitCatalyst right branch(aggregate) Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty());
                        left = tsn;
                    }
                } else {
                    traceBuffer.add("transformStatement <EOF>");
                }
            } else { // EOF, return  only left
                traceBuffer.add("transformStatement <EOF> return  only left transformation");
                traceBuffer.add("transformStatementEmitCatalyst EOF Stack=" + processingStack + " isEmpty:" + processingStack.isEmpty());
            }

            transformPipeline.add(left);
            traceBuffer.add(" --- transformStatementEmitCatalyst return left  rv:" + left);
            return left;
        } else {
            LOGGER.info("transformStatementEmitCatalyst left is null");
            LOGGER.info("EOF stackIsEmpty:" + processingStack.isEmpty() + " Stack:" + processingStack);
            traceBuffer.add("EOF stackIsEmpty:" + processingStack.isEmpty() + " Stack:" + processingStack);
            traceBuffer.add("transformStatement left = <EOF>");
            return null;
        }
//        throw new RuntimeException("Transformation operation(" + ctx.getText() + ") not supported yet left="+leftTree.getClass().getName() );
    }
}
