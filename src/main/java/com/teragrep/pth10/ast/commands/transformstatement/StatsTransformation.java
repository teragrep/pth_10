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

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.functions.dpf_02.SortByClause;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.steps.sort.SortStep;
import com.teragrep.pth10.steps.stats.StatsStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base transformation class for the <code>stats</code> command
 */
public class StatsTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsTransformation.class);
    List<Object> queueOfAggregates = new ArrayList<>(); // contains all aggregate ColumnNodes

    final List<String> byFields = new ArrayList<>();
    final List<Column> listOfByFields = new ArrayList<>(); // seq of fields to be used for groupBy
    private final List<SortByClause> listOfSbc = new ArrayList<>();

    public StatsStep statsStep;
    private final DPLParserCatalystContext catCtx;

    public StatsTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    /* 
     * Command info:
     * Performs the AggregateFunction with given fieldRenameInstruction and byInstruction
     * Example command:
     * index=index_A | stats avg(offset) AS avg_offset BY sourcetype
     * 
     * Tree:
     * --------------StatsTransformation-------------------
     * ---|------------------------|------------------|----------------------------|------------
     * COMMAND_MODE_STATS aggregateFunction t_stats_fieldRenameInstruction t_stats_byInstruction
     * ------------------------------------------------|--------------------------|-------------
     * --------------------------------------COMMAND_STATS_MODE_AS fieldType--COMMAND_STATS_MODE_BY fieldListType
     * */
    public Node visitStatsTransformation(DPLParser.StatsTransformationContext ctx) {
        Node rv = statsTransformationEmitCatalyst(ctx);
        return rv;
    }

    public Node statsTransformationEmitCatalyst(DPLParser.StatsTransformationContext ctx) {
        // Process children
        // COMMAND_MODE_STATS t_stats_partitions? t_stats_allnum? t_stats_delim? t_stats_agg
        for (int i = 0; i < ctx.getChildCount(); ++i) {
            ParseTree child = ctx.getChild(i);
            LOGGER.debug("Processing child: <{}>", child.getText());
            if (child instanceof TerminalNode) {
                LOGGER.debug("typeof child = TerminalNode");
                continue; /* Skip stats keyword */
            }
            else if (child instanceof DPLParser.T_stats_aggContext) {
                visit(child);
            }
            // FIXME Implement: t_stats_partitions , t_stats_allnum , t_stats_delim
        }

        List<Column> listOfCompleteAggregations = new ArrayList<>(); // contains all aggregate Columns

        for (int i = 0; i < queueOfAggregates.size(); ++i) {
            Object item = queueOfAggregates.get(i);

            // If next in queue is a column
            if (item instanceof ColumnNode) {

                // Check if out of index
                Object nextToItem = null;
                if (queueOfAggregates.size() - 1 >= i + 1) {
                    nextToItem = queueOfAggregates.get(i + 1);
                }

                // Check for fieldRename
                if (nextToItem instanceof String) {
                    listOfCompleteAggregations.add(((ColumnNode) item).getColumn().name((String) nextToItem));
                    i++;
                }
                // No fieldRename
                else {
                    listOfCompleteAggregations.add(((ColumnNode) item).getColumn());
                }
            }
        }

        statsStep = new StatsStep(listOfCompleteAggregations, listOfByFields);
        SortStep sortStep = new SortStep(catCtx, listOfSbc, this.catCtx.getDplRecallSize(), false);

        List<AbstractStep> steps = new ArrayList<>();
        steps.add(statsStep);
        steps.add(sortStep);
        return new StepListNode(steps);
    }

    // AS fieldType
    public Node visitT_stats_fieldRenameInstruction(DPLParser.T_stats_fieldRenameInstructionContext ctx) {
        return new StringNode(new Token(Type.STRING, ctx.getChild(1).getText()));
    }

    // BY fieldListType
    public Node visitT_stats_byInstruction(DPLParser.T_stats_byInstructionContext ctx) {
        // Child #0 "BY"
        // Child #1 fieldListType
        return visit(ctx.getChild(1));
    }

    // fieldListType : fieldType ((COMMA)? fieldType)*?
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        List<String> fields = new ArrayList<>();
        ctx.children.forEach(child -> {
            String field = child.getText();
            fields.addAll(Arrays.asList(field.split(",")));
        });

        return new StringListNode(fields);
    }

    // t_stats_agg
    public Node visitT_stats_agg(DPLParser.T_stats_aggContext ctx) {
        AggregateFunction aggregateFunction = new AggregateFunction(catCtx);

        ctx.children
                .forEach(
                        child -> {
                            // AS fieldType
                            if (child instanceof DPLParser.T_stats_fieldRenameInstructionContext) {
                                LOGGER.debug("typeof child = fieldRenameInstructionCtx");
                                queueOfAggregates.add(visit(child).toString());
                            }
                            // BY fieldListType
                            else if (child instanceof DPLParser.T_stats_byInstructionContext) {
                                LOGGER.debug("typeof child = byInstructionCtx");
                                byFields.addAll(((StringListNode) visit(child)).asList());
                                listOfByFields
                                        .addAll(byFields.stream().map(functions::col).collect(Collectors.toList()));
                                listOfSbc.addAll(byFields.stream().map(this::createSbc).collect(Collectors.toList()));
                            }
                            // other; aggregateFunction visit
                            else if (child instanceof DPLParser.AggregateFunctionContext) {
                                LOGGER.debug("typeof child = AggregateFunctionCtx");
                                queueOfAggregates
                                        .add(
                                                (ColumnNode) aggregateFunction
                                                        .visitAggregateFunction(
                                                                (DPLParser.AggregateFunctionContext) child
                                                        )
                                        );
                            }
                        }
                );

        return null;

    }

    private SortByClause createSbc(String fieldName) {
        SortByClause sbc = new SortByClause();
        sbc.setFieldName(fieldName);
        sbc.setDescending(false);
        sbc.setLimit(this.catCtx.getDplRecallSize());
        sbc.setSortAsType(SortByClause.Type.AUTOMATIC);
        return sbc;
    }
}
