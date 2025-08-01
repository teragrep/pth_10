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

import com.teragrep.functions.dpf_02.SortByClause;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.steps.sort.SortStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base transformation class for the <code>sort</code> command<br>
 * Processes the arguments and provides it for dpf_02 (BatchCollect) for sorting purposes.
 */
public class SortTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortTransformation.class);
    private final DPLParserCatalystContext catCtx;
    private final DPLParserCatalystVisitor catVisitor;

    // parameters
    private int limit = 10000;
    private boolean desc = false;

    // sort by clauses
    private final List<SortByClause> listOfSortByClauses = new ArrayList<>();

    public SortStep sortStep = null;

    public SortTransformation(DPLParserCatalystContext catCtx, DPLParserCatalystVisitor catVisitor) {
        this.catCtx = catCtx;
        this.catVisitor = catVisitor;
    }

    /**
     * Sets the variable values based on the parameters given in the command, and builds the SortByClauses to be used by
     * the dpf_02 BatchCollect, as the actual sorting happens in BatchCollect
     * 
     * @param ctx SortTransformationContext
     * @return CatalystNode containing result set (same as input)
     */
    @Override
    public Node visitSortTransformation(DPLParser.SortTransformationContext ctx) {
        LOGGER.info("Visiting sortTransformation: <{}>", ctx.getText());

        limit = this.catVisitor.getDPLRecallSize(); // get default sort limit via dpl recall size

        // limit parameter with 'limit=' prefix
        if (ctx.t_sort_limitParameter() != null && !ctx.t_sort_limitParameter().isEmpty()) {
            String limitString = ctx.t_sort_limitParameter(0).integerType().getText();

            Matcher m = Pattern.compile("\\d{1,7}").matcher(limitString);

            if (!m.matches()) {
                throw new IllegalArgumentException("Invalid limit parameter value: " + limitString);
            }

            int limitValue = Integer.parseUnsignedInt(limitString);

            // limit<=0 should be default "no limit" (still limited by dpl recall-size) behavior; only set limit if more than zero
            if (limitValue > 0) {
                limit = limitValue;
            }
        }

        // limit parameter without 'limit=' prefix
        if (ctx.t_sort_integerType() != null && !ctx.t_sort_integerType().isEmpty()) {
            String limitString = ctx.t_sort_integerType(0).getText();

            Matcher m = Pattern.compile("\\d{1,7}").matcher(limitString);

            if (!m.matches()) {
                throw new IllegalArgumentException("Invalid limit parameter value: " + limitString);
            }

            int limitValue = Integer.parseUnsignedInt(limitString);

            // limit<=0 should be default "no limit" (still limited by dpl recall-size) behavior; only set limit if more than zero
            if (limitValue > 0) {
                limit = limitValue;
            }
        }

        // desc parameter (reverse default ordering)
        if (ctx.t_sort_dParameter() != null && !ctx.t_sort_dParameter().isEmpty()) {
            desc = true;
        }

        // auto, str, ip, num, default
        if (ctx.t_sort_sortByClauseInstruction() != null) {
            ctx.t_sort_sortByClauseInstruction().forEach(this::visit);
        }

        // at least one sortByClause needs to be present
        // otherwise throw exception with details
        if (this.listOfSortByClauses.isEmpty()) {
            throw new IllegalArgumentException(
                    "Sort command should contain at least one sortByInstruction. Example: 'sort -_time'"
            );
        }

        this.sortStep = new SortStep(catCtx, listOfSortByClauses, limit, desc);

        LOGGER
                .info(
                        String
                                .format(
                                        "Set sortStep params to: sbc=%s, desc=%s, limit=%s", Arrays
                                                .toString(this.sortStep.getListOfSortByClauses().toArray()),
                                        this.sortStep.isDesc(), this.sortStep.getLimit()
                                )
                );

        return sortTransformationEmitCatalyst(ctx);
    }

    @Override
    public Node visitT_sort_sortByClauseInstruction(DPLParser.T_sort_sortByClauseInstructionContext ctx) {
        if (ctx.sortFieldType() != null) {
            boolean descending = false;

            if (ctx.t_sortMinusOption() != null) {
                TerminalNode plusOrMinus = (TerminalNode) ctx.t_sortMinusOption().getChild(0);
                if (plusOrMinus.getSymbol().getType() == DPLLexer.COMMAND_SORT_MODE_MINUS) {
                    descending = true;
                }
                // plus is descending=false
            }

            String fieldName = ctx.sortFieldType().getText();

            SortByClause sbc = new SortByClause();
            sbc.setLimit(this.limit);
            sbc.setDescending(this.desc != descending);
            sbc.setSortAsType(SortByClause.Type.DEFAULT);
            sbc.setFieldName(fieldName);
            this.listOfSortByClauses.add(sbc);

            return null;
        }
        else {
            return visitChildren(ctx);
        }
    }

    // PLUS (+) is descending=false
    // MINUS (-) is descending=true
    @Override
    public Node visitT_sort_byMethodAuto(DPLParser.T_sort_byMethodAutoContext ctx) {
        boolean descending = false;

        if (ctx.t_sortMinusOption() != null) {
            TerminalNode plusOrMinus = (TerminalNode) ctx.t_sortMinusOption().getChild(0);
            if (plusOrMinus.getSymbol().getType() == DPLLexer.COMMAND_SORT_MODE_MINUS) {
                descending = true;
            }
            // plus is descending=false
        }

        String field = ctx.fieldType().getText();

        SortByClause sbc = new SortByClause();
        sbc.setLimit(this.limit);
        sbc.setDescending(this.desc != descending);
        sbc.setSortAsType(SortByClause.Type.AUTOMATIC);
        sbc.setFieldName(field);
        this.listOfSortByClauses.add(sbc);

        return null;
    }

    @Override
    public Node visitT_sort_byMethodStr(DPLParser.T_sort_byMethodStrContext ctx) {
        boolean descending = false;

        if (ctx.t_sortMinusOption() != null) {
            TerminalNode plusOrMinus = (TerminalNode) ctx.t_sortMinusOption().getChild(0);
            if (plusOrMinus.getSymbol().getType() == DPLLexer.COMMAND_SORT_MODE_MINUS) {
                descending = true;
            }
            // plus is descending=false
        }

        String field = ctx.fieldType().getText();

        SortByClause sbc = new SortByClause();
        sbc.setLimit(this.limit);
        sbc.setDescending(this.desc != descending);
        sbc.setSortAsType(SortByClause.Type.STRING);
        sbc.setFieldName(field);
        this.listOfSortByClauses.add(sbc);

        return null;
    }

    @Override
    public Node visitT_sort_byMethodIp(DPLParser.T_sort_byMethodIpContext ctx) {
        boolean descending = false;

        if (ctx.t_sortMinusOption() != null) {
            TerminalNode plusOrMinus = (TerminalNode) ctx.t_sortMinusOption().getChild(0);
            if (plusOrMinus.getSymbol().getType() == DPLLexer.COMMAND_SORT_MODE_MINUS) {
                descending = true;
            }
            // plus is descending=false
        }

        String field = ctx.fieldType().getText();

        SortByClause sbc = new SortByClause();
        sbc.setLimit(this.limit);
        sbc.setDescending(this.desc != descending);
        sbc.setSortAsType(SortByClause.Type.IP_ADDRESS);
        sbc.setFieldName(field);
        this.listOfSortByClauses.add(sbc);

        return null;
    }

    @Override
    public Node visitT_sort_byMethodNum(DPLParser.T_sort_byMethodNumContext ctx) {
        boolean descending = false;

        if (ctx.t_sortMinusOption() != null) {
            TerminalNode plusOrMinus = (TerminalNode) ctx.t_sortMinusOption().getChild(0);
            if (plusOrMinus.getSymbol().getType() == DPLLexer.COMMAND_SORT_MODE_MINUS) {
                descending = true;
            }
            // plus is descending=false
        }

        String field = ctx.fieldType().getText();

        SortByClause sbc = new SortByClause();
        sbc.setLimit(this.limit);
        sbc.setDescending(this.desc != descending);
        sbc.setSortAsType(SortByClause.Type.NUMERIC);
        sbc.setFieldName(field);
        this.listOfSortByClauses.add(sbc);

        return null;
    }

    /**
     * Pushes the list of sortBy clauses to ProcessingStack, and pushes the dataset to the stack.
     * 
     * @param ctx
     * @return
     */
    private Node sortTransformationEmitCatalyst(DPLParser.SortTransformationContext ctx) {
        return new StepNode(sortStep);
    }
}
