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

import com.teragrep.functions.dpf_02.BatchCollect;
import com.teragrep.functions.dpf_02.SortByClause;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.steps.dedup.DedupStep;
import com.teragrep.pth10.steps.sort.SortStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DedupTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DedupTransformation.class);
    private final ProcessingStack STACK;
    private final DPLParserCatalystContext CAT_CTX;

    private boolean aggregatesUsed = false;
    private boolean isSorted = false;

    public DedupStep dedupStep = null;

    public void setAggregatesUsed(boolean aggregatesUsed) {
        this.aggregatesUsed = aggregatesUsed;
    }

    private boolean isSorted() {
        return isSorted;
    }

    public DedupTransformation(ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.STACK = stack;
        this.CAT_CTX = catCtx;
    }

    @Override
    public Node visitDedupTransformation(DPLParser.DedupTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!this.STACK.isEmpty()) {
            ds = this.STACK.pop();
        }

        int maxDuplicates = 0;  //0;
        boolean keepEmpty = false;  //false;
        boolean keepEvents = false; //false;
        boolean consecutive = false; //false;

        final List<String> listOfFields = visitT_dedup_fieldListParameter(ctx.t_dedup_fieldListParameter()).asList();

        if (ctx.t_dedup_numberParameter() != null) {
            try {
                maxDuplicates = Integer.parseInt(ctx.t_dedup_numberParameter().getText());
            }
            catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Invalid limit parameter value. It must be larger or equal to 1," +
                        "however within the limits of an IntegerType.");
            }

            if (maxDuplicates < 1) {
                throw new IllegalArgumentException("Dedup error: Limit parameter must be larger or equal to 1.");
            }
        }

        if (ctx.t_dedup_consecutiveParameter() != null && !ctx.t_dedup_consecutiveParameter().isEmpty()) {
            consecutive = ctx.t_dedup_consecutiveParameter(0).getChild(1).getText().trim().equalsIgnoreCase("true");
        }

        if (ctx.t_dedup_keepemptyParameter() != null && !ctx.t_dedup_keepemptyParameter().isEmpty()) {
            keepEmpty = ctx.t_dedup_keepemptyParameter(0).getChild(1).getText().trim().equalsIgnoreCase("true");
        }

        if (ctx.t_dedup_keepeventsParameter() != null && !ctx.t_dedup_keepeventsParameter().isEmpty()) {
            keepEvents = ctx.t_dedup_keepeventsParameter(0).getChild(1).getText().trim().equalsIgnoreCase("true");
        }

        // only one sortByInstruction, but can have multiple t_dedup_sortbyMethodXXX as children
        if (ctx.t_dedup_sortbyInstruction() != null) {
            DPLParser.T_dedup_sortbyInstructionContext sortByInstCtx = ctx.t_dedup_sortbyInstruction();

            SortByClause sbc = null;
            List<SortByClause> listOfSortByClauses = new ArrayList<>();

            for (int i = 0; i < sortByInstCtx.getChildCount(); i++) {
                ParseTree child = sortByInstCtx.getChild(i);


                if (child instanceof DPLParser.T_dedup_sortOrderContext) {
                    if (sbc != null) {
                        // add previous (if any) sortByClause to list
                        listOfSortByClauses.add(sbc);
                    }
                    sbc = new SortByClause();
                    sbc.setDescending(((DPLParser.T_dedup_sortOrderContext) child).COMMAND_DEDUP_MODE_PLUS() == null);
                }
                else if (child instanceof DPLParser.T_dedup_sortbyMethodAutoContext) {
                    assert sbc != null : "Sort by method AUTO expected a sort by clause, instead was null";
                    sbc.setSortAsType(SortByClause.Type.AUTOMATIC);
                    sbc.setFieldName(((DPLParser.T_dedup_sortbyMethodAutoContext) child).fieldType().getText());
                }
                else if (child instanceof DPLParser.T_dedup_sortbyMethodIpContext) {
                    assert sbc != null : "Sort by method IP expected a sort by clause, instead was null";
                    sbc.setSortAsType(SortByClause.Type.IP_ADDRESS);
                    sbc.setFieldName(((DPLParser.T_dedup_sortbyMethodIpContext) child).fieldType().getText());
                }
                else if (child instanceof DPLParser.T_dedup_sortbyMethodNumContext) {
                    assert sbc != null : "Sort by method NUM expected a sort by clause, instead was null";
                    sbc.setSortAsType(SortByClause.Type.NUMERIC);
                    sbc.setFieldName(((DPLParser.T_dedup_sortbyMethodNumContext) child).fieldType().getText());
                }
                else if (child instanceof DPLParser.T_dedup_sortbyMethodStrContext) {
                    assert sbc != null: "Sort by method STR expected a sort by clause, instead was null";
                    sbc.setSortAsType(SortByClause.Type.STRING);
                    sbc.setFieldName(((DPLParser.T_dedup_sortbyMethodStrContext) child).fieldType().getText());
                }
            }

            // the final sortBy clause needs to be added here,
            // as they get added only when a next one is started to be processed in the loop above
            listOfSortByClauses.add(sbc);

            SortStep sortStep = new SortStep(ds);
            sortStep.setListOfSortByClauses(listOfSortByClauses);
            sortStep.setLimit(this.STACK.getCatVisitor().getDPLRecallSize());
            sortStep.setDesc(false); // no support for desc in dedup
            sortStep.setAggregatesUsed(this.aggregatesUsed);

            final String sortBcId = Util.getObjectIdentifier(ctx.t_dedup_sortbyInstruction());
            LOGGER.info("sortCtxId:" + sortBcId);
            sortStep.setSortingBatchCollect((BatchCollect) this.CAT_CTX.getObjectStore().get(sortBcId));

            LOGGER.info(String.format("Processing sortByClauses in dedup with params: sbc=%s, limit=%s, desc=%s, aggsUsed=%s",
                            Arrays.toString(sortStep.getListOfSortByClauses().toArray()), sortStep.getLimit(), sortStep.isDesc(), sortStep.isAggregatesUsed()));

            ds = sortStep.get();

            this.isSorted = true; // Sort uses batchCollect internally
            this.CAT_CTX.getObjectStore().add(sortBcId, sortStep.getSortingBatchCollect());
        }

        // initialize dedupStep here, so the sorted ds will be used if it was set
        this.dedupStep = new DedupStep(ds);

        //convert to objectStore:
        final String id = Util.getObjectIdentifier(ctx);
        LOGGER.info("dedupId= " + id);
        if (!this.CAT_CTX.getObjectStore().has(id) || isSorted()) {
            LOGGER.info("Such id not found - init dedup map");
            this.CAT_CTX.getObjectStore().add(id, Collections.synchronizedMap(new ConcurrentHashMap<>()));
        }

       /* if (this.CAT_CTX.getDedupFieldsProcessed() == null) {
            this.CAT_CTX.setDedupFieldsProcessed(Collections.synchronizedMap(new ConcurrentHashMap<>()));
        }*/

        this.dedupStep.setListOfFields(listOfFields);
        //this.dedupStep.setFieldsProcessed(this.CAT_CTX.getDedupFieldsProcessed());

        this.dedupStep.setFieldsProcessed((Map<String, Map<String, Long>>) this.CAT_CTX.getObjectStore().get(id));
        this.dedupStep.setMaxDuplicates(maxDuplicates);
        this.dedupStep.setKeepEmpty(keepEmpty);
        this.dedupStep.setKeepEvents(keepEvents);
        this.dedupStep.setConsecutive(consecutive);

        LOGGER.info(String.format("Processing dedup with params: limit=%s, keepempty=%s, keepevents=%s, consecutive=%s, cols=%s",
                maxDuplicates, keepEmpty, keepEvents, consecutive, Arrays.toString(listOfFields.toArray())));

        ds = this.dedupStep.get();
        //this.CAT_CTX.setDedupFieldsProcessed(this.dedupStep.getFieldsProcessed());
        this.CAT_CTX.getObjectStore().add(id, this.dedupStep.getFieldsProcessed());
        this.STACK.setSorted(this.isSorted());
        this.STACK.push(ds);
        return new CatalystNode(ds);
    }

    public StringListNode visitT_dedup_fieldListParameter(DPLParser.T_dedup_fieldListParameterContext ctx) {
        List<String> fields = new ArrayList<>();

        ctx.fieldType().forEach(ftCtx -> fields.add(ftCtx.getText()));

        return new StringListNode(fields);
    }
}
