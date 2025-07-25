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
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepListNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.ast.bo.StringListNode;
import com.teragrep.pth10.steps.dedup.DedupStep;
import com.teragrep.pth10.steps.sort.SortStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DedupTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DedupTransformation.class);
    private final DPLParserCatalystContext catCtx;

    public DedupStep dedupStep = null;

    public DedupTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    @Override
    public Node visitDedupTransformation(DPLParser.DedupTransformationContext ctx) {
        SortStep sortStep = null;

        int maxDuplicates = 1;
        boolean keepEmpty = false;
        boolean keepEvents = false;
        boolean consecutive = false;

        final List<String> listOfFields = visitT_dedup_fieldListParameter(ctx.t_dedup_fieldListParameter()).asList();

        if (ctx.t_dedup_numberParameter() != null) {
            try {
                maxDuplicates = Integer.parseInt(ctx.t_dedup_numberParameter().getText());
            }
            catch (NumberFormatException nfe) {
                throw new IllegalArgumentException(
                        "Invalid limit parameter value. It must be larger or equal to 1,"
                                + "however within the limits of an IntegerType."
                );
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
                    assert sbc != null : "Sort by method STR expected a sort by clause, instead was null";
                    sbc.setSortAsType(SortByClause.Type.STRING);
                    sbc.setFieldName(((DPLParser.T_dedup_sortbyMethodStrContext) child).fieldType().getText());
                }
            }

            // the final sortBy clause needs to be added here,
            // as they get added only when a next one is started to be processed in the loop above
            listOfSortByClauses.add(sbc);

            sortStep = new SortStep(catCtx, listOfSortByClauses, this.catCtx.getDplRecallSize(), false); // no support for desc in dedup

            LOGGER
                    .info(
                            "Processing sortByClauses in dedup with params: sbc={}, limit={}, desc={}", Arrays
                                    .toString(sortStep.getListOfSortByClauses().toArray()),
                            sortStep.getLimit(), sortStep.isDesc()
                    );

        }

        // initialize dedupStep here, so the sorted ds will be used if it was set
        this.dedupStep = new DedupStep(
                listOfFields,
                maxDuplicates,
                keepEmpty,
                keepEvents,
                consecutive,
                catCtx.nullValue,
                sortStep != null
        );

        LOGGER
                .info(
                        "Processing dedup with params: limit={}, keepempty={}, keepevents={}, consecutive={}, cols={}",
                        maxDuplicates, keepEmpty, keepEvents, consecutive, Arrays.toString(listOfFields.toArray())
                );

        // only return StepListNode if sort is used as they're two separate step objects (dedup & sort)
        if (sortStep != null) {
            return new StepListNode(Arrays.asList(sortStep, this.dedupStep));
        }
        else {
            return new StepNode(this.dedupStep);
        }
    }

    public StringListNode visitT_dedup_fieldListParameter(DPLParser.T_dedup_fieldListParameterContext ctx) {
        List<String> fields = new ArrayList<>();

        ctx.fieldType().forEach(ftCtx -> fields.add(ftCtx.getText()));

        return new StringListNode(fields);
    }
}
