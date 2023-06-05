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
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.steps.table.TableStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base transformation class for the command <code>table</code><br>
 * Used to generate a table with the same order and fields as given in the command
 */
public class TableTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableTransformation.class);
    private DPLParserCatalystContext catCtx = null;
    private ProcessingStack processingStack = null;
    private boolean aggregatesUsed = false;
    List<String> fieldList = null;
    public TableStep tableStep = null;

    public TableTransformation(ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
        this.processingStack = stack;
    }

    public void setAggregatesUsed(boolean aggregatesUsed) {
        this.aggregatesUsed = aggregatesUsed;
    }

    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    @Override
    public Node visitTableTransformation(DPLParser.TableTransformationContext ctx) {
        StringListNode fieldListNode = (StringListNode) visit(ctx.t_table_wcfieldListParameter());
        fieldList = fieldListNode.asList();

        return tableTransformationEmitCatalyst(ctx);
    }

    private Node tableTransformationEmitCatalyst(DPLParser.TableTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!processingStack.isEmpty()) {
            ds = processingStack.pop();
        }

        tableStep = new TableStep(ds);
        tableStep.setListOfFields(this.fieldList);
        ds = tableStep.get();

        processingStack.push(ds);
        return new CatalystNode(ds);
    }

    @Override
    public Node visitT_table_wcfieldListParameter(DPLParser.T_table_wcfieldListParameterContext ctx) {
        List<String> listOfFields = new ArrayList<>();

        ctx.t_table_fieldType().forEach(fieldType -> {
            String fieldName = ((StringNode)visit(fieldType)).toString();

            if (!fieldName.equals("")) {
                listOfFields.addAll(Arrays.asList(fieldName.split(",")));
            }
        });

        return new StringListNode(listOfFields);
    }

    @Override
    public Node visitT_table_fieldType(DPLParser.T_table_fieldTypeContext ctx) {
        String fieldName = "";

        if (ctx.t_table_stringType() != null) {
            fieldName = Util.stripQuotes(ctx.t_table_stringType().getText());
        }

        return new StringNode(new Token(Token.Type.STRING, fieldName));
    }


}
