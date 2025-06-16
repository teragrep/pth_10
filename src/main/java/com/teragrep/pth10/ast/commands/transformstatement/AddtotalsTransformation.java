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
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.NullNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.steps.addtotals.AddtotalsStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;

import java.util.ArrayList;
import java.util.List;

public class AddtotalsTransformation extends DPLParserBaseVisitor<Node> {

    private final DPLParserCatalystContext catCtx;
    private AddtotalsStep addtotalsStep;
    private boolean row;
    private boolean col;
    private List<String> fieldList;
    private String label;
    private String labelField;
    private String fieldName;

    public AddtotalsTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
        this.row = true;
        this.col = false;
        this.fieldList = new ArrayList<>();
        this.labelField = "";
        this.label = "Total";
        this.fieldName = "Total";
    }

    @Override
    public Node visitAddtotalsTransformation(DPLParser.AddtotalsTransformationContext ctx) {
        visitChildren(ctx);
        addtotalsStep = new AddtotalsStep(catCtx, row, col, fieldName, labelField, label, fieldList);
        return new StepNode(addtotalsStep);
    }

    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        final List<String> listOfFields = new ArrayList<>();
        ctx.fieldType().forEach(f -> listOfFields.add(new UnquotedText(new TextString(f.getText())).read()));
        fieldList = listOfFields;
        return new NullNode();
    }

    @Override
    public Node visitT_addtotals_rowParameter(DPLParser.T_addtotals_rowParameterContext ctx) {
        if (ctx.booleanType().GET_BOOLEAN_FALSE() != null) {
            row = false;
        }
        else if (ctx.booleanType().GET_BOOLEAN_TRUE() != null) {
            row = true;
        }
        else {
            throw new IllegalArgumentException("Unexpected 'row' booleanType: " + ctx.getText());
        }
        return new NullNode();
    }

    @Override
    public Node visitT_addtotals_colParameter(DPLParser.T_addtotals_colParameterContext ctx) {
        if (ctx.booleanType().GET_BOOLEAN_FALSE() != null) {
            col = false;
        }
        else if (ctx.booleanType().GET_BOOLEAN_TRUE() != null) {
            col = true;
        }
        else {
            throw new IllegalArgumentException("Unexpected 'col' booleanType: " + ctx.getText());
        }
        return new NullNode();
    }

    @Override
    public Node visitT_addtotals_fieldnameParameter(DPLParser.T_addtotals_fieldnameParameterContext ctx) {
        fieldName = new UnquotedText(new TextString(ctx.fieldType().getText())).read();
        return new NullNode();
    }

    @Override
    public Node visitT_addtotals_labelfieldParameter(DPLParser.T_addtotals_labelfieldParameterContext ctx) {
        labelField = new UnquotedText(new TextString(ctx.fieldType().getText())).read();
        return new NullNode();
    }

    @Override
    public Node visitT_addtotals_labelParameter(DPLParser.T_addtotals_labelParameterContext ctx) {
        label = new UnquotedText(new TextString(ctx.stringType().getText())).read();
        return new NullNode();
    }
}
