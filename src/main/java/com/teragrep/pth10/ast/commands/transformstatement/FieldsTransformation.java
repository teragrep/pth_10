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
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.steps.fields.AbstractFieldsStep;
import com.teragrep.pth10.steps.fields.FieldsStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Base transformation class for the fields command. Allows the user to decide, which fields to retain or drop from the
 * result set.
 */
public class FieldsTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldsTransformation.class);
    DPLParserCatalystContext catCtx = null;
    public FieldsStep fieldsStep = null;

    public FieldsTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public Node visitFieldsTransformation(DPLParser.FieldsTransformationContext ctx) {
        return fieldsTransformationEmitCatalyst(ctx);
    }

    public Node fieldsTransformationEmitCatalyst(DPLParser.FieldsTransformationContext ctx) {
        this.fieldsStep = new FieldsStep();

        String oper = ctx.getChild(1).getText();

        if ("-".equals(oper)) {
            StringListNode sln = (StringListNode) visit(ctx.fieldListType());
            LOGGER.debug("Drop fields: stringListNode=<{}>", sln);

            this.fieldsStep.setMode(AbstractFieldsStep.FieldMode.REMOVE_FIELDS);
            this.fieldsStep.setListOfFields(sln.asList());
        }
        else {
            StringListNode sln = (StringListNode) visit(ctx.fieldListType());
            this.fieldsStep.setMode(AbstractFieldsStep.FieldMode.KEEP_FIELDS);
            this.fieldsStep.setListOfFields(sln.asList());
        }
        return new StepNode(fieldsStep);
    }

    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        List<String> fields = new ArrayList<>();
        ctx.children
                .forEach(
                        f -> {
                            // skip non-fieldType children
                            if (f instanceof DPLParser.FieldTypeContext) {
                                String fieldType = visit(f).toString();
                                fields.add(fieldType);
                            }
                        }
                );
        return new StringListNode(fields);
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        String sql = ctx.getChild(0).getText();
        return new StringNode(new Token(Type.STRING, sql));
    }
}
