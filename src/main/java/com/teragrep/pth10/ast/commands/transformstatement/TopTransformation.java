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
import com.teragrep.pth10.steps.top.TopStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains the visitor methods for the <code>top</code> command <br>
 * Limits the dataset to n results
 */
public class TopTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopTransformation.class);

    DPLParserCatalystContext catCtx = null;

    public TopStep topStep = null;

    public TopTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public Node visitTopTransformation(DPLParser.TopTransformationContext ctx) {
        LOGGER.info("TopTransformation incoming: children=<{}> text=<{}>", ctx.getChildCount(), ctx.getText());
        return topTransformationEmitCatalyst(ctx);
    }

    public Node topTransformationEmitCatalyst(DPLParser.TopTransformationContext ctx) {
        int limit = 10; // Default limit

        this.topStep = new TopStep();

        // Check limit
        if (ctx.integerType() != null) {
            limit = Integer.parseInt(ctx.integerType().getText());
        }

        List<DPLParser.T_top_topOptParameterContext> opts = ctx.t_top_topOptParameter();
        for (DPLParser.T_top_topOptParameterContext o : opts) {
            if (o.t_top_limitParameter() != null) {
                LOGGER.info("param= <{}>", o.t_top_limitParameter().getChild(1).getText());
                limit = Integer.parseInt(o.t_top_limitParameter().integerType().getText());
            }
        }
        ;
        // Get field list
        List<String> fields = null;
        if (ctx.fieldListType() != null) {
            Node ret = visitFieldListType(ctx.fieldListType());
            fields = ((StringListNode) ret).asList();
        }

        // step
        this.topStep.setLimit(limit);
        this.topStep.setListOfFields(fields); //TODO not used

        return new StepNode(topStep);
    }

    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        List<String> fields = new ArrayList<>();
        ctx.children.forEach(f -> {
            String fieldType = visit(f).toString();
            fields.add(fieldType);
        });
        return new StringListNode(fields);
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        String sql = ctx.getChild(0).getText();
        return new StringNode(new Token(Type.STRING, sql));
    }
}
