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
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.steps.top.TopStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains the visitor methods for the <code>top</code> command <br>
 * Limits the dataset to n results
 */
public class TopTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopTransformation.class);

    private List<String> traceBuffer = null;
    DPLParserCatalystContext catCtx = null;
    private Map<String, String> symbolTable = new HashMap<>();
    ProcessingStack processingPipe = null;

    public TopStep topStep = null;

    public TopTransformation(DPLParserCatalystContext catCtx, ProcessingStack processingPipe, List<String> buf)
    {
        this.symbolTable = symbolTable;
        this.processingPipe = processingPipe;
        this.traceBuffer = buf;
        this.catCtx = catCtx;
    }

    public Node visitTopTransformation(DPLParser.TopTransformationContext ctx) {
        traceBuffer.add(ctx.getChildCount() + " TopTransformation:" + ctx.getText());
        LOGGER.info(ctx.getChildCount() + " TopTransformation:" + ctx.getText());
        Node rv = topTransformationEmitCatalyst(ctx);
        traceBuffer.add("visitTopTransformation returns:" + rv.toString());

        return rv;
    }


    public Node topTransformationEmitCatalyst(DPLParser.TopTransformationContext ctx) {
        Dataset<Row> rv = null;
        int limit = 10; // Default limit

        if (!this.processingPipe.isEmpty()) {
            rv = processingPipe.pop();
        }
        this.topStep = new TopStep(rv);

        traceBuffer.add(ctx.getChildCount() + " topTransformation:" + ctx.getText());
        // Check limit
        if (ctx.integerType() != null) {
            limit = Integer.parseInt(ctx.integerType().getText());
        }

        List<DPLParser.T_top_topOptParameterContext> opts = ctx.t_top_topOptParameter();
        for (DPLParser.T_top_topOptParameterContext o : opts) {
            if (o.t_top_limitParameter() != null) {
                LOGGER.info("param= " + o.t_top_limitParameter().getChild(1).getText());
                limit = Integer.parseInt(o.t_top_limitParameter().integerType().getText());
            }
        };
        // Get field list
        List<String> fields = null;
        if (ctx.fieldListType() != null) {
            Node ret = visitFieldListType(ctx.fieldListType());
            fields = ((StringListNode)ret).asList();
        }

        // step
        this.topStep.setLimit(limit);
        this.topStep.setListOfFields(fields); //TODO not used
        rv = this.topStep.get();

        // Put back result
        processingPipe.push(rv);
        return new CatalystNode(rv);
    }


    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        List<String> fields = new ArrayList<>();
        ctx.children.forEach(f ->{
            String fieldType =visit(f).toString();
            fields.add(fieldType);
        });
        traceBuffer.add("visitFieldListType:" + fields);
        return new StringListNode(fields);
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        traceBuffer.add("Visit fieldtype:" + ctx.getChild(0).getText());
        // Check if symbol-table has it
        String sql = ctx.getChild(0).getText();
        if (symbolTable != null && symbolTable.containsKey(sql)) {
            sql = symbolTable.get(sql);
        }
        traceBuffer.add("return fieldtype:" + sql);
        return new StringNode(new Token(Type.STRING, sql));
    }
}
