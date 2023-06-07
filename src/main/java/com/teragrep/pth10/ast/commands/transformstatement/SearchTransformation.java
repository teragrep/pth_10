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

import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.commands.logicalstatement.LogicalStatement;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Base transformation for the 'search' command
 * <pre>| 'search' logicalStatement </pre>
 */
public class SearchTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchTransformation.class);

    private List<String> traceBuffer = null;
    private Map<String, Object> symbolTable;
    ProcessingStack processingPipe = null;

    public SearchTransformation(Map<String, Object> symbolTable, List<String> buf, ProcessingStack stack)
    {
        this.symbolTable = symbolTable;
        this.processingPipe = stack;
        this.traceBuffer = buf;
    }

    public Node visitSearchTransformation(DPLParser.SearchTransformationContext ctx) {
        traceBuffer.add(ctx.getChildCount() + " SearchTransformation: " + ctx.getText());

        Node rv = searchTransformationEmitCatalyst(ctx);

        LOGGER.info("searchTransformation rvType: " + rv.getClass().getName());
        traceBuffer.add("visitSearchTransformation returns: " + rv);
        return rv;
    }

    public Node searchTransformationEmitCatalyst(DPLParser.SearchTransformationContext ctx) {
        LOGGER.info(">> Entering searchTransformation with: " + ctx.getText());
        Dataset<Row> rv;

        // '| search <searchTransformationStatement> | ...'
        DPLParser.SearchTransformationRootContext searchRootCtx = ctx.searchTransformationRoot();

        if (searchRootCtx != null) {
            // isSearchCommand=true is used to skip generating archiveQuery as 'search' is used to filter existing dataset
            // rather than getting a new one and filtering it
            LogicalStatement logiStat = new LogicalStatement(this.processingPipe, this.processingPipe.getCatVisitor().getCatalystContext(), true);
            logiStat.visitSearchTransformationRoot(searchRootCtx);
        }
        else {
            throw new IllegalStateException("Invalid search command. Expected SearchTransformationRoot, instead got '" + ctx.getText() + "'");
        }

        // pop & push stack to make catalystNode
        rv = processingPipe.pop();
        processingPipe.push(rv);
        return new CatalystNode(rv);
    }
}
