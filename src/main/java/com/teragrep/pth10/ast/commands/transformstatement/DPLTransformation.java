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
import com.teragrep.pth10.ast.TreeUtils;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import com.teragrep.pth10.steps.dpl.AbstractDplStep;
import com.teragrep.pth10.steps.dpl.DplStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Base transformation for DPL command
 * Currently allows only one subcommand, "parsetree"
 */
public class DPLTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLTransformation.class);

    private List<String> traceBuffer = null;
    DPLParserCatalystContext catCtx = null;
    private Map<String, Object> symbolTable = new HashMap<>();
    ProcessingStack processingPipe = null;

    public DplStep dplStep = null;

    public DPLTransformation(DPLParserCatalystContext catCtx, ProcessingStack processingPipe, List<String> buf, Map<String, Object> symbolTable) {
        this.processingPipe = processingPipe;
        this.traceBuffer = buf;
        this.catCtx = catCtx;
        this.symbolTable = symbolTable;
    }

    public Node visitDplTransformation(DPLParser.DplTransformationContext ctx) {
        traceBuffer.add(ctx.getChildCount() + " DPLTransformation:" + ctx.getText());
        LOGGER.info(ctx.getChildCount() + " DPLTransformation:" + ctx.getText());
        Node rv = dplTransformationEmitCatalyst(ctx);
        traceBuffer.add("visitDPLTransformation returns:" + rv.toString());

        return rv;
    }

    public Node dplTransformationEmitCatalyst(DPLParser.DplTransformationContext ctx) {
        Dataset<Row> rv = null;
        if (!this.processingPipe.isEmpty()) {
            rv = processingPipe.pop();
        }
        this.dplStep = new DplStep(rv);

        String explainStr;
        List<String> lines = new ArrayList<>();

        traceBuffer.add(ctx.getChildCount() + " dplTransformation:" + ctx.getText());

        explainStr = "dpl";
        if (ctx.t_dpl_basefilenameParameter() != null) {
            String command = ctx.t_dpl_basefilenameParameter().getChild(1).getText();
            if (!command.equalsIgnoreCase("parsetree")) {
                throw new RuntimeException("dpl command:" + command + " not yet supported");
            }

            explainStr = "dpl debug";
            if (catCtx.getRuleNames() != null) {
                explainStr = "dpl debug subsearch=true";
                ParserRuleContext curr = ctx;

                //  Search root;
                while (curr.getParent() != null) {
                    curr = curr.getParent();
                }

                lines.add(TreeUtils.toPrettyTree(curr.getChild(0), Arrays.asList(catCtx.getRuleNames())));
            }
        }

        if(ctx.t_dpl_subsearchParameter()!= null ){
            //add subsearch result
            if (ctx.t_dpl_subsearchParameter().getChild(1).getText().equalsIgnoreCase("true")) {
                if (symbolTable.containsKey("SubsearchParseTree")) {
                    lines.addAll ((List<String>)symbolTable.get("SubsearchParseTree"));
                }
            }
        }

        // step
        this.dplStep.setCommandType(AbstractDplStep.DplCommandType.PARSETREE); // no other option
        this.dplStep.setExplainStr(explainStr);
        this.dplStep.setLines(lines);
        this.dplStep.setGeneratedDatasource(new GeneratedDatasource(catCtx));
        rv = this.dplStep.get();

        // Put back result
        processingPipe.push(rv);
        return new CatalystNode(rv);
    }
}
