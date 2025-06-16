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
import com.teragrep.pth10.ast.PrettyTree;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import com.teragrep.pth10.steps.dpl.AbstractDplStep;
import com.teragrep.pth10.steps.dpl.DplStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.ParserRuleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Base transformation for DPL command Currently allows only one subcommand, "parsetree"
 */
public class DPLTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLTransformation.class);
    DPLParserCatalystContext catCtx = null;

    public DplStep dplStep = null;

    public DPLTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public Node visitDplTransformation(DPLParser.DplTransformationContext ctx) {
        LOGGER.info("DPLTransformation: children=<{}> text=<{}>", ctx.getChildCount(), ctx.getText());

        return dplTransformationEmitCatalyst(ctx);
    }

    public Node dplTransformationEmitCatalyst(DPLParser.DplTransformationContext ctx) {
        this.dplStep = new DplStep();

        String explainStr;
        List<String> lines = new ArrayList<>();

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

                PrettyTree prettyTree = new PrettyTree(curr.getChild(0), Arrays.asList(catCtx.getRuleNames()));
                lines.add(prettyTree.getTree());
            }
        }

        // FIXME: This has never been functional, seemingly it was supposed to print subsearch parse tree.
        /* if(ctx.t_dpl_subsearchParameter()!= null ){
            //add subsearch result
            if (ctx.t_dpl_subsearchParameter().getChild(1).getText().equalsIgnoreCase("true")) {
                if (symbolTable.containsKey("SubsearchParseTree")) {
                    lines.addAll ((List<String>)symbolTable.get("SubsearchParseTree"));
                }
            }
        }*/

        // step
        this.dplStep.setCommandType(AbstractDplStep.DplCommandType.PARSETREE); // no other option
        this.dplStep.setExplainStr(explainStr);
        this.dplStep.setLines(lines);
        this.dplStep.setGeneratedDatasource(new GeneratedDatasource(catCtx));

        return new StepNode(this.dplStep);
    }
}
