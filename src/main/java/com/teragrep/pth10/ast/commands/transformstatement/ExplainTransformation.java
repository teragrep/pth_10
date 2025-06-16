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
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import com.teragrep.pth10.steps.explain.AbstractExplainStep;
import com.teragrep.pth10.steps.explain.ExplainStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base transformation class for explain command. Allows to view the Spark physical plan of the dataset
 */
public class ExplainTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainTransformation.class);
    DPLParserCatalystContext catCtx = null;

    public ExplainStep explainStep = null;

    public ExplainTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public Node visitExplainTransformation(DPLParser.ExplainTransformationContext ctx) {
        LOGGER.info("ExplainTransformation incoming: children=<{}> text=<{}>", ctx.getChildCount(), ctx.getText());

        return explainTransformationEmitCatalyst(ctx);
    }

    /**
     * Gets the physical plan and puts in into a dataset to view
     * 
     * @param ctx explainTransformationContext
     * @return catalystnode containing result dataset
     */
    public Node explainTransformationEmitCatalyst(DPLParser.ExplainTransformationContext ctx) {
        this.explainStep = new ExplainStep();

        // Default mode is brief
        // just physical plan
        AbstractExplainStep.ExplainMode explainMode = AbstractExplainStep.ExplainMode.BRIEF;
        if (ctx.t_explain_extendedOption() != null) {
            // physical + logical
            explainMode = AbstractExplainStep.ExplainMode.EXTENDED;
        }

        if (ctx.t_explain_codegenOption() != null) {
            throw new RuntimeException("Current version does not support 'Codegen' option.");
        }

        if (ctx.t_explain_costOption() != null) {
            throw new RuntimeException("Current version does not support 'Cost' option.");
        }

        if (ctx.t_explain_formattedOption() != null) {
            throw new RuntimeException("Current version does not support 'Formatted' option.");
        }

        GeneratedDatasource datasource = new GeneratedDatasource(catCtx);

        // step
        this.explainStep.setMode(explainMode);
        this.explainStep.setGeneratedDatasource(datasource);

        return new StepNode(explainStep);
    }

}
