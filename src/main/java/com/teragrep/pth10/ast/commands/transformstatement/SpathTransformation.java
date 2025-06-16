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
import com.teragrep.pth10.steps.spath.SpathStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base transformation class for the command <code>spath</code><br>
 * Allows the user to extract data from JSON or XML data formats using an spath / xpath expression.
 * <pre>spath input=... output=... path=...|...</pre> Defaults: <pre>spath input=_raw output=path path=...</pre> Path
 * omitted -&gt; auto-extract mode: extracts all fields from the first 5000 characters in the input field.
 */
public class SpathTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpathTransformation.class);
    private final DPLParserCatalystContext catCtx;
    public SpathStep spathStep = null;

    public SpathTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    @Override
    public Node visitSpathTransformation(DPLParser.SpathTransformationContext ctx) {
        this.spathStep = new SpathStep();

        String inputCol = "_raw";
        String outputCol = null;
        String path = null;
        boolean autoExtractionMode = true;

        // Go through parameters
        if (ctx.t_spath_inputParameter(0) != null) {
            inputCol = ctx.t_spath_inputParameter(0).getChild(1).getText();
        }

        if (ctx.t_spath_pathParameter(0) != null) {
            path = ((StringNode) visit(ctx.t_spath_pathParameter(0))).toString();
            autoExtractionMode = false;
        }

        if (ctx.t_spath_outputParameter(0) != null) {
            outputCol = ctx.t_spath_outputParameter(0).getChild(1).getText();
            assert !autoExtractionMode : "Output cannot be specified when using auto-extraction mode.";
        }
        else {
            if (path != null) {
                outputCol = path;
            }
            else { // if path is null, use auto-extract mode
                autoExtractionMode = true;
                outputCol = "$$dpl_pth10_internal_column_spath_output$$";
            }
        }

        LOGGER.debug("input=<[{}]>, output=<[{}]>, path=<[{}]>", inputCol, outputCol, path);

        // set parameters for spathStep and use get() to perform the action
        this.spathStep.setPath(path);
        this.spathStep.setInputColumn(inputCol);
        this.spathStep.setOutputColumn(outputCol);
        this.spathStep.setAutoExtractionMode(autoExtractionMode);
        this.spathStep.setCatCtx(catCtx);

        return new StepNode(spathStep);
    }

    @Override
    public Node visitT_spath_pathParameter(DPLParser.T_spath_pathParameterContext ctx) {
        String path = "";

        if (ctx.stringType() != null) {
            path = ctx.stringType().getText();
        }

        return new StringNode(new Token(Token.Type.STRING, path));
    }
}
