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

import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.steps.regex.RegexStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base visitor class for command <code>regex</code>
 */
public class RegexTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegexTransformation.class);
    public RegexStep regexStep = null;

    public RegexTransformation() {

    }

    /*
    (root
    (searchTransformationRoot (logicalStatement (searchQualifier index = (stringType index_A))))
    (transformStatement | (regexTransformation regex (fieldType _raw) != (regexStringType "data data")) (transformStatement <EOF>)))
    
    (root
    (searchTransformationRoot (logicalStatement (searchQualifier index = (stringType index_A))))
    (transformStatement | (regexTransformation regex (fieldType _raw) = (regexStringType "data data")) (transformStatement <EOF>)))
     */

    @Override
    public Node visitRegexTransformation(DPLParser.RegexTransformationContext ctx) {
        regexStep = new RegexStep();
        // 0 regex 1 fieldType 2 EQ/NEQ 3 regexStringType
        // -- OR --
        // 0 regex 1 regexStringType

        boolean equals = true;
        String fromField = "_raw";
        String regexStatement = null;

        // field on the left side of the EQ/NEQ sign (or omitted)
        if (ctx.fieldType() != null) {
            fromField = ctx.fieldType().getText();
        }

        // field on the right side of the EQ/NEQ sign or as the single parameter
        if (ctx.regexStringType() != null) {
            regexStatement = ctx.regexStringType().getText();
        }

        // Check for EQ/NEQ sign
        if (ctx.getChild(2) != null) {
            ParseTree eq = ctx.getChild(2);
            LOGGER.debug(eq.getText());
            if (eq instanceof TerminalNode) {
                if (((TerminalNode) eq).getSymbol().getType() == DPLLexer.COMMAND_REGEX_MODE_EQ) {
                    equals = true;
                }
                else if (((TerminalNode) eq).getSymbol().getType() == DPLLexer.COMMAND_REGEX_MODE_NEQ) {
                    equals = false;
                }
            }
        }

        // FIXME (root (searchTransformationRoot
        //  (logicalStatement (searchQualifier index = (stringType index_A))))
        //  (transformStatement | (regexTransformation regex "data data") (transformStatement <EOF>)))
        // --> regexStatement does not get parsed correctly without fieldType
        if (regexStatement == null) {
            regexStatement = ctx.getChild(1).getText();
        }

        LOGGER.debug("from=<{}> regex=<{}> equals=<{}>", fromField, regexStatement, equals);

        regexStep.setRegexString(regexStatement);
        regexStep.setEquals(equals);
        regexStep.setFromField(fromField);

        return new StepNode(regexStep);
    }
}
