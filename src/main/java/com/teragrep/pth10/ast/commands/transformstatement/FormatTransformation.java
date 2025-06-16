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

import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.ast.bo.StringNode;
import com.teragrep.pth10.ast.bo.Token;
import com.teragrep.pth10.steps.format.FormatStep;
import com.teragrep.pth_03.antlr.DPLParser;

import java.util.ArrayList;
import java.util.List;

public class FormatTransformation {

    public FormatTransformation() {

    }

    public Node visitFormatTransformation(DPLParser.FormatTransformationContext ctx) {
        // Row prefix, Col prefix, Col sep, Col suffix, Row sep and Row suffix.
        int maxResults = 0;
        String mvSeparator = "OR";
        List<String> rowAndColumnOpts = new ArrayList<>();

        if (ctx.t_format_maxresultsParameter() != null) {
            maxResults = Integer
                    .parseInt(visitT_format_maxresultsParameter(ctx.t_format_maxresultsParameter()).toString());
        }

        if (ctx.t_format_mvSeparatorParameter() != null) {
            mvSeparator = visitT_format_mvSeparatorParameter(ctx.t_format_mvSeparatorParameter()).toString();
        }

        if (ctx.stringType() != null && !ctx.stringType().isEmpty()) {
            if (ctx.stringType().size() == 6) {
                ctx
                        .stringType()
                        .forEach(strCtx -> rowAndColumnOpts.add(new UnquotedText(new TextString(strCtx.getText())).read()));
            }
            else {
                throw new IllegalArgumentException(
                        "All of the row and column options must be specified in the command: "
                                + "Row prefix, Column prefix, Column separator, Column suffix, Row separator and Row suffix. Only "
                                + ctx.stringType().size() + " out of 6 options were provided."
                );
            }
        }

        FormatStep formatStep = new FormatStep();
        if (rowAndColumnOpts.size() == 6) {
            formatStep.setRowPrefix(rowAndColumnOpts.get(0));
            formatStep.setColPrefix(rowAndColumnOpts.get(1));
            formatStep.setColSep(rowAndColumnOpts.get(2));
            formatStep.setColSuffix(rowAndColumnOpts.get(3));
            formatStep.setRowSep(rowAndColumnOpts.get(4));
            formatStep.setRowSuffix(rowAndColumnOpts.get(5));
        }
        formatStep.setMaxResults(maxResults);
        formatStep.setMvSep(mvSeparator);

        return new StepNode(formatStep);
    }

    public Node visitT_format_maxresultsParameter(DPLParser.T_format_maxresultsParameterContext ctx) {
        if (ctx.integerType() != null) {
            return new StringNode(new Token(Token.Type.STRING, ctx.integerType().getText()));
        }
        return new StringNode(new Token(Token.Type.STRING, "0"));
    }

    public Node visitT_format_mvSeparatorParameter(DPLParser.T_format_mvSeparatorParameterContext ctx) {
        if (ctx.stringType() != null) {
            return new StringNode(new Token(Token.Type.STRING, ctx.stringType().getText()));
        }
        return new StringNode(new Token(Token.Type.STRING, "OR"));
    }
}
