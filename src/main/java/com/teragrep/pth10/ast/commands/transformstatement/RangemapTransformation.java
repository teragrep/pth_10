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
import com.teragrep.pth10.ast.bo.NullNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.steps.rangemap.RangemapStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class RangemapTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RangemapTransformation.class);
    private final RangemapStep rangemapStep;

    public RangemapTransformation() {
        this.rangemapStep = new RangemapStep();
        this.rangemapStep.attributeRangeMap = new HashMap<>();
        this.rangemapStep.defaultValue = "None";
    }

    public Node visitRangemapTransformation(DPLParser.RangemapTransformationContext ctx) {
        LOGGER.info("Visit RangemapTransformation");
        visitChildren(ctx);
        return new StepNode(rangemapStep);
    }

    @Override
    public Node visitT_rangemap_fieldParameter(DPLParser.T_rangemap_fieldParameterContext ctx) {
        if (ctx.fieldType() != null) {
            this.rangemapStep.sourceField = new UnquotedText(new TextString(ctx.fieldType().getText())).read();
        }
        return new NullNode();
    }

    @Override
    public Node visitT_rangemap_attrnParameter(DPLParser.T_rangemap_attrnParameterContext ctx) {
        final String key = ctx.stringType().getText();
        String valueLeft = ctx.t_rangemap_rangeParameter().GET_RANGE_NUMBER_LEFT().getText();
        final String valueRight = ctx
                .t_rangemap_rangeParameter()
                .t_rangemap_rangeRightParameter()
                .GET_RANGE_NUMBER_RIGHT()
                .getText();

        // left side of range contains a trailing '-' character which needs to be removed
        if (valueLeft.endsWith("-")) {
            valueLeft = valueLeft.substring(0, valueLeft.length() - 1);
        }

        this.rangemapStep.attributeRangeMap.put(key, new String[] {
                valueLeft, valueRight
        });
        return new NullNode();
    }

    @Override
    public Node visitT_rangemap_defaultParameter(DPLParser.T_rangemap_defaultParameterContext ctx) {
        this.rangemapStep.defaultValue = new UnquotedText(new TextString(ctx.stringType().getText())).read();
        return new NullNode();
    }
}
