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
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.steps.rex.RexStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RexTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RexTransformation.class);
    private ProcessingStack stack;
    private DPLParserCatalystContext catCtx;

    public RexStep rexStep = null;

    public RexTransformation(ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.stack = stack;
        this.catCtx = catCtx;
    }

    @Override
    public Node visitRexTransformation(DPLParser.RexTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!this.stack.isEmpty()) {
            ds = this.stack.pop();
        }
        this.rexStep = new RexStep(ds);

        String regexStr;
        String fieldParam = "_raw";
        int maxMatch = -1;
        boolean sedMode = false;
        String offsetFieldParam = null;

        if (ctx.regexStringType() != null) {
            regexStr = Util.stripQuotes(ctx.regexStringType().getText());
        }
        else {
            throw new IllegalArgumentException("Either a sed-style string or a regex extraction string is required to be provided in the command, depending on the selected mode.");
        }

        if (ctx.t_rex_fieldParameter() != null) {
            fieldParam = Util.stripQuotes(ctx.t_rex_fieldParameter().fieldType().getText());
        }

        if (ctx.t_rex_modeSedParameter() != null) {
            if (ctx.t_rex_modeSedParameter().COMMAND_REX_MODE_SED() != null) {
                sedMode = true;
            }
        }

        if (ctx.t_rex_maxMatchParameter() != null) {
            maxMatch = Integer.parseInt(ctx.t_rex_maxMatchParameter().integerType().getText());
        }

        if (ctx.t_rex_offsetFieldParameter() != null) {
            offsetFieldParam = Util.stripQuotes(ctx.t_rex_offsetFieldParameter().stringType().getText());
        }

        LOGGER.info("regexStr= " + regexStr);
        LOGGER.info("field= " + fieldParam);
        LOGGER.info("offsetField= " + offsetFieldParam);
        LOGGER.info("maxMatch= " + maxMatch);
        LOGGER.info("sedMode= " + sedMode);

        this.rexStep.setField(fieldParam);
        this.rexStep.setMaxMatch(maxMatch);
        this.rexStep.setOffsetField(offsetFieldParam);
        this.rexStep.setRegexStr(regexStr);
        this.rexStep.setSedMode(sedMode);
        this.rexStep.setCatCtx(catCtx);
        ds = this.rexStep.get();

        this.stack.push(ds);
        return new CatalystNode(ds);
    }


}
