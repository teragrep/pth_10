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
import com.teragrep.pth10.steps.iplocation.IplocationStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Visitor class for the 'iplocation' command. <br>
 * The command provides location information for the provided ip address. <br>
 * Parameters:<br>
 * {@literal | iplocation prefix=<string> allfields=<boolean> lang=<string> <field-name>}
 */
public class IplocationTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IplocationTransformation.class);
    private static final String DATABASE_PATH_CONFIG_ITEM = "dpl.pth_10.transform.iplocation.db.path";
    private final ProcessingStack stack;
    private final DPLParserCatalystContext catCtx;

    public IplocationStep iplocationStep = null;
    public IplocationTransformation(ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.stack = stack;
        this.catCtx = catCtx;
    }

    @Override
    public Node visitIplocationTransformation(DPLParser.IplocationTransformationContext ctx) {
        // variables for command parameters
        String lang = "en";
        String field = null; // required
        boolean allFields = false;
        String prefix = "";

        // pop from stack; init step object
        Dataset<Row> ds = null;
        if (!this.stack.isEmpty()) {
            ds = this.stack.pop();
        }
        this.iplocationStep = new IplocationStep(ds);

        // go through command parameters, and place them into variables
        // <field-type>, lang, allfields, prefix
        if (ctx.fieldType() != null) {
            field = Util.stripQuotes(ctx.fieldType().getText());
        }
        else {
            throw new IllegalArgumentException("The field parameter cannot be empty!");
        }

        if (ctx.t_iplocation_langParameter() != null) {
            lang = Util.stripQuotes(ctx.t_iplocation_langParameter().stringType().getText());
        }

        if (ctx.t_iplocation_allFieldsParameter() != null) {
            final TerminalNode boolNode = ((TerminalNode) ctx.t_iplocation_allFieldsParameter().booleanType().getChild(0));
            final int boolType = boolNode.getSymbol().getType();

            switch (boolType) {
                case DPLLexer.GET_BOOLEAN_TRUE:
                    allFields = true;
                    break;
                case DPLLexer.GET_BOOLEAN_FALSE:
                    allFields = false;
                    break;
                default:
                    throw new RuntimeException("Invalid boolean type provided for 'allfields' parameter!\nMake sure that" +
                            " the parameter is followed by 'true' or 'false'.");
            }
        }

        if (ctx.t_iplocation_prefixParameter() != null) {
            prefix = Util.stripQuotes(ctx.t_iplocation_prefixParameter().stringType().getText());
        }

        // set params to step object and perform actual ds ops
        this.iplocationStep.setLang(lang);
        this.iplocationStep.setField(field);
        this.iplocationStep.setAllFields(allFields);
        this.iplocationStep.setPrefix(prefix);
        this.iplocationStep.setCatCtx(catCtx);

        // get mmdb filepath
        if (this.catCtx.getConfig() != null && this.catCtx.getConfig().hasPath(DATABASE_PATH_CONFIG_ITEM)) {
            this.iplocationStep.setPathToDb(this.catCtx.getConfig().getString(DATABASE_PATH_CONFIG_ITEM));
        }
        else {
            // no config item, get backup path from visitor (mostly for testing purposes)
            this.iplocationStep.setPathToDb(this.stack.getCatVisitor().getIplocationMmdbPath());
        }

        ds = this.iplocationStep.get();

        this.stack.push(ds);
        return new CatalystNode(ds);
    }
}
