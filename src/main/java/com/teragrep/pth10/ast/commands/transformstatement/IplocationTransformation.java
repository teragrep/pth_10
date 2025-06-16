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

import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.steps.iplocation.IplocationStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
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
    private final DPLParserCatalystContext catCtx;
    private final DPLParserCatalystVisitor catVisitor;

    public IplocationStep iplocationStep = null;

    public IplocationTransformation(DPLParserCatalystContext catCtx, DPLParserCatalystVisitor catVisitor) {
        this.catCtx = catCtx;
        this.catVisitor = catVisitor;
    }

    @Override
    public Node visitIplocationTransformation(DPLParser.IplocationTransformationContext ctx) {
        // init step object
        this.iplocationStep = new IplocationStep();

        // go through command parameters, and set them into iplocationStep
        visitChildren(ctx);

        this.iplocationStep.setCatCtx(catCtx);

        // get mmdb filepath
        if (this.catCtx.getConfig() != null && this.catCtx.getConfig().hasPath(DATABASE_PATH_CONFIG_ITEM)) {
            this.iplocationStep.setPathToDb(this.catCtx.getConfig().getString(DATABASE_PATH_CONFIG_ITEM));
        }
        else {
            // no config item, get backup path from visitor (mostly for testing purposes)
            this.iplocationStep.setPathToDb(this.catVisitor.getIplocationMmdbPath());
        }

        return new StepNode(iplocationStep);
    }

    @Override
    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        String field = new UnquotedText(new TextString(ctx.getText())).read();
        this.iplocationStep.setField(field);
        return new StringNode(new Token(Token.Type.STRING, field));
    }

    @Override
    public Node visitT_iplocation_langParameter(DPLParser.T_iplocation_langParameterContext ctx) {
        String lang = new UnquotedText(new TextString(ctx.stringType().getText())).read();
        this.iplocationStep.setLang(lang);
        return new StringNode(new Token(Token.Type.STRING, lang));
    }

    @Override
    public Node visitT_iplocation_allFieldsParameter(DPLParser.T_iplocation_allFieldsParameterContext ctx) {
        boolean allFields;
        final TerminalNode boolNode = ((TerminalNode) ctx.booleanType().getChild(0));
        final int boolType = boolNode.getSymbol().getType();

        switch (boolType) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                allFields = true;
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                allFields = false;
                break;
            default:
                throw new RuntimeException(
                        "Invalid boolean type provided for 'allfields' parameter!\nMake sure that"
                                + " the parameter is followed by 'true' or 'false'."
                );
        }

        this.iplocationStep.setAllFields(allFields);
        return new StringNode(new Token(Token.Type.STRING, boolNode.getText()));
    }

    @Override
    public Node visitT_iplocation_prefixParameter(DPLParser.T_iplocation_prefixParameterContext ctx) {
        String prefix = new UnquotedText(new TextString(ctx.stringType().getText())).read();
        this.iplocationStep.setPrefix(prefix);
        return new StringNode(new Token(Token.Type.STRING, prefix));
    }
}
