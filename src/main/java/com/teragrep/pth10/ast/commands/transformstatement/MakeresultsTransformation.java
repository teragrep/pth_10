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
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StringNode;
import com.teragrep.pth10.ast.bo.Token;
import com.teragrep.pth10.steps.makeresults.MakeresultsStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base transformation class for the command makeresults.<br>
 * Generates $count rows with _time column. More columns can be added by setting $annotate=true
 */
public class MakeresultsTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MakeresultsTransformation.class);
    private ProcessingStack processingStack = null;
    private DPLParserCatalystContext catCtx = null;

    public MakeresultsStep makeresultsStep = null;

    public MakeresultsTransformation(ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.processingStack = stack;
        this.catCtx = catCtx;
    }

    @Override
    public Node visitMakeresultsTransformation(DPLParser.MakeresultsTransformationContext ctx) {
        return makeresultsTransformationEmitCatalyst(ctx);
    }

    /**
     * Sets all the parameters based on the values given on the command, and generates
     * a streaming dataset.
     * @param ctx
     * @return
     */
    private Node makeresultsTransformationEmitCatalyst(DPLParser.MakeresultsTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!processingStack.isEmpty()) {
            ds = processingStack.pop();
        }
        this.makeresultsStep = new MakeresultsStep(ds);

        int count = 1;
        boolean annotate = false;
        String server = "local";
        List<String> serverGroup = new ArrayList<>(); // default none

        // Go through any parameters
        if (ctx.t_makeresults_annotateOptParameter() != null) {
            annotate = ((StringNode) visit(ctx.t_makeresults_annotateOptParameter())).toString().equals("true");
        }

        if (ctx.t_makeresults_countParameter() != null) {
            String countParameter = ctx.t_makeresults_countParameter().getChild(1).getText();

            Matcher m = Pattern.compile("\\d{1,7}").matcher(countParameter);

            if (m.matches()) {
                count = Integer.parseInt(countParameter);
                if (count < 1 || count > 2_000_000) {
                    // based on local testing >2M causes memory issues and running out of heap space
                    throw new IllegalArgumentException("Makeresults: Count parameter value must be a positive integer between 1 and 2 000 000.");
                }
            }
            else {
                throw new IllegalArgumentException("Makeresults: Invalid count parameter value provided! It must be a positive integer between 1 and 2 000 000.");
            }


        }

        if (ctx.t_makeresults_struckServerGroupParameter() != null) {
            // TODO implement
            ctx.t_makeresults_struckServerGroupParameter().forEach(group -> serverGroup.add(group.getText()));
        }

        if (ctx.t_makeresults_struckServerParameter() != null) {
            // TODO implement
            server = ctx.t_makeresults_struckServerParameter().getText();
        }


        this.makeresultsStep.setAnnotate(annotate);
        this.makeresultsStep.setServer(server);
        this.makeresultsStep.setCount(count);
        this.makeresultsStep.setServerGroups(serverGroup);
        this.makeresultsStep.setCatCtx(catCtx);
        Dataset<Row> generated = null;
        if (this.catCtx.getSparkSession() != null) {
            generated = this.makeresultsStep.get();
        }

        processingStack.push(generated);
        return new CatalystNode(generated);
    }

    @Override
    public Node visitT_makeresults_annotateOptParameter(DPLParser.T_makeresults_annotateOptParameterContext ctx) {
        TerminalNode boolValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (boolValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        return new StringNode(new Token(Token.Type.STRING, value));
    }


}
