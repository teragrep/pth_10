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
import com.teragrep.pth10.ast.bo.StringListNode;
import com.teragrep.pth10.ast.commands.transformstatement.replace.ReplaceCmd;
import com.teragrep.pth10.steps.replace.ReplaceStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The base transformation class used for the command <code>replace</code>
 */
public class ReplaceTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceTransformation.class);
    private DPLParserCatalystContext catCtx = null;
    private ProcessingStack processingStack = null;
    public ReplaceStep replaceStep = null;

    public ReplaceTransformation(ProcessingStack processingStack, DPLParserCatalystContext catCtx) {
        this.processingStack = processingStack;
        this.catCtx = catCtx;
    }

    /**
     * Gets all the parameters from given command and applies the {@link ReplaceCmd}
     * @param ctx
     * @return
     */
    @Override
    public Node visitReplaceTransformation(DPLParser.ReplaceTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!this.processingStack.isEmpty()) {
            ds = processingStack.pop();
        }
        this.replaceStep = new ReplaceStep(ds);

        // replace field WITH field IN fieldList
        String contentToReplace = null;
        String replaceWith = null;
        boolean afterWith = false;

        List<String> listOfFields = null;

        // Get the necessary parameters from the given DPL command
        for (int i = 1; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (child instanceof DPLParser.FieldTypeContext) {
                if (afterWith) {
                    // after "WITH" keyword is the WITH clause
                    replaceWith = Util.stripQuotes(child.getText());
                    afterWith = false;
                }
                else {
                    // before "WITH" is the content to replace
                    contentToReplace = Util.stripQuotes(child.getText());
                }
            }
            else if (child instanceof DPLParser.FieldListTypeContext) {
                // fields given in "IN" clause
                listOfFields = ((StringListNode)visit(child)).asList();
            }
            else {
                if (child.getText().equalsIgnoreCase("WITH")) {
                    // Next fieldtype must be the WITH clause in this case
                    afterWith = true;
                }
            }
        }

        replaceStep.setReplaceWith(replaceWith);
        replaceStep.setContentToReplace(contentToReplace);
        replaceStep.setListOfFields(listOfFields);
        ds = replaceStep.get();

        processingStack.push(ds);
        return new CatalystNode(ds);
    }

    /**
     * Gets the list of field names from the parse tree / command
     * @param ctx fieldList context
     * @return StringListNode of field names
     */
    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        List<String> listOfFields = new ArrayList<>();
        ctx.children.forEach(c -> {
            if (!(c instanceof TerminalNode)) {
                listOfFields.add(Util.stripQuotes(c.getText()));
            }
        });

        return new StringListNode(listOfFields);
    }
}
