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
import com.teragrep.pth10.steps.rename.RenameStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains the visitor functions for the command <code>rename</code>.
 * <pre>{@literal ... | rename <original-field> AS <new-field-name>}</pre>
 */
public class RenameTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenameTransformation.class);
    private DPLParserCatalystContext catCtx = null;
    private ProcessingStack processingStack = null;

    public RenameStep renameStep = null;
    public RenameTransformation(ProcessingStack processingStack, DPLParserCatalystContext catCtx) {
        this.processingStack = processingStack;
        this.catCtx = catCtx;
    }

    @Override
    public Node visitRenameTransformation(DPLParser.RenameTransformationContext ctx) {
        // rename field AS field , field AS field , ...
        Dataset<Row> ds = null;
        if (!this.processingStack.isEmpty()) {
            ds = this.processingStack.pop();
        }
        this.renameStep = new RenameStep(ds);

        String originalName = null;
        String newName = null;
        boolean gettingNewName = false;
        Map<String, String> mapOfRenamedFields = new HashMap<>(); // key->original, value->new

        for (int i = 1; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);

            if (child instanceof DPLParser.FieldTypeContext) {
                if (gettingNewName) {
                    // get the new field name after AS
                    newName = child.getText();
                    gettingNewName = false;
                }
                else {
                    // first, get the original field name
                    originalName = child.getText();
                }

            }
            else if (child instanceof TerminalNode) {
                if (child.getText().equalsIgnoreCase("AS")) {
                    // after "AS" -> the next child is the new name for the field
                    gettingNewName = true;
                }
            }

            if (originalName != null && newName != null) {
                // rename the column based on original and new name
                mapOfRenamedFields.put(Util.stripQuotes(originalName), Util.stripQuotes(newName));
                originalName = null;
                newName = null;
            }
        }

        // step
        this.renameStep.setMapOfRenamedFields(mapOfRenamedFields);
        ds = this.renameStep.get();

        processingStack.push(ds);
        return new CatalystNode(ds);
    }
}
