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
import com.teragrep.pth10.steps.rename.RenameStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
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

    public RenameStep renameStep = null;

    public RenameTransformation() {

    }

    @Override
    public Node visitRenameTransformation(DPLParser.RenameTransformationContext ctx) {
        // rename field AS field , field AS field , ...
        this.renameStep = new RenameStep();

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
                mapOfRenamedFields
                        .put(new UnquotedText(new TextString(originalName)).read(), new UnquotedText(new TextString(newName)).read());
                originalName = null;
                newName = null;
            }
        }

        // step
        this.renameStep.setMapOfRenamedFields(mapOfRenamedFields);

        return new StepNode(renameStep);
    }
}
