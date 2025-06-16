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
import com.teragrep.pth10.ast.commands.transformstatement.replace.ReplaceCmd;
import com.teragrep.pth10.steps.replace.ReplaceStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The base transformation class used for the command <code>replace</code>
 */
public class ReplaceTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceTransformation.class);
    private final Map<String, String> replacements;
    private final List<String> listOfFields;
    public ReplaceStep replaceStep;

    public ReplaceTransformation() {
        this.replacements = new HashMap<>();
        this.listOfFields = new ArrayList<>();
    }

    /**
     * Gets all the parameters from given command and applies the {@link ReplaceCmd}
     * 
     * @param ctx ReplaceTransformationContext
     * @return StepNode with Step for replace command
     */
    @Override
    public Node visitReplaceTransformation(DPLParser.ReplaceTransformationContext ctx) {
        visitChildren(ctx);

        this.replaceStep = new ReplaceStep(listOfFields, replacements);

        return new StepNode(this.replaceStep);
    }

    @Override
    public Node visitT_replace_withInstruction(DPLParser.T_replace_withInstructionContext ctx) {
        // field WITH field
        String contentToReplace = new UnquotedText(new TextString(ctx.fieldType(0).getText())).read();
        String replaceWith = new UnquotedText(new TextString(ctx.fieldType(1).getText())).read();
        replacements.put(contentToReplace, replaceWith);

        return new NullNode();
    }

    /**
     * Gets the list of field names from the parse tree / command
     * 
     * @param ctx fieldList context
     * @return NullNode
     */
    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        ctx.fieldType().forEach(c -> listOfFields.add(new UnquotedText(new TextString(c.getText())).read()));

        return new NullNode();
    }
}
