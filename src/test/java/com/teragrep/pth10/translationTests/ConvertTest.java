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
package com.teragrep.pth10.translationTests;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.commands.transformstatement.ConvertTransformation;
import com.teragrep.pth10.steps.convert.AbstractConvertStep;
import com.teragrep.pth10.steps.convert.ConvertStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConvertTest {

    @Test
    void testConvertTranslation() throws Exception {
        String query = "| convert dur2sec(duration) AS result";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        ProcessingStack stack = new ProcessingStack(visitor);
        try {
            ConvertTransformation ct = new ConvertTransformation(new ArrayList<>(), stack, ctx);
            ct.visitConvertTransformation((DPLParser.ConvertTransformationContext) tree.getChild(0).getChild(1));
            ConvertStep cs = ct.convertStep;

            AbstractConvertStep.ConvertCommand cmd = cs.getListOfCommands().get(0);

            assertEquals(AbstractConvertStep.ConvertCommand.ConvertCommandType.DUR2SEC, cmd.getCommandType());
            assertEquals("duration", cmd.getFieldParam());
            assertEquals("result", cmd.getRenameField());
            assertEquals(0, cs.getListOfFieldsToOmit().size());
            assertEquals("%m/%d/%Y %H:%M:%S", cs.getTimeformat());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testConvertTranslation2() throws Exception {
        String query = "| convert memk(field) AS result";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        ProcessingStack stack = new ProcessingStack(visitor);
        try {
            ConvertTransformation ct = new ConvertTransformation(new ArrayList<>(), stack, ctx);
            ct.visitConvertTransformation((DPLParser.ConvertTransformationContext) tree.getChild(0).getChild(1));
            ConvertStep cs = ct.convertStep;

            AbstractConvertStep.ConvertCommand cmd = cs.getListOfCommands().get(0);

            assertEquals(AbstractConvertStep.ConvertCommand.ConvertCommandType.MEMK, cmd.getCommandType());
            assertEquals("field", cmd.getFieldParam());
            assertEquals("result", cmd.getRenameField());
            assertEquals(0, cs.getListOfFieldsToOmit().size());
            assertEquals("%m/%d/%Y %H:%M:%S", cs.getTimeformat());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testConvertTranslation3() throws Exception {
        String query = "| convert timeformat=\"%H:%M:%s %Y-%m-%d\" auto(duration) AS result";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        ProcessingStack stack = new ProcessingStack(visitor);
        try {
            ConvertTransformation ct = new ConvertTransformation(new ArrayList<>(), stack, ctx);
            ct.visitConvertTransformation((DPLParser.ConvertTransformationContext) tree.getChild(0).getChild(1));
            ConvertStep cs = ct.convertStep;

            AbstractConvertStep.ConvertCommand cmd = cs.getListOfCommands().get(0);

            assertEquals(AbstractConvertStep.ConvertCommand.ConvertCommandType.AUTO, cmd.getCommandType());
            assertEquals("duration", cmd.getFieldParam());
            assertEquals("result", cmd.getRenameField());
            assertEquals(0, cs.getListOfFieldsToOmit().size());
            assertEquals("%H:%M:%s %Y-%m-%d", cs.getTimeformat());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}

