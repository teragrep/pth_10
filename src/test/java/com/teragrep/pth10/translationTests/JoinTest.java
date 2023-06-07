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
import com.teragrep.pth10.ast.commands.transformstatement.JoinTransformation;
import com.teragrep.pth10.steps.join.JoinStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JoinTest {
    /*
        String joinMode = "inner";
		Boolean usetime = false;
		Boolean earlier = true;
		Boolean overwrite = true;
		Integer max = 1;
     */
    @Test
    void testJoinTranslation() throws Exception {
        String query = "| join offset [ | eval offset=1 ]";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        ProcessingStack stack = new ProcessingStack(visitor);
        try {
            JoinTransformation ct = new JoinTransformation(new ArrayList<>(), stack, ctx);
            ct.visitJoinTransformation((DPLParser.JoinTransformationContext) tree.getChild(0).getChild(1));
            JoinStep cs = ct.joinStep;

            assertEquals("inner", cs.getJoinMode());
            assertEquals(1, cs.getMax());
            assertEquals("[offset]", Arrays.toString(cs.getListOfFields().toArray()));
            assertTrue(cs.getEarlier());
            assertNotNull(cs.getPathForSubsearchSave());
            assertTrue(cs.getOverwrite());
            assertFalse(cs.getUsetime());


        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testJoinTranslation2() throws Exception {
        String query = "| join type=left offset [ | eval offset=1 ]";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        ProcessingStack stack = new ProcessingStack(visitor);
        try {
            JoinTransformation ct = new JoinTransformation(new ArrayList<>(), stack, ctx);
            ct.visitJoinTransformation((DPLParser.JoinTransformationContext) tree.getChild(0).getChild(1));
            JoinStep cs = ct.joinStep;

            assertEquals("left", cs.getJoinMode());
            assertEquals(1, cs.getMax());
            assertEquals("[offset]", Arrays.toString(cs.getListOfFields().toArray()));
            assertTrue(cs.getEarlier());
            assertNotNull(cs.getPathForSubsearchSave());
            assertTrue(cs.getOverwrite());
            assertFalse(cs.getUsetime());


        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}

