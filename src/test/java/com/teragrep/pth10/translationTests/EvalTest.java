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
import com.teragrep.pth10.ast.commands.transformstatement.EvalTransformation;
import com.teragrep.pth10.ast.commands.transformstatement.StatsTransformation;
import com.teragrep.pth10.steps.eval.EvalStep;
import com.teragrep.pth10.steps.stats.StatsStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EvalTest {
    SparkSession spark = null;
    DPLParserCatalystContext ctx = null;
    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ctx = new DPLParserCatalystContext(spark);
    }
    @Test
    void testEvalTranslation() throws Exception {
        final String query = "| eval a = abs(-3)";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final ProcessingStack stack = new ProcessingStack(visitor);
        try {
            final EvalTransformation ct = new EvalTransformation(ctx, stack, new ArrayList<>());
            ct.visitEvalTransformation((DPLParser.EvalTransformationContext) tree.getChild(0).getChild(1));
            final EvalStep cs = ct.evalStatement.evalStep;

            assertEquals("a",cs.getLeftSide());
            assertEquals("abs(-3)", cs.getRightSide().toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testEvalTranslation2() throws Exception {
        final String query = "| eval a = (3+4)*7";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final ProcessingStack stack = new ProcessingStack(visitor);
        try {
            final EvalTransformation ct = new EvalTransformation(ctx, stack, new ArrayList<>());
            ct.visitEvalTransformation((DPLParser.EvalTransformationContext) tree.getChild(0).getChild(1));
            final EvalStep cs = ct.evalStatement.evalStep;

            assertEquals("a",cs.getLeftSide());
            assertEquals("EvalArithmetic(EvalArithmetic(3, +, 4), *, 7)",cs.getRightSide().toString());
            //assertEquals("((3 + 4) * 7)", cs.getRightSide().toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testEvalTranslation3() throws Exception {
        final String query = "| eval a = \"string\"";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final ProcessingStack stack = new ProcessingStack(visitor);
        try {
            final EvalTransformation ct = new EvalTransformation(ctx, stack, new ArrayList<>());
            ct.visitEvalTransformation((DPLParser.EvalTransformationContext) tree.getChild(0).getChild(1));
            final EvalStep cs = ct.evalStatement.evalStep;

            assertEquals("a",cs.getLeftSide());
            assertEquals("string", cs.getRightSide().toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}

