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
package com.teragrep.pth_10.translationTests;

import com.teragrep.pth_10.ast.DPLParserCatalystContext;
import com.teragrep.pth_10.ast.commands.transformstatement.EvalTransformation;
import com.teragrep.pth_10.steps.eval.EvalStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.ZoneId;
import java.time.ZonedDateTime;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EvalTest {

    SparkSession spark = null;
    DPLParserCatalystContext ctx = null;

    @BeforeAll
    void setEnv() {
        spark = SparkSession.builder().appName("Java Spark SQL basic example").master("local[2]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ctx = new DPLParserCatalystContext(spark, ZonedDateTime.now(ZoneId.of("UTC")));
    }

    @Test
    void testEvalTranslation() {
        final String query = "| eval a = abs(-3)";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null, ZonedDateTime.now(ZoneId.of("UTC")));
        ctx.setEarliest("-1w");

        final EvalTransformation ct = new EvalTransformation(ctx);
        ct.visitEvalTransformation((DPLParser.EvalTransformationContext) tree.getChild(1).getChild(0));
        final EvalStep cs = ct.evalStatement.evalStep;

        Assertions.assertEquals("a", cs.getLeftSide());
        Assertions.assertEquals("abs(-3)", cs.getRightSide().toString());

    }

    @Test
    void testEvalTranslation2() {
        final String query = "| eval a = (3+4)*7";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null, ZonedDateTime.now(ZoneId.of("UTC")));
        ctx.setEarliest("-1w");

        final EvalTransformation ct = new EvalTransformation(ctx);
        ct.visitEvalTransformation((DPLParser.EvalTransformationContext) tree.getChild(1).getChild(0));
        final EvalStep cs = ct.evalStatement.evalStep;

        Assertions.assertEquals("a", cs.getLeftSide());
        Assertions.assertEquals("EvalArithmetic(EvalArithmetic(3, +, 4), *, 7)", cs.getRightSide().toString());
    }

    @Test
    void testEvalTranslation3() {
        final String query = "| eval a = \"string\"";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null, ZonedDateTime.now(ZoneId.of("UTC")));
        ctx.setEarliest("-1w");

        final EvalTransformation ct = new EvalTransformation(ctx);
        ct.visitEvalTransformation((DPLParser.EvalTransformationContext) tree.getChild(1).getChild(0));
        final EvalStep cs = ct.evalStatement.evalStep;

        Assertions.assertEquals("a", cs.getLeftSide());
        Assertions.assertEquals("string", cs.getRightSide().toString());
    }
}
