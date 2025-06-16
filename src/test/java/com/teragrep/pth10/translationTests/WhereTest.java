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
package com.teragrep.pth10.translationTests;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.commands.transformstatement.WhereTransformation;
import com.teragrep.pth10.steps.where.WhereStep;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WhereTest {

    SparkSession spark = null;
    DPLParserCatalystContext ctx = null;

    @BeforeAll
    void setEnv() {
        spark = SparkSession.builder().appName("Java Spark SQL basic example").master("local[2]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ctx = new DPLParserCatalystContext(spark);
    }

    @Test
    void testWhereTranslation() {
        final String query = " | where offset < 5";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final WhereTransformation ct = new WhereTransformation(this.ctx);
        ct.visitWhereTransformation((DPLParser.WhereTransformationContext) tree.getChild(1).getChild(0));
        final WhereStep cs = ct.whereStep;

        Assertions
                .assertEquals(
                        "EvalOperation(offset, " + DPLLexer.EVAL_LANGUAGE_MODE_LT + ", 5)",
                        cs.getWhereColumn().toString()
                );
    }

    @Test
    void testWhereTranslation2() {
        final String query = " | where like(field, %5%)";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        final WhereTransformation ct = new WhereTransformation(ctx);
        ct.visitWhereTransformation((DPLParser.WhereTransformationContext) tree.getChild(1).getChild(0));
        final WhereStep cs = ct.whereStep;

        Assertions.assertEquals("field LIKE %5%", cs.getWhereColumn().toString());
    }

    @Test
    void testWhereTranslation3() {
        final String query = " | where offset = 5";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final WhereTransformation ct = new WhereTransformation(this.ctx);
        ct.visitWhereTransformation((DPLParser.WhereTransformationContext) tree.getChild(1).getChild(0));
        final WhereStep cs = ct.whereStep;

        Assertions
                .assertEquals(
                        "EvalOperation(offset, " + DPLLexer.EVAL_LANGUAGE_MODE_EQ + ", 5)",
                        cs.getWhereColumn().toString()
                );
    }

    @Test
    void testWhereTranslation4() {
        final String query = " | where NOT like(field,%40%)";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final WhereTransformation ct = new WhereTransformation(ctx);
        ct.visitWhereTransformation((DPLParser.WhereTransformationContext) tree.getChild(1).getChild(0));
        final WhereStep cs = ct.whereStep;

        Assertions.assertEquals("(NOT field LIKE %40%)", cs.getWhereColumn().toString());
    }
}
