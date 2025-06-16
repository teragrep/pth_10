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
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.commands.transformstatement.TimechartTransformation;
import com.teragrep.pth10.steps.timechart.TimechartStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TimechartTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimechartTest.class);

    @Test
    void testTimeChartTranslation() {
        String query = "| timechart span=5min sum(sales) as sales by product";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/timechartTestData.jsonl";
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        TimechartTransformation tct = new TimechartTransformation(ctx, visitor);
        tct.visitTimechartTransformation((DPLParser.TimechartTransformationContext) tree.getChild(1).getChild(0));
        TimechartStep tcs = tct.timechartStep;

        Assertions.assertEquals("window(_time, 300000000, 300000000, 0) AS window", tcs.getSpan().toString());
        Assertions
                .assertEquals(
                        "sumaggregator(encodeusingserializer(input[0, java.lang.Object, true], false) AS value, decodeusingserializer(input[0, binary, true], com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.SumBuffer, false), staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false, true)) AS `sum(sales)` AS sales",
                        tcs.getAggCols().get(0).toString()
                );
        Assertions.assertEquals("product", tcs.getDivByInsts().get(0));
    }

    @Test
    void testTimeChartTranslation_NoByClause() {
        String query = "| timechart span=5min sum(sales) as sales";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        TimechartTransformation tct = new TimechartTransformation(ctx, visitor);
        tct.visitTimechartTransformation((DPLParser.TimechartTransformationContext) tree.getChild(1).getChild(0));
        TimechartStep tcs = tct.timechartStep;

        Assertions.assertEquals("window(_time, 300000000, 300000000, 0) AS window", tcs.getSpan().toString());
        Assertions
                .assertEquals(
                        "sumaggregator(encodeusingserializer(input[0, java.lang.Object, true], false) AS value, decodeusingserializer(input[0, binary, true], com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.SumBuffer, false), staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false, true)) AS `sum(sales)` AS sales",
                        tcs.getAggCols().get(0).toString()
                );
        Assertions.assertEquals(0, tcs.getDivByInsts().size());
    }

    @Test
    void testTimeChartTranslationBasic() {
        String query = "| timechart count";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        TimechartTransformation tct = new TimechartTransformation(ctx, visitor);
        tct.visitTimechartTransformation((DPLParser.TimechartTransformationContext) tree.getChild(1).getChild(0));
        TimechartStep tcs = tct.timechartStep;

        Assertions.assertEquals("window(_time, 86400000000, 86400000000, 0) AS window", tcs.getSpan().toString());
        Assertions
                .assertEquals(
                        "countaggregator(input[0, java.lang.Long, true].longValue AS value, staticinvoke(class java.lang.Long, ObjectType(class java.lang.Long), valueOf, input[0, bigint, true], true, false, true), input[0, java.lang.Long, true].longValue) AS count",
                        tcs.getAggCols().get(0).toString()
                );
        Assertions.assertEquals(0, tcs.getDivByInsts().size());
    }
}
