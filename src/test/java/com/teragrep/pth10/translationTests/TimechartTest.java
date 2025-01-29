/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
import com.teragrep.pth10.ast.bo.ColumnNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.ast.commands.transformstatement.TimechartTransformation;
import com.teragrep.pth10.steps.AbstractStep;
import com.teragrep.pth10.steps.timechart.TimechartStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TimechartTest {

    @Test
    void testTimeChartTranslation() {
        String rename = "sales";
        String byField = "product";
        String query = "| timechart span=5min sum(sales) as " + rename + " by " + byField;

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);

        // create parse tree with PTH-03
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParser.AggregateMethodSumContext aggContext = (DPLParser.AggregateMethodSumContext) tree
                .getChild(1)
                .getChild(0)
                .getChild(2)
                .getChild(0);
        DPLParser.TimechartTransformationContext timechartContext = (DPLParser.TimechartTransformationContext) tree
                .getChild(1)
                .getChild(0);

        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        // traverse the tree in PTH-10 and create TimechartStep
        TimechartTransformation tct = new TimechartTransformation(ctx, visitor);
        StepNode timechartNode = (StepNode) tct.visitTimechartTransformation(timechartContext);
        AbstractStep tcs = timechartNode.get();

        // expected BY clause
        List<String> divByInsts = new ArrayList<>();
        divByInsts.add(byField);

        // expected aggregations
        AggregateFunction aggregateFunction = new AggregateFunction(ctx);
        ColumnNode aggColNode = (ColumnNode) aggregateFunction.visitAggregateMethodSum(aggContext); // "sum(sales)" aggregation
        Column aggCol = aggColNode.getColumn().as(rename); // "as sales"
        List<Column> aggCols = new ArrayList<>();
        aggCols.add(aggCol);

        // expected span
        CalendarInterval ival = new CalendarInterval(0, 0, 5 * 60 * 1000 * 1000);
        Column span = functions.window(new Column("_time"), String.valueOf(ival), "5 minutes", "0 minutes");

        TimechartStep expected = new TimechartStep(aggCols, divByInsts, span);

        Assertions.assertEquals(expected, tcs);
    }

    @Test
    void testTimeChartTranslation_NoByClause() {
        String rename = "sales";
        String query = "| timechart span=5min sum(sales) as " + rename;

        // create parse tree with PTH-03
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParser.AggregateMethodSumContext aggContext = (DPLParser.AggregateMethodSumContext) tree
                .getChild(1)
                .getChild(0)
                .getChild(2)
                .getChild(0);
        DPLParser.TimechartTransformationContext timechartContext = (DPLParser.TimechartTransformationContext) tree
                .getChild(1)
                .getChild(0);

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        // traverse the tree in PTH-10 and create TimechartStep
        TimechartTransformation tct = new TimechartTransformation(ctx, visitor);
        StepNode timechartNode = (StepNode) tct.visitTimechartTransformation(timechartContext);
        AbstractStep tcs = timechartNode.get();

        // expected aggregations
        AggregateFunction aggregateFunction = new AggregateFunction(ctx);
        ColumnNode aggColNode = (ColumnNode) aggregateFunction.visitAggregateMethodSum(aggContext); // "sum(sales)" aggregation
        Column aggCol = aggColNode.getColumn().as(rename); // "as sales"
        List<Column> aggCols = new ArrayList<>();
        aggCols.add(aggCol);

        // expected span
        CalendarInterval ival = new CalendarInterval(0, 0, 5 * 60 * 1000 * 1000);
        Column span = functions.window(new Column("_time"), String.valueOf(ival), "5 minutes", "0 minutes");

        TimechartStep expected = new TimechartStep(aggCols, new ArrayList<>(), span);

        Assertions.assertEquals(expected, tcs);
    }

    @Test
    void testTimeChartTranslationBasic() {
        String query = "| timechart count";

        // create parse tree with PTH-03
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParser.AggregateFunctionContext aggContext = (DPLParser.AggregateFunctionContext) tree
                .getChild(1)
                .getChild(0)
                .getChild(1);

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        // traverse the tree in PTH-10 and create TimechartStep
        TimechartTransformation tct = new TimechartTransformation(ctx, visitor);
        StepNode timechartNode = (StepNode) tct
                .visitTimechartTransformation((DPLParser.TimechartTransformationContext) tree.getChild(1).getChild(0));
        AbstractStep tcs = timechartNode.get();

        // expected aggregations
        AggregateFunction aggregateFunction = new AggregateFunction(ctx);
        ColumnNode aggColNode = (ColumnNode) aggregateFunction.visitAggregateFunction(aggContext); // "count" aggregation
        List<Column> aggCols = new ArrayList<>();
        aggCols.add(aggColNode.getColumn());

        // expected default span of 1 day when "span=" parameter is not specified
        CalendarInterval ival = new CalendarInterval(0, 1, 0);
        Column span = functions.window(new Column("_time"), String.valueOf(ival), "1 day", "0 minutes");

        TimechartStep expected = new TimechartStep(aggCols, new ArrayList<>(), span);

        Assertions.assertEquals(expected, tcs);
    }
}
