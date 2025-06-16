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
import com.teragrep.pth10.ast.commands.transformstatement.PredictTransformation;
import com.teragrep.pth10.steps.predict.PredictStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PredictTest {

    @Test
    void testPredictTranslation_Basic() {
        String query = " | predict field";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        PredictTransformation pt = new PredictTransformation();
        pt.visitPredictTransformation((DPLParser.PredictTransformationContext) tree.getChild(1).getChild(0));
        PredictStep ps = pt.predictStep;

        Assertions.assertEquals(5, ps.getFutureTimespan());
        Assertions.assertEquals(1, ps.getListOfColumnsToPredict().size());
        Assertions.assertEquals("field", ps.getListOfColumnsToPredict().get(0).toString());
    }

    @Test
    void testPredictTranslation_Rename() {
        String query = " | predict field as xyz";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        PredictTransformation pt = new PredictTransformation();
        pt.visitPredictTransformation((DPLParser.PredictTransformationContext) tree.getChild(1).getChild(0));
        PredictStep ps = pt.predictStep;

        Assertions.assertEquals(5, ps.getFutureTimespan());
        Assertions.assertEquals(1, ps.getListOfColumnsToPredict().size());
        Assertions.assertEquals("field AS xyz", ps.getListOfColumnsToPredict().get(0).toString());
    }

    @Test
    void testPredictTranslation_FutureTimespan() {
        String query = " | predict field as xyz future_timespan=20";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        PredictTransformation pt = new PredictTransformation();
        pt.visitPredictTransformation((DPLParser.PredictTransformationContext) tree.getChild(1).getChild(0));
        PredictStep ps = pt.predictStep;

        Assertions.assertEquals(20, ps.getFutureTimespan());
        Assertions.assertEquals(1, ps.getListOfColumnsToPredict().size());
        Assertions.assertEquals("field AS xyz", ps.getListOfColumnsToPredict().get(0).toString());
    }

    @Test
    void testPredictTranslation_LowerUpper() {
        String query = " | predict field as xyz upper 69 = a lower 70 = b";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        PredictTransformation pt = new PredictTransformation();
        pt.visitPredictTransformation((DPLParser.PredictTransformationContext) tree.getChild(1).getChild(0));
        PredictStep ps = pt.predictStep;

        Assertions.assertEquals(5, ps.getFutureTimespan());
        Assertions.assertEquals(1, ps.getListOfColumnsToPredict().size());
        Assertions.assertEquals("field AS xyz", ps.getListOfColumnsToPredict().get(0).toString());
        Assertions.assertEquals(70, ps.getLower());
        Assertions.assertEquals("a", ps.getUpperField());
        Assertions.assertEquals(69, ps.getUpper());
        Assertions.assertEquals("b", ps.getLowerField());
    }

    @Test
    void testPredictTranslation_algoLL() {
        String query = " | predict field algorithm=LL";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        PredictTransformation pt = new PredictTransformation();
        pt.visitPredictTransformation((DPLParser.PredictTransformationContext) tree.getChild(1).getChild(0));
        PredictStep ps = pt.predictStep;

        Assertions.assertEquals(5, ps.getFutureTimespan());
        Assertions.assertEquals(1, ps.getListOfColumnsToPredict().size());
        Assertions.assertEquals("field", ps.getListOfColumnsToPredict().get(0).toString());
        Assertions.assertEquals("LL", ps.getAlgorithm().name());
    }

    @Test
    void testPredictTranslation_algoLLT() {
        String query = " | predict field algorithm=LLT";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        PredictTransformation pt = new PredictTransformation();
        pt.visitPredictTransformation((DPLParser.PredictTransformationContext) tree.getChild(1).getChild(0));
        PredictStep ps = pt.predictStep;

        Assertions.assertEquals(5, ps.getFutureTimespan());
        Assertions.assertEquals(1, ps.getListOfColumnsToPredict().size());
        Assertions.assertEquals("field", ps.getListOfColumnsToPredict().get(0).toString());
        Assertions.assertEquals("LLT", ps.getAlgorithm().name());
    }
}
