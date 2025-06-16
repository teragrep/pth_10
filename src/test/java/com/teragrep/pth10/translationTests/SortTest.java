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

import com.teragrep.functions.dpf_02.SortByClause;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.commands.transformstatement.SortTransformation;
import com.teragrep.pth10.steps.sort.SortStep;
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

import java.util.ArrayList;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SortTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortTest.class);

    @Test
    void testSortTranslation() {
        String query = "| sort +num(offset)";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        SortTransformation ct = new SortTransformation(ctx, visitor);
        ct.visitSortTransformation((DPLParser.SortTransformationContext) tree.getChild(1).getChild(0));
        SortStep cs = ct.sortStep;

        // expected
        SortByClause sbc = new SortByClause();
        sbc.setFieldName("offset");
        sbc.setDescending(false);
        sbc.setSortAsType(SortByClause.Type.NUMERIC);

        // actual
        SortByClause testSbc = cs.getListOfSortByClauses().get(0);

        // check contents rather than objects
        Assertions.assertEquals(sbc.getSortAsType(), testSbc.getSortAsType());
        Assertions.assertEquals(sbc.getFieldName(), testSbc.getFieldName());
        Assertions.assertEquals(sbc.getLimit(), testSbc.getLimit());

    }

    @Test
    void testSortTranslation2() {
        String query = "| sort 1234 +num(a) -str(b) +ip(c) +auto(d) desc";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        SortTransformation ct = new SortTransformation(ctx, visitor);
        ct.visitSortTransformation((DPLParser.SortTransformationContext) tree.getChild(1).getChild(0));
        SortStep cs = ct.sortStep;

        // expected
        // desc flips setDescending from actual given, add ! in front of each boolean
        SortByClause sbc = new SortByClause();
        sbc.setFieldName("a");
        sbc.setLimit(1234);
        sbc.setDescending(true);
        sbc.setSortAsType(SortByClause.Type.NUMERIC);

        SortByClause sbc2 = new SortByClause();
        sbc2.setFieldName("b");
        sbc2.setLimit(1234);
        sbc2.setDescending(false);
        sbc2.setSortAsType(SortByClause.Type.STRING);

        SortByClause sbc3 = new SortByClause();
        sbc3.setFieldName("c");
        sbc3.setLimit(1234);
        sbc3.setDescending(true);
        sbc3.setSortAsType(SortByClause.Type.IP_ADDRESS);

        SortByClause sbc4 = new SortByClause();
        sbc4.setFieldName("d");
        sbc4.setLimit(1234);
        sbc4.setDescending(true);
        sbc4.setSortAsType(SortByClause.Type.AUTOMATIC);

        // build list from expected
        List<SortByClause> expected = new ArrayList<>();
        expected.add(sbc);
        expected.add(sbc2);
        expected.add(sbc3);
        expected.add(sbc4);

        for (int i = 0; i < cs.getListOfSortByClauses().size(); i++) {
            SortByClause fromRun = cs.getListOfSortByClauses().get(i);
            SortByClause fromExpected = expected.get(i);

            // check contents rather than objects
            Assertions.assertEquals(fromExpected.getSortAsType(), fromRun.getSortAsType());
            Assertions.assertEquals(fromExpected.getFieldName(), fromRun.getFieldName());
            Assertions.assertEquals(fromExpected.getLimit(), fromRun.getLimit());
        }

        Assertions.assertEquals(1234, cs.getLimit());
    }
}
