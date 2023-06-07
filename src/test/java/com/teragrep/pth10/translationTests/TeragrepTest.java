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
import com.teragrep.pth10.ast.commands.transformstatement.TeragrepTransformation;
import com.teragrep.pth10.steps.teragrep.AbstractTeragrepStep;
import com.teragrep.pth10.steps.teragrep.TeragrepStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TeragrepTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepTest.class);
    @Test
    void testTeragrepTranslation() throws Exception {
        final String query = "| teragrep exec syslog stream host 127.0.0.123 port 1337";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);


        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_SYSLOG_STREAM, cs.getCmdMode());
            assertEquals("127.0.0.123", cs.getHost());
            assertEquals(1337, cs.getPort());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepDefaultParamsTranslation() throws Exception {
        final String query = "| teragrep exec syslog stream";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);


        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_SYSLOG_STREAM, cs.getCmdMode());
            assertEquals("127.0.0.1", cs.getHost());
            assertEquals(601, cs.getPort());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepHdfsSaveTranslation() throws Exception {
        final String query = "| teragrep exec hdfs save /tmp/path";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);


        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_SAVE, cs.getCmdMode());
            assertEquals("/tmp/path", cs.getPath());
            assertNull(cs.getRetentionSpan());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepHdfsSaveRetentionTranslation() throws Exception {
        final String query = "| teragrep exec hdfs save \"/tmp/path\" retention=1d";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        LOGGER.debug(tree.toStringTree(parser));

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_SAVE, cs.getCmdMode());
            assertEquals("/tmp/path", cs.getPath());
            assertEquals("1d", cs.getRetentionSpan());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepHdfsSaveOverwriteTranslation() throws Exception {
        final String query = "| teragrep exec hdfs save \"/tmp/path\" overwrite=true";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        LOGGER.debug(tree.toStringTree(parser));

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_SAVE, cs.getCmdMode());
            assertEquals("/tmp/path", cs.getPath());
            assertTrue(cs.isOverwrite());
            assertNull(cs.getRetentionSpan());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepHdfsLoadTranslation() throws Exception {
        final String query = "| teragrep exec hdfs load /tmp/path";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);


        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_LOAD, cs.getCmdMode());
            assertEquals("/tmp/path", cs.getPath());
            assertNull(cs.getRetentionSpan());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepTranslationKafkaSave() throws Exception {
        final String query = "| teragrep exec kafka save MY-TOPIC";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_KAFKA_SAVE, cs.getCmdMode());
            assertEquals("MY-TOPIC", cs.getKafkaTopic());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepHdfsListTranslation() throws Exception {
        final String query = "| teragrep exec hdfs list /tmp/";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_LIST, cs.getCmdMode());
            assertEquals("/tmp/", cs.getPath());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepHdfsDeleteTranslation() throws Exception {
        final String query = "| teragrep exec hdfs delete /tmp/something";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_DELETE, cs.getCmdMode());
            assertEquals("/tmp/something", cs.getPath());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepBloomCreateTranslation() throws Exception {
        final String query = "| teragrep exec bloom create";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_BLOOM_CREATE, cs.getCmdMode());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    void testTeragrepBloomUpdateTranslation() throws Exception {
        final String query = "| teragrep exec bloom update";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        final ProcessingStack stack = new ProcessingStack(visitor);

        try {
            final TeragrepTransformation ct = new TeragrepTransformation(ctx, stack, new ArrayList<>(), false);
            ct.visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(0).getChild(1));
            final TeragrepStep cs = ct.teragrepStep;

            assertEquals(AbstractTeragrepStep.TeragrepCommandMode.EXEC_BLOOM_UPDATE, cs.getCmdMode());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}

