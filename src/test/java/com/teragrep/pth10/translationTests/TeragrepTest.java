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

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.StepListNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.ast.commands.transformstatement.TeragrepTransformation;
import com.teragrep.pth10.steps.teragrep.*;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TeragrepTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepTest.class);

    @Test
    void testTeragrepTranslation() {
        final String query = "| teragrep exec syslog stream host 127.0.0.123 port 1337";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepSyslogStep.class, step.getClass());
        TeragrepSyslogStep syslogStep = (TeragrepSyslogStep) step;

        Assertions.assertEquals("127.0.0.123", syslogStep.relpHost);
        Assertions.assertEquals(1337, syslogStep.relpPort);
    }

    @Test
    void testTeragrepDefaultParamsTranslation() {
        final String query = "| teragrep exec syslog stream";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepSyslogStep.class, step.getClass());
        final TeragrepSyslogStep syslogStep = (TeragrepSyslogStep) step;

        Assertions.assertEquals("127.0.0.1", syslogStep.relpHost);
        Assertions.assertEquals(601, syslogStep.relpPort);
    }

    @Test
    void testTeragrepSyslogConfigTranslation() {
        final String host = "127.0.0.3";
        final int port = 603;

        final String query = "| teragrep exec syslog stream";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        // Create config
        HashMap<String, Object> map = new HashMap<>();
        map.put("dpl.pth_10.transform.teragrep.syslog.parameter.host", host);
        map.put("dpl.pth_10.transform.teragrep.syslog.parameter.port", port);
        Config c = ConfigFactory.parseMap(map);

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        ctx.setConfig(c);
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepSyslogStep.class, step.getClass());
        final TeragrepSyslogStep syslogStep = (TeragrepSyslogStep) step;

        Assertions.assertEquals(host, syslogStep.relpHost);
        Assertions.assertEquals(port, syslogStep.relpPort);
    }

    @Test
    void testTeragrepHdfsSaveTranslation() {
        final String query = "| teragrep exec hdfs save /tmp/path";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepHdfsSaveStep.class, step.getClass());
        final TeragrepHdfsSaveStep saveStep = (TeragrepHdfsSaveStep) step;

        Assertions.assertEquals("/tmp/path", saveStep.pathStr);
        Assertions.assertNull(saveStep.retentionSpan);
    }

    @Test
    void testTeragrepHdfsSaveRetentionTranslation() {
        final String query = "| teragrep exec hdfs save \"/tmp/path\" retention=1d";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        LOGGER.debug(tree.toStringTree(parser));

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepHdfsSaveStep.class, step.getClass());
        final TeragrepHdfsSaveStep saveStep = (TeragrepHdfsSaveStep) step;

        Assertions.assertEquals("/tmp/path", saveStep.pathStr);
        Assertions.assertEquals("1d", saveStep.retentionSpan);
    }

    @Test
    void testTeragrepHdfsSaveOverwriteTranslation() {
        final String query = "| teragrep exec hdfs save \"/tmp/path\" overwrite=true";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        LOGGER.debug(tree.toStringTree(parser));

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepHdfsSaveStep.class, step.getClass());
        final TeragrepHdfsSaveStep saveStep = (TeragrepHdfsSaveStep) step;

        Assertions.assertEquals("/tmp/path", saveStep.pathStr);
        Assertions.assertTrue(saveStep.overwrite);
        Assertions.assertNull(saveStep.retentionSpan);
    }

    @Test
    void testTeragrepHdfsLoadTranslation() {
        final String query = "| teragrep exec hdfs load /tmp/path";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepHdfsLoadStep.class, step.getClass());
        TeragrepHdfsLoadStep loadStep = (TeragrepHdfsLoadStep) step;

        Assertions.assertEquals("/tmp/path", loadStep.pathStr);
    }

    @Test
    void testTeragrepTranslationKafkaSave() {
        final String query = "| teragrep exec kafka save MY-TOPIC";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepKafkaStep.class, step.getClass());
        TeragrepKafkaStep kafkaStep = (TeragrepKafkaStep) step;

        Assertions.assertEquals("MY-TOPIC", kafkaStep.kafkaTopic);
    }

    @Test
    void testTeragrepHdfsListTranslation() {
        final String query = "| teragrep exec hdfs list /tmp/";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepHdfsListStep.class, step.getClass());
        TeragrepHdfsListStep listStep = (TeragrepHdfsListStep) step;

        Assertions.assertEquals("/tmp/", listStep.getPathStr());
    }

    @Test
    void testTeragrepHdfsDeleteTranslation() {
        final String query = "| teragrep exec hdfs delete /tmp/something";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepNode stepNode = (StepNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        AbstractStep step = stepNode.get();

        Assertions.assertEquals(TeragrepHdfsDeleteStep.class, step.getClass());
        TeragrepHdfsDeleteStep deleteStep = (TeragrepHdfsDeleteStep) step;

        Assertions.assertEquals("/tmp/something", deleteStep.pathStr);
    }

    @Test
    void testTeragrepBloomCreateTranslation() {
        final String query = "| teragrep exec bloom create table myTable regex \\w{4}";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepListNode stepNode = (StepListNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.asList().get(0); // first is an aggregation step
        final AbstractStep step1 = stepNode.asList().get(1); // second is bloom create

        Assertions.assertEquals(TeragrepBloomStep.class, step.getClass());
        Assertions.assertEquals(TeragrepBloomStep.class, step1.getClass());
        TeragrepBloomStep bloomStep = (TeragrepBloomStep) step;
        TeragrepBloomStep bloomStep1 = (TeragrepBloomStep) step1;

        Assertions.assertEquals(TeragrepBloomStep.BloomMode.AGGREGATE, bloomStep.mode);
        Assertions.assertEquals(TeragrepBloomStep.BloomMode.CREATE, bloomStep1.mode);
    }

    @Test
    void testTeragrepBloomUpdateTranslation() {
        final String query = "| teragrep exec bloom update table myTable regex \\w{4}";
        final CharStream inputStream = CharStreams.fromString(query);
        final DPLLexer lexer = new DPLLexer(inputStream);
        final DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        final ParseTree tree = parser.root();

        final DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");
        final DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);

        final TeragrepTransformation ct = new TeragrepTransformation(ctx, visitor);
        StepListNode stepNode = (StepListNode) ct
                .visitTeragrepTransformation((DPLParser.TeragrepTransformationContext) tree.getChild(1).getChild(0));
        final AbstractStep step = stepNode.asList().get(0); // first is an aggregation step
        final AbstractStep step1 = stepNode.asList().get(1); // second is bloom update

        Assertions.assertEquals(TeragrepBloomStep.class, step.getClass());
        Assertions.assertEquals(TeragrepBloomStep.class, step1.getClass());
        TeragrepBloomStep bloomStep = (TeragrepBloomStep) step;
        TeragrepBloomStep bloomStep1 = (TeragrepBloomStep) step1;

        Assertions.assertEquals(TeragrepBloomStep.BloomMode.AGGREGATE, bloomStep.mode);
        Assertions.assertEquals(TeragrepBloomStep.BloomMode.UPDATE, bloomStep1.mode);
    }
}
