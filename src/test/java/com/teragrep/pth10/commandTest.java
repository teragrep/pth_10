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
package com.teragrep.pth10;

import com.teragrep.pth10.ast.DPLAuditInformation;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class commandTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(commandTest.class);

    DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor catalystVisitor;
    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestData.json";
    SparkSession spark = null;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ctx = new DPLParserCatalystContext(spark);
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        // initialize test dataset
        SparkSession curSession = spark.newSession();
        Dataset<Row> df = curSession.read().json(testFile);
        ctx.setDs(df);
    }


    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        catalystVisitor = null;
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void explainTest() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index=index_A sourcetype= A:X:0 | top limit=1 host | fields + host |explain ";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

   // @Test
    void syslogStreamTest() {
        String q;
        q = "index=index_A | teragrep exec syslog stream";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void explain1Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index=index_A sourcetype= A:X:0 | top limit=1 host | fields + host |explain extended";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            LOGGER.info("result");
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void explain2Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|explain extended";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void auditTest() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host] | explain extended";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        LOGGER.debug(tree.toStringTree(parser));

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        DPLAuditInformation auditInfo = new DPLAuditInformation();
        auditInfo.setQuery(q);
        auditInfo.setUser("TestUser");
        auditInfo.setReason("Testing audit log");
        ctx.setAuditInformation(auditInfo);
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
            DPLAuditInformation ainf = visitor.getCatalystContext().getAuditInformation();
            // Check auditInformation
            assertEquals("TestUser",ainf.getUser());
            assertEquals(q,ainf.getQuery());
            assertEquals("Testing audit log",ainf.getReason());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void teragrepTest() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index=index_A sourcetype= A:X:0 | top limit=1 host | fields + host | teragrep get system version";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void teragrep_Issue149_Test() {
        String q = " | teragrep get system version";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // dataset commented out to test fake stream
        //String testFile = "src/test/resources/subsearchData.json";
        //Dataset<Row> inDs = spark.read().json(testFile);
        //ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        //System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
            
            List<Row> rawCol = res.select("_raw").collectAsList();
            
            for (Row r : rawCol) {
            	// _ raw should contain TG version information
            	// teragrep.XXX_XX.version: X.X.X
            	// Teragrep version: X.X.X
            	assertTrue(r.getAs(0).toString().contains("version:"));
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void dplTest() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|dpl debug=parsetree subsearch=true";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        String[] ruleNames=parser.getRuleNames();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);

        List<String> ruleNamesList = Arrays.asList(parser.getRuleNames());
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        ctx.setRuleNames(ruleNames);
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void dpl2Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|dpl debug=parsetree subsearch=false";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        String[] ruleNames=parser.getRuleNames();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);

        List<String> ruleNamesList = Arrays.asList(parser.getRuleNames());
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        ctx.setRuleNames(ruleNames);
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void dpl3Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]|dpl debug=parsetree";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        String[] ruleNames=parser.getRuleNames();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);

        List<String> ruleNamesList = Arrays.asList(parser.getRuleNames());
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        ctx.setRuleNames(ruleNames);
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void dpl4Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]  [ search sourcetype= c:X:0| top limit=1 host | fields + host] |dpl debug=parsetree subsearch=true";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        String[] ruleNames=parser.getRuleNames();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);

        List<String> ruleNamesList = Arrays.asList(parser.getRuleNames());
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        ctx.setRuleNames(ruleNames);
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show(false);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

}

