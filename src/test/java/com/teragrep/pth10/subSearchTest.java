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

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class subSearchTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(subSearchTest.class);

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
        spark.sparkContext().setLogLevel("INFO");
        ctx = new DPLParserCatalystContext(spark);
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
//        ctx = new DPLParserCatalystContext(spark);
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
    void endToEndSubSearch2Test() {
        String e, q;
        // First parse incoming DPL
        q="index = index_A [ search sourcetype= A:X:0 | top limit=1 host | fields + host]";
        e="`index` RLIKE '(?i)^index_A'";

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
            Dataset<Row> res = n.getDataset();
            Column col = visitor.getLogicalPartAsColumn();
            // Check that sub-query get executed and result is used as query parameter
            assertEquals(e ,visitor.getLogicalPart(),visitor.getTraceBuffer().toString());
            // Check full result
            e = "+-------------------------+\n" +
                    "|value                    |\n" +
                    "+-------------------------+\n" +
                    "|{\"host\":\"computer01.example.com\"}|\n" +
                    "+-------------------------+\n";
            String jsonStr = res.toJSON().showString(7, 0, false);
            assertEquals(e, jsonStr);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEndSubSearch3Test() {
        String e,q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]";
        e="`index` RLIKE '(?i)^index_A'";

        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        inDs.show();
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            res.show();
            Column col = visitor.getLogicalPartAsColumn();
            // Check that sub-query get executed and result is used as query parameter
            assertEquals(e ,visitor.getLogicalPart(),visitor.getTraceBuffer().toString());
            // Check full result
            e = "+-------------------------+\n" +
                    "|value                    |\n" +
                    "+-------------------------+\n" +
                    "|{\"host\":\"computer01.example.com\"}|\n" +
                    "|{\"host\":\"computer02.example.com\"}|\n" +
                    "|{\"host\":\"computer01.example.com\"}|\n" +
                    "+-------------------------+\n";
            String jsonStr = res.toJSON().showString(7, 0, false);
            assertEquals(e, jsonStr);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Disabled
	@Test
    void endToEndSubSearch4Test() {
        String e,q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A [ search sourcetype= A:X:0 | top limit=1 host | fields + host] [ search sourcetype= c:X:0| top limit=1 host | fields + host]";
//        q="index = index_A [ search sourcetype= A:X:0 | top limit=1 host | fields + host] [ search host= computer03.example.com | top limit=1 host | fields + host]";
        e="(`index` LIKE 'index_A' AND ((`_raw` LIKE '%computer01.example.com%' AND `_raw` LIKE '%computer02.example.com%') AND `_raw` LIKE '%computer01.example.com%'))";

        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        inDs.show();
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            res.show();
            Column col = visitor.getLogicalPartAsColumn();
            // Check that sub-query get executed and result is used as query parameter
            assertEquals(e ,visitor.getLogicalPart(),visitor.getTraceBuffer().toString());
            // Check full result
            e = "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                    "|value                                                                                                                                                                                                                                                                                                                                                                                                                                                     |\n" +
                    "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                    "|{\"_raw\":\"127.0.0.123:4567 [26/Nov/2021:07:02:44.809] https-in~ https-in/<NOSRV> 0/-1/-1/-1/0 302 104 - - LR-- 1/1/0/0/0 0/0 \\\"GET /Monster_boy_normal_(entity) HTTP/1.1\\\" A:X:0 computer01.example.com computer02.example.com\",\"_time\":\"2001-01-01T01:01:01.011+03:00\",\"host\":\"computer02.example.com\",\"index\":\"index_A\",\"offset\":1,\"partition\":\"hundred-year/2001/01-01/computer01.example.com/01/01.logGLOB-2001010101.log.gz\",\"source\":\"imfile:computer01.example.com:01.log\",\"sourcetype\":\"A:X:0\"}|\n" +
                    "+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n";
            String jsonStr = res.toJSON().showString(7, 0, false);
            assertEquals(e, jsonStr);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEndSearchTest() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="sourcetype=A:X:0| top limit=2 host | fields + host";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/xmlWalkerTestData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        inDs.show();
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show();

            String head=res.head().getString(0);
            List<Row> lst = res.collectAsList();
            lst.forEach(item->{
                LOGGER.info("item value="+item.getString(0));
            });
            // Correct  item count
            assertEquals(2,lst.size(),visitor.getTraceBuffer().toString());
            assertEquals("computer01.example.com",lst.get(0).getString(0),visitor.getTraceBuffer().toString());
            assertEquals("computer02.example.com",lst.get(1).getString(0),visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEndSearch1Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="index = index_A AND computer01.example.com AND computer02.example.com";
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
            res.show();
            boolean aggregates = visitor.getAggregatesUsed();
            assertFalse(aggregates,visitor.getTraceBuffer().toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void endToEndSearch3Test() {
        String q;
        // First parse incoming DPL
        // check that timechart returns also aggregatesUsed=true
        q="sourcetype=c:X:0| top limit=1 host | fields + host";
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/subsearchData.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        inDs.show();
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        System.currentTimeMillis();
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            LOGGER.info("Logical part ="+visitor.getLogicalPart());
            Dataset<Row> res = n.getDataset();
            res.show();
            List<Row> lst = res.collectAsList();
            lst.forEach(item->{
                LOGGER.info("item value="+item.getString(0));
            });
            // Correct  item count
            assertEquals(1,lst.size(),visitor.getTraceBuffer().toString());
            assertEquals("computer03.example.com",lst.get(0).getString(0),visitor.getTraceBuffer().toString());
            boolean aggregates = visitor.getAggregatesUsed();
            assertFalse(aggregates,visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }


}

