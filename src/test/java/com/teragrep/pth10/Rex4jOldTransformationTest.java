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
import com.teragrep.pth10.ast.commands.transformstatement.rex4j.NamedGroupsRex;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Rex4jOldTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(Rex4jOldTransformationTest.class);

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
    void rexTransformTest() {
        String q = "index=index_B | rex4j \"(?<TargetText>raw )\""; // original_fields.target  value = extracted-field
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            // Check schema
            String schema = "StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(TargetText,StringType,true))";
            assertEquals(schema, res.schema().toString(), visitor.getTraceBuffer().toString());

            // Check full result
            String e = "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n"
            		+ "|value                                                                                                                                                                                                                                                                                |\n"
            		+ "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n"
            		+ "|{\"_raw\":\"raw 06\",\"_time\":\"2006-06-06T06:06:06.060+03:00\",\"host\":\"computer06.example.com\",\"index\":\"index_B\",\"offset\":6,\"partition\":\"hundred-year/2006/06-06/computer06.example.com/06/06.logGLOB-2006060601.log.gz\",\"source\":\"imfile:computer06.example.com:06.log\",\"sourcetype\":\"B:X:0\",\"TargetText\":\"raw \"} |\n"
            		+ "|{\"_raw\":\"raw 07\",\"_time\":\"2007-07-07T07:07:07.070+03:00\",\"host\":\"computer07.example.com\",\"index\":\"index_B\",\"offset\":7,\"partition\":\"hundred-year/2007/07-07/computer07.example.com/07/07.logGLOB-2007070701.log.gz\",\"source\":\"imfile:computer07.example.com:07.log\",\"sourcetype\":\"B:X:0\",\"TargetText\":\"raw \"} |\n"
            		+ "|{\"_raw\":\"raw 08\",\"_time\":\"2008-08-08T08:08:08.080+03:00\",\"host\":\"computer08.example.com\",\"index\":\"index_B\",\"offset\":8,\"partition\":\"hundred-year/2008/08-08/computer08.example.com/08/08.logGLOB-2008080801.log.gz\",\"source\":\"imfile:computer08.example.com:08.log\",\"sourcetype\":\"B:X:0\",\"TargetText\":\"raw \"} |\n"
            		+ "|{\"_raw\":\"raw 09\",\"_time\":\"2009-09-09T09:09:09.090+03:00\",\"host\":\"computer09.example.com\",\"index\":\"index_B\",\"offset\":9,\"partition\":\"hundred-year/2009/09-09/computer09.example.com/09/09.logGLOB-2009090901.log.gz\",\"source\":\"imfile:computer09.example.com:09.log\",\"sourcetype\":\"B:Y:0\",\"TargetText\":\"raw \"} |\n"
            		+ "|{\"_raw\":\"raw 10\",\"_time\":\"2010-10-10T10:10:10.100+03:00\",\"host\":\"computer10.example.com\",\"index\":\"index_B\",\"offset\":10,\"partition\":\"hundred-year/2010/10-10/computer10.example.com/10/10.logGLOB-2010101001.log.gz\",\"source\":\"imfile:computer10.example.com:10.log\",\"sourcetype\":\"B:Y:0\",\"TargetText\":\"raw \"}|\n"
            		+ "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n"
            		+ "";
            
            String jsonStr = res.toJSON().showString(10, 0, false);
            assertEquals(e, jsonStr,visitor.getTraceBuffer().toString());
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

   @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformNamedGroupClassTest() {
        String q="(?<TargetText>raw )(?<TargetNumber>[0-9]+)";
        String e="{TargetText=1, TargetNumber=2}";
        try {
            Map<String,Integer> r = NamedGroupsRex.getNamedGroups(q);
            assertEquals(e, r.toString());
        } catch (Exception ex){
                ex.printStackTrace();;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformNamedMultiTest() {
        String q, e;
        List<Integer> mn = new ArrayList<>();
        String query[] = {"index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}).*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}).*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)].*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?).*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?).*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?).*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+).*\"",
                "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+) (?<responseSize>d+).*\""
        };

        q = "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+) (?<responseSize>d+).*\"";
//        q = "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+) (?<responseSize>d+) (?<serverStats>.*?).*\"";
//        q = "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}){1}:(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+) (?<responseSize>d+) (?<serverStats>.*).*\"";
        //q = "index=index_B | rex4j \"(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}){1}:(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+) (?<responseSize>d+) (?<serverStats>.*)\"(?<request>.*)\"";
        e = "StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(sourceAddress,StringType,true), StructField(sourcePort,StringType,true), StructField(timeStamp,StringType,true), StructField(frontend,StringType,true), StructField(backend,StringType,true), StructField(timings,StringType,true), StructField(httpResponseCode,StringType,true), StructField(responseSize,StringType,true), StructField(serverStats,StringType,true))";
        String testFile = "src/test/resources/rexTestData.json";
        SparkSession curSession = spark.newSession();
        Dataset<Row> df = curSession.read().json(testFile);
        ctx.setDs(df);
        ctx.setEarliest("-1Y");
//        ctx.getDs().show();
        int iter = 0;
        for(String s:query) {
            int loops = 10;
            int totaltime = 0;
            iter++;
            for (int l = 0; l < loops; l++) {
                long start = System.currentTimeMillis();
                try {
                    DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
                    CharStream inputStream = CharStreams.fromString(s); // q
                    DPLLexer lexer = new DPLLexer(inputStream);
                    // Catch also lexer-errors
                    lexer.addErrorListener(new BaseErrorListener() {
                                               @Override
                                               public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                                                   throw new IllegalStateException("failed to parse at line " + line + ":" + charPositionInLine + " due to " + msg, e);
                                               }
                                           }
                    );
                    DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
                    parser.addErrorListener(new BaseErrorListener() {
                        @Override
                        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                            throw new IllegalStateException("failed to parse at line " + line + ":" + charPositionInLine + " due to " + msg, e);
                        }
                    });

                    ParseTree tree = parser.root();
                    CatalystNode n = (CatalystNode) visitor.visit(tree);
                    Dataset<Row> res = n.getDataset();
                    long end = System.currentTimeMillis();
                    totaltime = totaltime + (int) (end - start);
                    //LOGGER.info(s+ "- Results after query" + res.schema());
                    // check chema, I.E. we get also TargetText- and TargetNumber-fields
                    //res.show();
                    String schema = res.schema().toString();
                    //assertEquals(e, schema, visitor.getTraceBuffer().toString());
                    LOGGER.info("Took " + (end - start) + "ms for "+(8+iter)+" fields");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            LOGGER.info("Mean for " + loops + "-iterations =" + totaltime / loops);
            mn.add(totaltime / loops);
        }
        for(int i=1;i<mn.size();i++) {
            LOGGER.info("added field count="+i+" mean=" + mn.get(i));
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformNamedMulti1Test() {
        String e="{sourceAddress=1, sourcePort=2, timeStamp=3, frontend=4, backend=5, timings=6, httpResponseCode=7, responseSize=8, serverStats=9, request=10}";
        String testFile = "src/test/resources/rexTestData.json";
        SparkSession curSession = spark.newSession();
        Dataset<Row> df = curSession.read().json(testFile);
        ctx.setDs(df);
        String regexp="(?<sourceAddress>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5}) [(?<timeStamp>.*?)] (?<frontend>.*?) (?<backend>.*?) (?<timings>.*?) (?<httpResponseCode>d+) (?<responseSize>d+) (?<serverStats>.*?)\"(?<request>.*)\"";
        //String regexp="(?<sourceAddress>[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}):(?<sourcePort>[0-9]{1,5})\s\[(?<timeStamp>.*?)\]\s(?<frontend>.*?)\s(?<backend>.*?)\s(?<timings>.*?)\s(?<httpResponseCode>\d+)\s(?<responseSize>\d+)\s(?<serverStats>.*?)\"(?<request>.*)\"";
        // sourceAddress, sourcePort, timeStamp, frontend, backend, timings, httpResponseCode, responseSize, serverStats, request
        String source="127.0.0.123:4567 [26/Nov/2021:07:02:44.809] https-in~ https-in/<NOSRV> 0/-1/-1/-1/0 302 104 - - LR-- 1/1/0/0/0 0/0 \"GET /Monster_boy_normal_(entity) HTTP/1.1\"";
        Column result;
        try {
            Map<String,Integer> r = NamedGroupsRex.getNamedGroups(regexp);
            assertEquals(e, r.toString());
        } catch (Exception ex){
            ex.printStackTrace();;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void aTest() {
        Dataset<Row> ds = ctx.getDs();
        ds = ds.withColumn("TextField", functions.regexp_extract(new Column("_raw"),"raw ",0));
        ds = ds.withColumn("NumField", functions.regexp_extract(new Column("_raw"),"[0-9]+",0));
        ds.show();
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformNamedFieldTest() {
        String q, e;
        //q = "index=index_B | rex4j (?<target>raw.*)"; // original_fields.target  value = extracted-field
        q = "index=index_B | rex4j \"(?<TargetText>raw )(?<TargetNumber>[0-9]+)\""; // original_fields.target  value = extracted-field

        e = "StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(TargetText,StringType,true), StructField(TargetNumber,StringType,true))";
        ctx.setEarliest("-1Y");
        try {
            DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
            CharStream inputStream = CharStreams.fromString(q);
            DPLLexer lexer = new DPLLexer(inputStream);
            DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
            ParseTree tree = parser.root();
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            LOGGER.info("Results after query" + res.toString());
            LOGGER.info("-------------------");
            // check chema, I.E. we get also TargetText- and TargetNumber-fields
            res.show();
            String schema = res.schema().toString();
            assertEquals(e, schema, visitor.getTraceBuffer().toString());

        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformNamedField1Test() {
        String q, e;
        //q = "index=index_B | rex4j (?<target>raw.*)"; // original_fields.target  value = extracted-field
        q = "index=index_B | rex4j \"(?<TargetNumber>[0-9]+)\""; // original_fields.target  value = extracted-field

        e = "StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(TargetNumber,StringType,true))";
        ctx.setEarliest("-1Y");
        try {
            DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
            CharStream inputStream = CharStreams.fromString(q);
            DPLLexer lexer = new DPLLexer(inputStream);
            DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
            ParseTree tree = parser.root();
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            LOGGER.info("Results after query" + res.toString());
            LOGGER.info("-------------------");
            // check chema, I.E. we get also TargetText- and TargetNumber-fields
            res.show();
            String schema = res.schema().toString();
            assertEquals(e, schema, visitor.getTraceBuffer().toString());

        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

    // Check error case where  group-names are missing
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformNamedField2Test() {
        String q, e;
        q = "index=index_B | rex4j \"[0-9]+\"";
        e = "StructType(StructField(_raw,StringType,true), StructField(_time,StringType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(TargetNumber,StringType,true))";
        ctx.setEarliest("-1Y");
        try {
            DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
            CharStream inputStream = CharStreams.fromString(q);
            DPLLexer lexer = new DPLLexer(inputStream);
            DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
            ParseTree tree = parser.root();
            e = "Error in rex4j command, regexp-string missing mandatory match groups.";
            Throwable exception = assertThrows(IllegalArgumentException.class, () -> visitor.visit(tree));
            assertEquals(e, exception.getMessage());
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Disabled
	@Test
    void rexTransformFieldTest() {
        String q, e;
        Column result;
        q = "index=index_B | rex4j field=data \".+ResourceAccessException:\\s+(?<ResourceAccessException>[^\\|]+)\"| rex4j \".+Exception:\\s+(?<ExceptionMsg>[^\\|]+)\"";
        e = "`index` LIKE 'index_B'";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        Dataset<Row> res = n.getDataset();
        LOGGER.info("Results after query" + res.toString());
        LOGGER.info("-------------------");
        // check that we get only _time-column
        res.show();
        assertEquals("[_time: string]", res.toString(), visitor.getTraceBuffer().toString());

        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql(), visitor.getTraceBuffer().toString());
    }

    @Disabled
	@Test
    void rexTransformQuotedFieldTest() {
        String q, e;
        Column result;
        q = "index=index_B | rex4j field=\"data\"  mode=sed raw replaced";
        e = "`index` LIKE 'index_B'";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        Dataset<Row> res = n.getDataset();
        LOGGER.info("Results after query" + res.toString());
        LOGGER.info("-------------------");
        // check that we get only _time-column
        res.show();
        assertEquals("[_time: string]", res.toString(), visitor.getTraceBuffer().toString());

        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        assertEquals(e, result.expr().sql(), visitor.getTraceBuffer().toString());
    }

//    @Test
    void rexTransformSedTest() {
        String q, e;
        Column result;
        q = "index=index_B | rex4j mode=sed raw replaced";
        e = "`index` LIKE 'index_B'";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            LOGGER.info("Results after query" + res.toString());
            LOGGER.info("-------------------");
            // check that we get only _time-column
            res.show();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void rexTransformSed1Test() {
		String q, e;
		q = " index=index_B | rex4j mode=sed \"s/raw/replaced/g\" | rex4j field=host mode=sed \"s/computer09/XXX/g\" | fields _raw host ";
		// q = "index=index_B | rex4j field=\"host\" mode=sed \"s/computer/laptop/g\""; 	////// seq mode seems to work
		ctx.setEarliest("-1Y");
		//LOGGER.info("incoming" );
		//LOGGER.info("-------------------");
		// check that we get only _time-column
		//ctx.getDs().show(15);
		DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
		CharStream inputStream = CharStreams.fromString(q);
		DPLLexer lexer = new DPLLexer(inputStream);
		DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
		ParseTree tree = parser.root();
		try {
			CatalystNode n = (CatalystNode) visitor.visit(tree);
			Dataset<Row> res = n.getDataset();
            // Check full result
            e = "+----------------------------------------------+\n" +
                    "|value                                         |\n" +
                    "+----------------------------------------------+\n" +
                    "|{\"_raw\":\"replaced 06\",\"host\":\"computer06.example.com\"}|\n" +
                    "|{\"_raw\":\"replaced 07\",\"host\":\"computer07.example.com\"}|\n" +
                    "|{\"_raw\":\"replaced 08\",\"host\":\"computer08.example.com\"}|\n" +
                    "|{\"_raw\":\"replaced 09\",\"host\":\"XXX.example.com\"}       |\n" +
                    "|{\"_raw\":\"replaced 10\",\"host\":\"computer10.example.com\"}|\n" +
                    "+----------------------------------------------+\n";
            String jsonStr = res.toJSON().showString(10, 0, false);
            assertEquals(e, jsonStr,visitor.getTraceBuffer().toString());

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformSed2Test() {
        String q, e;
        Column result;
        q = "index=index_A OR index=index_B | rex4j field=host mode=sed \"s/computer09/XXX/g\" | fields _raw host";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            // Check full result
            e = "+-----------------------------------------+\n" +
                    "|value                                    |\n" +
                    "+-----------------------------------------+\n" +
                    "|{\"_raw\":\"raw 01\",\"host\":\"computer01.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 02\",\"host\":\"computer02.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 03\",\"host\":\"computer03.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 04\",\"host\":\"computer04.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 05\",\"host\":\"computer05.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 06\",\"host\":\"computer06.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 07\",\"host\":\"computer07.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 08\",\"host\":\"computer08.example.com\"}|\n" +
                    "|{\"_raw\":\"raw 09\",\"host\":\"XXX.example.com\"}       |\n" +
                    "|{\"_raw\":\"raw 10\",\"host\":\"computer10.example.com\"}|\n" +
                    "+-----------------------------------------+\n";
            String jsonStr = res.toJSON().showString(10, 0, false);
            assertEquals(e, jsonStr,visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void rexTransformMaxMatchTest() {
        String q, e;
        Column result;
        q = "index=index_A OR index=index_B| rex4j \"(?<TargetText>raw )(?<TargetNumber>[0-9]*)\" max_match=1\n";
        ctx.setEarliest("-1Y");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            // Check full result
            e = "+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                    "|value                                                                                                                                                                                                                                                                                                    |\n" +
                    "+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                    "|{\"_raw\":\"raw 01\",\"_time\":\"2001-01-01T01:01:01.010+03:00\",\"host\":\"computer01.example.com\",\"index\":\"index_A\",\"offset\":1,\"partition\":\"hundred-year/2001/01-01/computer01.example.com/01/01.logGLOB-2001010101.log.gz\",\"source\":\"imfile:computer01.example.com:01.log\",\"sourcetype\":\"A:X:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"01\"} |\n" +
                    "|{\"_raw\":\"raw 02\",\"_time\":\"2002-02-02T02:02:02.020+03:00\",\"host\":\"computer02.example.com\",\"index\":\"index_A\",\"offset\":2,\"partition\":\"hundred-year/2002/02-02/computer02.example.com/02/02.logGLOB-2002020201.log.gz\",\"source\":\"imfile:computer02.example.com:02.log\",\"sourcetype\":\"A:X:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"02\"} |\n" +
                    "|{\"_raw\":\"raw 03\",\"_time\":\"2003-03-03T03:03:03.030+03:00\",\"host\":\"computer03.example.com\",\"index\":\"index_A\",\"offset\":3,\"partition\":\"hundred-year/2003/03-03/computer03.example.com/03/03.logGLOB-2003030301.log.gz\",\"source\":\"imfile:computer03.example.com:03.log\",\"sourcetype\":\"A:Y:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"03\"} |\n" +
                    "|{\"_raw\":\"raw 04\",\"_time\":\"2004-04-04T04:04:04.040+03:00\",\"host\":\"computer04.example.com\",\"index\":\"index_A\",\"offset\":4,\"partition\":\"hundred-year/2004/04-04/computer04.example.com/04/04.logGLOB-2004040401.log.gz\",\"source\":\"imfile:computer04.example.com:04.log\",\"sourcetype\":\"A:Y:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"04\"} |\n" +
                    "|{\"_raw\":\"raw 05\",\"_time\":\"2005-05-05T05:05:05.050+03:00\",\"host\":\"computer05.example.com\",\"index\":\"index_A\",\"offset\":5,\"partition\":\"hundred-year/2005/05-05/computer05.example.com/05/05.logGLOB-2005050501.log.gz\",\"source\":\"imfile:computer05.example.com:05.log\",\"sourcetype\":\"A:Y:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"05\"} |\n" +
                    "|{\"_raw\":\"raw 06\",\"_time\":\"2006-06-06T06:06:06.060+03:00\",\"host\":\"computer06.example.com\",\"index\":\"index_B\",\"offset\":6,\"partition\":\"hundred-year/2006/06-06/computer06.example.com/06/06.logGLOB-2006060601.log.gz\",\"source\":\"imfile:computer06.example.com:06.log\",\"sourcetype\":\"B:X:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"06\"} |\n" +
                    "|{\"_raw\":\"raw 07\",\"_time\":\"2007-07-07T07:07:07.070+03:00\",\"host\":\"computer07.example.com\",\"index\":\"index_B\",\"offset\":7,\"partition\":\"hundred-year/2007/07-07/computer07.example.com/07/07.logGLOB-2007070701.log.gz\",\"source\":\"imfile:computer07.example.com:07.log\",\"sourcetype\":\"B:X:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"07\"} |\n" +
                    "|{\"_raw\":\"raw 08\",\"_time\":\"2008-08-08T08:08:08.080+03:00\",\"host\":\"computer08.example.com\",\"index\":\"index_B\",\"offset\":8,\"partition\":\"hundred-year/2008/08-08/computer08.example.com/08/08.logGLOB-2008080801.log.gz\",\"source\":\"imfile:computer08.example.com:08.log\",\"sourcetype\":\"B:X:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"08\"} |\n" +
                    "|{\"_raw\":\"raw 09\",\"_time\":\"2009-09-09T09:09:09.090+03:00\",\"host\":\"computer09.example.com\",\"index\":\"index_B\",\"offset\":9,\"partition\":\"hundred-year/2009/09-09/computer09.example.com/09/09.logGLOB-2009090901.log.gz\",\"source\":\"imfile:computer09.example.com:09.log\",\"sourcetype\":\"B:Y:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"09\"} |\n" +
                    "|{\"_raw\":\"raw 10\",\"_time\":\"2010-10-10T10:10:10.100+03:00\",\"host\":\"computer10.example.com\",\"index\":\"index_B\",\"offset\":10,\"partition\":\"hundred-year/2010/10-10/computer10.example.com/10/10.logGLOB-2010101001.log.gz\",\"source\":\"imfile:computer10.example.com:10.log\",\"sourcetype\":\"B:Y:0\",\"TargetText\":\"raw \",\"TargetNumber\":\"10\"}|\n" +
                    "+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+\n";
            String jsonStr = res.toJSON().showString(10, 0, false);
            assertEquals(e, jsonStr,visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
