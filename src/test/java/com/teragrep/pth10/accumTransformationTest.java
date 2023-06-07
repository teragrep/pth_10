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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class accumTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(accumTransformationTest.class);

    DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor catalystVisitor;
    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestData.json";
    //String testFile = "src/test/resources/xmlWalkerTestDataWithMissingField.json"; // data with row #4 missing value for column _raw
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
    
    // --- Catalyst emit mode tests ---
    
	// accum test; results to origin field
    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void accumTransformTest() {
        String q, e;
        Column result;
                
        q = "index=index_A | accum offset";
        e = "`index` RLIKE '(?i)^index_A'";
        
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        
        Dataset<Row> res = n.getDataset();
        
        LOGGER.info("Results after query :" + q + " : " + res.toString());
        LOGGER.info("-------------------");
        res.show();
        
        //LOGGER.info("---visitor");
        //LOGGER.info(visitor.getTraceBuffer().toString());
        //LOGGER.info(visitor.getTransformPart());
        
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        
        // assert if result in sql matches expected
        assertEquals(e, result.expr().sql(), visitor.getTraceBuffer().toString());
        // check if result contains the correct amount of columns (no columns should have been added)
        assertTrue(res.columns().length == 8);       
        // List of expected values for the accum destination field
        List<Long> expectedValues = new ArrayList<>(Arrays.asList(
        		1L, 3L, 6L, 10L, 15L
        ));
        
        // Destination field from result dataset<row>
        List<Long> destAsList = res.select("offset").collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
    
    // accum test; results to new field
    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void accumTransformWithNewFieldTest() {
        String q, e;
        Column result;
                
        q = "index=index_A | accum offset AS result";
        e = "`index` RLIKE '(?i)^index_A'";
        
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        
        Dataset<Row> res = n.getDataset();
        
        LOGGER.info("Results after query :" + q + " : " + res.toString());
        LOGGER.info("-------------------");
        res.show();
        
        //LOGGER.info("---visitor");
        //LOGGER.info(visitor.getTraceBuffer().toString());
        //LOGGER.info(visitor.getTransformPart());
        
        result = visitor.getLogicalPartAsColumn();
        LOGGER.info("Query=" + q);
        LOGGER.info("Expected=" + e);
        LOGGER.info("Result=" + result.expr().sql());
        
        // assert if result in sql matches expected
        assertEquals(e, result.expr().sql(), visitor.getTraceBuffer().toString());
        // check if result contains the correct amount of columns (should have the result column added) and that the result column is named correctly
        assertTrue(res.columns().length == 9 && Arrays.toString(res.columns()).contains("result"));       
        // List of expected values for the accum destination field
        List<Long> expectedValues = new ArrayList<>(Arrays.asList(
        		1L, 3L, 6L, 10L, 15L
        ));
        
        // Destination field from result dataset<row>
        List<Long> destAsList = res.select("result").collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
}
