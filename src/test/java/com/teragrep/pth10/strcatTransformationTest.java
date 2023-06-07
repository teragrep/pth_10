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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class strcatTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(strcatTransformationTest.class);

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
    
	// strcat without allRequired parameter provided (defaults to allRequired=f)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void strcatTransformTest() {
        String q = "index=index_A | strcat _raw sourcetype \"literal\" dest";

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

        // check if result contains the column that was created for strcat result
        assertTrue(Arrays.toString(res.columns()).contains("dest"));
        
        // List of expected values for the strcat destination field
        List<String> expectedValues = new ArrayList<>(Arrays.asList(
        		"raw 01A:X:0literal", "raw 02A:X:0literal", "raw 03A:Y:0literal", "raw 04A:Y:0literal", "raw 05A:Y:0literal"
        ));
        
        // Destination field from result dataset<row>
        List<String> destAsList = res.select("dest").collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
    
    // strcat with allRequired=True
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void strcatTransformAllRequiredTrueTest() {
        String q = "index=index_A | strcat allrequired=t _raw \"literal\" sourcetype dest";
        
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

        // check if result contains the column that was created for strcat result
        assertTrue(Arrays.toString(res.columns()).contains("dest"));
        
        // List of expected values for the strcat destination field
        List<String> expectedValues = new ArrayList<>(Arrays.asList(
        		"raw 01literalA:X:0", "raw 02literalA:X:0", "raw 03literalA:Y:0", "raw 04literalA:Y:0", "raw 05literalA:Y:0"
        ));
        
        // Destination field from result dataset<row>
        List<String> destAsList = res.select("dest").collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
    
    // strcat with allRequired=False
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void strcatTransformAllRequiredFalseTest() {
        String q = "index=index_A | strcat allrequired=f _raw sourcetype \"hello world\" dest";

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

        // check if result contains the column that was created for strcat result
        assertTrue(Arrays.toString(res.columns()).contains("dest"));
        
        // List of expected values for the strcat destination field
        List<String> expectedValues = new ArrayList<>(Arrays.asList(
        		"raw 01A:X:0hello world", "raw 02A:X:0hello world", "raw 03A:Y:0hello world", "raw 04A:Y:0hello world", "raw 05A:Y:0hello world"
        ));
        
        // Destination field from result dataset<row>
        List<String> destAsList = res.select("dest").collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
    
    // strcat with allRequired=True AND missing(incorrect) field
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void strcatTransformAllRequiredTrueWithMissingFieldTest() {
        String q = "index=index_A | strcat allrequired=t _raw sourcetype NOT_A_REAL_FIELD \"literal\" dest";

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

        // check if result contains the column that was created for strcat result
        assertTrue(Arrays.toString(res.columns()).contains("dest"));
        
        // List of expected values for the strcat destination field
        List<String> expectedValues = new ArrayList<>(Arrays.asList(
        		null, null, null, null, null
        ));
        
        // Destination field from result dataset<row>
        List<String> destAsList = res.select("dest").collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
    
	// strcat with allRequired=False AND missing(incorrect) field
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void strcatTransformAllRequiredFalseWithMissingFieldTest() {
        String q = "index=index_A | strcat allrequired=f _raw sourcetype \"literal\" NOT_A_REAL_FIELD dest";

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

        // check if result contains the column that was created for strcat result
        assertTrue(Arrays.toString(res.columns()).contains("dest"));
        
        // List of expected values for the strcat destination field
        List<String> expectedValues = new ArrayList<>(Arrays.asList(
        		"raw 01A:X:0literal", "raw 02A:X:0literal", "raw 03A:Y:0literal", "raw 04A:Y:0literal", "raw 05A:Y:0literal"
        ));
        
        // Destination field from result dataset<row>
        List<String> destAsList = res.select("dest").collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
     
    // strcat with allRequired=False AND three fields and two literals
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void strcatTransformWithMoreThanTwoFields() {
        String q = "index=index_A | strcat allrequired=f _raw \",\" sourcetype \",\" index dest";

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

        // check if result contains the column that was created for strcat result
        assertTrue(Arrays.toString(res.columns()).contains("dest"));
        
        // List of expected values for the strcat destination field
        List<String> expectedValues = new ArrayList<>(Arrays.asList(
        		"raw 01,A:X:0,index_A", "raw 02,A:X:0,index_A", "raw 03,A:Y:0,index_A", "raw 04,A:Y:0,index_A", "raw 05,A:Y:0,index_A"
        ));
        
        // Destination field from result dataset<row>
        List<String> destAsList = res.select("dest").collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
       
        // assert dest field contents as equals with expected contents
        assertEquals(expectedValues, destAsList); 
    }
}
