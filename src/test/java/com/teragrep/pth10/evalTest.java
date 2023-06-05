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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class evalTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(evalTest.class);

    DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor catalystVisitor;
    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestData.json";
    SparkSession spark = null;

    private static TimeZone originalTimeZone = null;

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

        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        // initialize test dataset
        SparkSession curSession = spark.newSession();
        Dataset<Row> df = curSession.read().json(testFile);
        ctx.setDs(df);

        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
    }

    @AfterAll
    void recoverTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        catalystVisitor = null;
    }

    @Disabled
	@Test
    public void parseEvalIfTest() throws Exception {
        String q = "index=voyager | eval err = if ( error == 200, \"OK\", \"Error\")";
        String e = "SELECT IF (error == 200, \"OK\", \"Error\") AS err FROM `temporaryDPLView` WHERE index LIKE\"voyager\"";
        String result = null;
        //utils.printDebug(e,result);
        assertEquals(e,result);
    }

    @Disabled
	@Test
    public void parseEvalIfEvalTest() throws Exception {
        String q,e,result;
        // IF with eval
        q = "index=voyager | eval err = if( substr(raw,115,5) == \"0 pol\", \"null\", \"x>0\")";
        e = "SELECT IF (SUBSTR(raw, 115, 5) == \"0 pol\", \"null\", \"x>0\") AS err FROM `temporaryDPLView` WHERE index LIKE \"voyager\"";
        result = null;
        assertEquals(e,result);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseEvalIfWhereTest() throws Exception {
        String q,e,result;

        // IF with where and using true/false method mapped to SQL true/false values
        q = "index=voyager | where if ( substr(_raw,0,14) == \"127.0.0.49\", true(), false())";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" IF (SUBSTR(_raw, 0, 14) == \"127.0.0.49\", true, false)";
        result = utils.getQueryAnalysis(q);
        //utils.printDebug(e,result);
        assertEquals(e,result);
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalLenCatalystTest() throws Exception {
        String q = "index=index_A | eval lenField = len(_raw)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(lenField,IntegerType,true))";
        String result;
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
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            res.show(false);
            // check schema, ie. we get extra field named lenField
            String resSchema=res.schema().toString();
            assertEquals(e,resSchema,visitor.getTraceBuffer().toString());
            //  Get only distinct lenField and sort it by value
            Dataset<Row> orderedDs = res.select("lenField").orderBy("lenField").distinct();
            //orderedDs.show();
            List<Row> lst = orderedDs.collectAsList();
            // we should get 3 distinct values
            assertEquals(3,lst.size(),visitor.getTraceBuffer().toString());
            // Compare values
            assertEquals(154,lst.get(0).getInt(0),visitor.getTraceBuffer().toString());
            assertEquals(175,lst.get(1).getInt(0),visitor.getTraceBuffer().toString());
            assertEquals(190,lst.get(2).getInt(0),visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test upper(x) lower(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalUpperLowerCatalystTest() throws Exception {
        String q = "index=index_A | eval a=upper(\"hello world\") | eval b=lower(\"HELLO WORLD\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " +
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,false), StructField(b,StringType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            assertEquals("HELLO WORLD", lstA.get(0));
            assertEquals("hello world", lstB.get(0));
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // test eval method urldecode()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalUrldecodeCatalystTest() throws Exception {
        String q = "index=index_A | eval a=urldecode(\"http%3A%2F%2Fwww.example.com%2Fdownload%3Fr%3Dlatest\") | eval b=urldecode(\"https%3A%2F%2Fwww.longer-domain-here.example.com%2Fapi%2Fv1%2FgetData%3Fmode%3Dall%26type%3Dupdate%26random%3Dtrue\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " +
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true), StructField(b,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            assertEquals("http://www.domain.example.com/download?r=latest", lstA.get(0));
            assertEquals("https://www.longer-domain-here.example.com/api/v1/getData?mode=all&type=update&random=true", lstB.get(0));
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // test ltrim() rtrim() trim()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTrimCatalystTest() throws Exception {
        String q = "index=index_A | eval a=ltrim(\" \t aabbccdd \") | eval b=ltrim(\"  zZaabcdzz \",\" zZ\") " +
        		 	"| eval c=rtrim(\"\t abcd  \t\") | eval d=rtrim(\" AbcDeF g\",\"F g\") | eval e=trim(\"\tabcd\t\") | eval f=trim(\"\t zzabcdzz \t\",\"\t zz\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " +
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,false), StructField(b,StringType,false), " +
        "StructField(c,StringType,false), StructField(d,StringType,false), StructField(e,StringType,false), StructField(f,StringType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // ltrim()
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // rtrim()
            // Get column 'c'
            Dataset<Row> resC = res.select("c").orderBy("c").distinct();
            List<String> lstC = resC.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'd'
            Dataset<Row> resD = res.select("d").orderBy("d").distinct();
            List<String> lstD = resD.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // trim()
            // Get column 'e'
            Dataset<Row> resE = res.select("e").orderBy("e").distinct();
            List<String> lstE = resE.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'f'
            Dataset<Row> resF = res.select("f").orderBy("f").distinct();
            List<String> lstF = resF.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            
            // ltrim()
            assertEquals("aabbccdd ", lstA.get(0));
            assertEquals("aabcdzz ", lstB.get(0));
            
            // rtrim()
            assertEquals("\t abcd", lstC.get(0));
            assertEquals(" AbcDe", lstD.get(0));
            
            // trim()
            assertEquals("abcd", lstE.get(0));
            assertEquals("abcd", lstF.get(0));
            
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method replace(x,y,z)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalReplaceCatalystTest() throws Exception {
        String q = "index=index_A | eval a=replace(\"Hello world\", \"He\", \"Ha\") | eval b=replace(a, \"world\", \"welt\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " +
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,false), StructField(b,StringType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            assertEquals("Hallo world", lstA.get(0));
            assertEquals("Hallo welt", lstB.get(0));
            
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalSubstringCatalystTest() throws Exception {
        String q = "index=index_A | eval str = substr(_raw,0,14)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(str,StringType,true))";
        String result;
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
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            //res.show(false);
            // check schema, ie. we get extra field named lenField
            String resSchema=res.schema().toString();
            assertEquals(e,resSchema,visitor.getTraceBuffer().toString());
            //  Get only distinct lenField and sort it by value
            Dataset<Row> orderedDs = res.select("str").orderBy("str").distinct();
            orderedDs.show();
            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values
            assertEquals(1,lst.size(),visitor.getTraceBuffer().toString());
            // Compare values
            assertEquals("127.0.0.123:45",lst.get(0).getString(0),visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalIfCatalystTest() throws Exception {
		String q = "index=index_A | eval val2=if((false() OR true()), \"a\", \"b\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(val2,StringType,false))";
        String result;
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
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            Dataset<Row> res = n.getDataset();
            //res.show(false);
            // check schema, ie. we get extra field named val2
            String resSchema=res.schema().toString();
            assertEquals(e,resSchema,visitor.getTraceBuffer().toString());
            //  Get only distinct val2 and sort it by value
            Dataset<Row> orderedDs = res.select("val2").orderBy("val2").distinct();
            orderedDs.show();

			List<Row> lst = orderedDs.collectAsList();
			// we should get 1 distinct values
			assertEquals(1,lst.size(),visitor.getTraceBuffer().toString());
			// Compare values
			assertEquals("a",lst.get(0).getString(0),visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalIfCatalyst1Test() throws Exception {
		String q = "index=index_A | eval val2=if( 1 < 2  , substr(_raw,161,100) , \"b\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(val2,StringType,true))";
        String result;
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
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            //res.show(false);
            // check schema, ie. we get extra field named lenField
            String resSchema=res.schema().toString();
            assertEquals(e,resSchema,visitor.getTraceBuffer().toString());
            //  Get only distinct lenField and sort it by value
            Dataset<Row> orderedDs = res.select("val2","host").orderBy("val2").distinct();
            orderedDs.show(false);

			List<Row> lst = orderedDs.collectAsList();
			// we should get 8 distinct values
			assertEquals(8,lst.size(),visitor.getTraceBuffer().toString());
			// Compare values
			assertEquals(" computer01.example.com",lst.get(4).getString(0),visitor.getTraceBuffer().toString());
            assertEquals(" computer01.example.com cOmPuter02.example.com",lst.get(5).getString(0),visitor.getTraceBuffer().toString());
            assertEquals(" computer02.example.com",lst.get(6).getString(0),visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalLen1Test() throws Exception {
        String q = "index=index_A | eval a=if(substr(_raw,0,7)=\"127.0.0.123\",len( _raw), 0)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,IntegerType,true))";
        String result;
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
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            //res.show(false);
            // check schema
            String resSchema=res.schema().toString();
            assertEquals(e,resSchema,visitor.getTraceBuffer().toString());
            //  Get only distinct field and sort it by value
            Dataset<Row> orderedDs = res.select("a").orderBy("a").distinct();
            //orderedDs.show(false);
            List<Row> lst = orderedDs.collectAsList();
            // check result count
            assertEquals(1,lst.size(),visitor.getTraceBuffer().toString());
            // Compare values
            assertEquals(213,lst.get(0).getInt(0),visitor.getTraceBuffer().toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function null()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalNullCatalystTest() throws Exception {
        String q = "index=index_A | eval a=null()";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(origin,StringType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
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
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema=res.schema().toString();
            assertEquals(e,resSchema,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get only distinct field 'a' and sort it by value
            Dataset<Row> orderedDs = res.select("a").orderBy("a").distinct();
            orderedDs.show();
            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values (all should be null)
            assertEquals(1,lst.size(),visitor.getTraceBuffer().toString());
            // Compare values (is it null?)
            assertEquals(null,lst.get(0).getString(0),visitor.getTraceBuffer().toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function pow()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalPowCatalystTest() throws Exception {
        String q = "index=index_A | eval a=pow(offset,2)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
            		"1.0", "4.0", "9.0", "16.0", "25.0", "36.0",
            		"49.0", "64.0", "81.0", "100.0", "121.0", "144.0", 
            		"169.0", "196.0", "225.0", "256.0", "1.0", "1.0", "1.0");
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function nullif()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalNullifCatalystTest() throws Exception {
        String q = "index=index_A | eval a=nullif(offset,_raw)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
            		"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","1","1","1");
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function abs()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalAbsCatalystTest() throws Exception {
        String q = "index=index_A | eval a=abs(offset)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
            		"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","1","1","1");
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method ceiling(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCeilingCatalystTest() throws Exception {
        String q = "index=index_A | eval a=ceiling(offset+0.5)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Long> expectedLst = new ArrayList<>();
            
            for (Long val : srcLst) {
            	Double v = Math.ceil(val.doubleValue() + 0.5d);
            	expectedLst.add(v.longValue());
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method exp(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalExpCatalystTest() throws Exception {
        String q = "index=index_A | eval a=exp(offset)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            
            for (Long val : srcLst) {
            	Double v = Math.exp(val.doubleValue());
            	expectedLst.add(v);
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method floor(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalFloorCatalystTest() throws Exception {
        String q = "index=index_A | eval a=floor(offset+0.5)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Long> expectedLst = new ArrayList<>();
            
            for (Long val : srcLst) {
            	Double v = Math.floor(val.doubleValue() + 0.5d);
            	expectedLst.add(v.longValue());
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method ln(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalLnCatalystTest() throws Exception {
        String q = "index=index_A | eval a=ln(offset)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            
            for (Long val : srcLst) {
            	Double v = Math.log(val.doubleValue());
            	expectedLst.add(v);
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method log(x,y)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalLogCatalystTest() throws Exception {
        String q = "index=index_A | eval a=log(offset, 10)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        // for rounding since there are small deviations between spark log10 and java log10
        final DecimalFormat df = new DecimalFormat("0.00000000");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            
            // Round both column 'a' contents and the expected
            for (int i = 0; i < lst.size(); i++){
            	Double v = lst.get(i);
            	v = Double.valueOf(df.format(v));
            	lst.set(i, v);
            }
            
            for (Long val : srcLst) {
            	Double v = Math.log10(val.doubleValue());
            	expectedLst.add(Double.valueOf(df.format(v)));
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test random()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalRandomCatalystTest() throws Exception {
        String q = "index=index_A | eval a=random()";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,IntegerType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<Integer> lst = resA.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
            
            // Check that random() produced a result within set limits
            assertTrue(lst.get(0) <= Math.pow(2d, 31d) - 1);
            assertTrue(lst.get(0) >= 0);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method pi()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalPiCatalystTest() throws Exception {
        String q = "index=index_A | eval a=pi()";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            assertEquals(3.14159265358d, lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method round(x,y)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalRoundCatalystTest() throws Exception {
        String q = "index=index_A | eval a=round(1.545) | eval b=round(5.7432, 3)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " +
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true), StructField(b,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Double> lstA = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            assertEquals(2d, lstA.get(0));
            assertEquals(5.743d, lstB.get(0));
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method sigfig(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalSigfigCatalystTest() throws Exception {
        String q = "index=index_A | eval a=sigfig(1.00 * 1111) | eval b=sigfig(offset - 1.100) | eval c=sigfig(offset * 1.234) | eval d=sigfig(offset / 3.245)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " +
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true), StructField(b,DoubleType,true), " + 
        "StructField(c,DoubleType,true), StructField(d,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            /*
             *  eval a=sigfig(1.00 * 1111) | eval b=sigfig(offset - 1.100) | eval c=sigfig(offset * 1.234) | eval d=sigfig(offset / 3.245)
             */
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            List<Double> lstA = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b");
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'c'
            Dataset<Row> resC = res.select("c");
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'd'
            Dataset<Row> resD = res.select("d");
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            boolean isOfEqualSize = (lstA.size() == lstB.size()) && (lstB.size() == lstC.size()) && (lstC.size() == lstD.size());
            assertTrue(isOfEqualSize);
            
            for (int i = 0 ; i < lstA.size() ; i++) {
            	assertFalse(Double.isNaN(lstA.get(i)));
            	assertFalse(Double.isNaN(lstB.get(i)));
            	assertFalse(Double.isNaN(lstC.get(i)));
            	assertFalse(Double.isNaN(lstD.get(i)));
            	
            	// 1.00 * 1111 => 1110(.0)
            	assertEquals(1110d, lstA.get(i));
            }
            
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method sqrt(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalSqrtCatalystTest() throws Exception {
        String q = "index=index_A | eval a=sqrt(offset)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
          
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            //resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get col offset
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // Assert
            for (int i = 0; i < srcLst.size(); i++) {
            	assertEquals(Math.sqrt(srcLst.get(i)), lst.get(i));
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval concat ab+cd
    // TODO concat breaks arithmetic, fix
    @Disabled
	@Test
    public void parseEvalConcatCatalystTest() throws Exception {
        String q = "index=index_A | eval a=\"ab\"+\"cd\"";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // we should get 1
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals("abcd", lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval plus
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalPlusCatalystTest() throws Exception {
        String q = "index=index_A | eval a=0.1+1.4";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // we should get 1
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals(1.5d, lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval minus
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMinusCatalystTest() throws Exception {
        String q = "index=index_A | eval a = offset - 1";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
           
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
            	assertEquals(lstOffset.get(i)-1, lst.get(i));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval multiply
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMultiplyCatalystTest() throws Exception {
        String q = "index=index_A | eval a = offset * offset";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
           
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
            	assertEquals(lstOffset.get(i) * lstOffset.get(i), lst.get(i));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval divide
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalDivideCatalystTest() throws Exception {
        String q = "index=index_A | eval a = offset / offset";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
           
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
            	assertEquals((double)lstOffset.get(i)/(double)lstOffset.get(i), lst.get(i));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval mod (%)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalModCatalystTest() throws Exception {
        String q = "index=index_A | eval a = offset % 2";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
           
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
            	assertEquals(lstOffset.get(i) % 2, lst.get(i));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test cryptographic functions: md5, sha1, sha256, sha512
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCryptographicCatalystTest() throws Exception {
        String q = "index=index_A | eval md5=md5(_raw) | eval sha1=sha1(_raw) | eval sha256=sha256(_raw) | eval sha512=sha512(_raw)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(md5,StringType,true), StructField(sha1,StringType,true), StructField(sha256,StringType,true), StructField(sha512,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column '_raw'
            Dataset<Row> resRaw = res.select("_raw");
            List<String> lstRaw = resRaw.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Get column 'md5'
            Dataset<Row> resmd5 = res.select("md5");
            List<String> lstMd5 = resmd5.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
   
            // Get column 'sha1'
            Dataset<Row> ress1 = res.select("sha1");
            List<String> lstS1 = ress1.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
               
            // Get column 'sha256'
            Dataset<Row> ress256 = res.select("sha256");
            List<String> lstS256 = ress256.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
                   
            // Get column 'sha512'
            Dataset<Row> ress512 = res.select("sha512");
            List<String> lstS512 = ress512.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Assert expected to result
            
            // Amount of data
            assertEquals(lstRaw.size(), lstMd5.size());
            assertEquals(lstRaw.size(), lstS1.size());
            assertEquals(lstRaw.size(), lstS256.size());
            assertEquals(lstRaw.size(), lstS512.size());
            
            // Contents
            for (int i = 0; i < lstRaw.size(); ++i) {
            	assertEquals(DigestUtils.md5Hex(lstRaw.get(i)), lstMd5.get(i));
            	assertEquals(DigestUtils.sha1Hex(lstRaw.get(i)), lstS1.get(i));
            	assertEquals(DigestUtils.sha256Hex(lstRaw.get(i)), lstS256.get(i));
            	assertEquals(DigestUtils.sha512Hex(lstRaw.get(i)), lstS512.get(i));
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function case(x1,y1,x2,y2, ..., xn, yn)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCaseCatalystTest() throws Exception {
        String q = "index=index_A | eval a=case(offset < 2, \"Less than two\", offset > 2, \"More than two\", offset == 2, \"Exactly two\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
            		"Less than two","Exactly two","More than two","More than two","More than two",
            		"More than two","More than two","More than two","More than two","More than two",
            		"More than two","More than two","More than two","More than two","More than two",
            		"More than two","Less than two","Less than two","Less than two");
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function validate(x1,y1,x2,y2, ..., xn, yn)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalValidateCatalystTest() throws Exception {
        String q = "index=index_A | eval a=validate(offset < 10, \"Not less than 10\", offset < 9, \"Not less than 9\", offset < 6, \"Not less than 6\", offset > 0, \"Not more than 0\", offset == 0, \"Not 0\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
            		"Not 0","Not 0","Not 0","Not 0","Not 0",
            		"Not less than 6","Not less than 6","Not less than 6","Not less than 9",
            		"Not less than 10","Not less than 10","Not less than 10","Not less than 10",
            		"Not less than 10","Not less than 10","Not less than 10","Not 0","Not 0","Not 0"
            		);
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method tostring(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTostring_NoOptionalArgument_CatalystTest() throws Exception {
        String q = "index=index_A | eval a=tostring(true())";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            LOGGER.info("schema=" + resSchema);
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> orderedDs = res.select("a").orderBy("a").distinct();
            orderedDs.show();
            List<String> lst = orderedDs.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
           
            
            // we should get one result because all = "true"
            assertEquals(1, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList("true");
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method tostring(x,y="hex")
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTostring_Hex_CatalystTest() throws Exception {
        String q = "index=index_A | eval a=tostring(offset, \"hex\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            LOGGER.info("schema=" + resSchema);
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
           
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // check amount of results
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = new ArrayList<>();
            
            for (Long item : srcLst) {
            	expectedLst.add("0x".concat(Integer.toHexString(item.intValue()).toUpperCase()));
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method tostring(x,y="duration")
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTostring_Duration_CatalystTest() throws Exception {
        String q = "index=index_A | eval a=tostring(offset, \"duration\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            LOGGER.info("schema=" + resSchema);
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
           
            // Get column 'offset'
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // check amount of results
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = new ArrayList<>();
            
            for (Long item : srcLst) {
            	expectedLst.add("00:00:".concat(item < 10 ? "0".concat(item.toString()) : item.toString()));
            }
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method tostring(x,y="commas")
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTostring_Commas_CatalystTest() throws Exception {
        String q = "index=index_A | eval a=tostring(12345.6789, \"commas\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            LOGGER.info("schema=" + resSchema);
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
           
            // we should get one result
            assertEquals(1, lst.size());
            // Compare values to expected            
            assertEquals("12,345.68", lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method tonumber(numstr, base)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTonumberCatalystTest() throws Exception {
        String q = "index=index_A | eval a=tonumber(\"0A4\", 16)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,IntegerType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            LOGGER.info("schema=" + resSchema);
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<Integer> lst = resA.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
           
            // we should get one result
            assertEquals(1, lst.size());
            // Compare values to expected            
            assertEquals(164, lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method tonumber(numstr)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTonumberNoBaseArgumentCatalystTest() throws Exception {
        String q = "index=index_A | eval a=tonumber(\"12345\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,IntegerType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            LOGGER.info("schema=" + resSchema);
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<Integer> lst = resA.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
           
            // we should get one result
            assertEquals(1, lst.size());
            // Compare values to expected            
            assertEquals(12345, lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function acos, acosh, cos, cosh
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCosCatalystTest() throws Exception {
        String q = "index=index_A | eval a=acos(offset / 10) | eval b=acosh(offset) | eval c=cos(offset / 10) | eval d=cosh(offset / 10)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " +
        "StructField(sourcetype,StringType,true), StructField(a,DoubleType,true), StructField(b,DoubleType,true), StructField(c,DoubleType,true), StructField(d,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            //resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b");
            //resB.show();
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // c, cos
            Dataset<Row> resC = res.select("c");
            //resC.show();
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // d, cosh
            Dataset<Row> resD = res.select("d");
            //resD.show();
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get source column
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            assertEquals(19, lstB.size());
            assertEquals(19, lstC.size());
            assertEquals(19, lstD.size());
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            List<Double> expectedLstB = new ArrayList<>();
            List<Double> expectedLstC = new ArrayList<>();
            List<Double> expectedLstD = new ArrayList<>();
            
            org.apache.commons.math3.analysis.function.Acosh acoshFunction = 
            		new org.apache.commons.math3.analysis.function.Acosh();
            
            for (Long val : srcLst) {
            	expectedLst.add(Math.acos(Double.valueOf((double)val/10d)));
            	expectedLstB.add(acoshFunction.value(Double.valueOf(val)));
            	expectedLstC.add(Math.cos(Double.valueOf((double)val/10d)));
            	expectedLstD.add(Math.cosh(Double.valueOf((double)val/10d)));
            }
            
            assertEquals(expectedLst, lst);
            assertEquals(expectedLstB, lstB);
            assertEquals(expectedLstC, lstC);
            assertEquals(expectedLstD, lstD);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function asin, asinh, sin, sinh
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalSinCatalystTest() throws Exception {
        String q = "index=index_A | eval a=asin(offset / 10) | eval b=asinh(offset) | eval c=sin(offset / 10) | eval d=sinh(offset / 10)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,true), StructField(b,DoubleType,true), " +
        "StructField(c,DoubleType,true), StructField(d,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            //resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b");
            //resB.show();
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'c'
            Dataset<Row> resC = res.select("c");
            //resC.show();
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'd'
            Dataset<Row> resD = res.select("d");
            //resD.show();
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get source column
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            assertEquals(19, lstB.size());
            assertEquals(19, lstC.size());
            assertEquals(19, lstD.size());
            
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            List<Double> expectedLstB = new ArrayList<>();
            List<Double> expectedLstC = new ArrayList<>();
            List<Double> expectedLstD = new ArrayList<>();
            
            org.apache.commons.math3.analysis.function.Asinh asinhFunction =
            		new org.apache.commons.math3.analysis.function.Asinh();
            
            for (Long val : srcLst) {
            	expectedLst.add(Math.asin(Double.valueOf((double)val/10d)));
            	expectedLstB.add(asinhFunction.value(Double.valueOf(val)));
            	expectedLstC.add(Math.sin(Double.valueOf((double)val/10d)));
            	expectedLstD.add(Math.sinh(Double.valueOf((double)val/10d)));
            }
            assertEquals(expectedLst, lst);
            assertEquals(expectedLstB, lstB);
            assertEquals(expectedLstC, lstC);
            assertEquals(expectedLstD, lstD);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function tan, tanh, atan, atanh, atan2
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTanCatalystTest() throws Exception {
        String q = "index=index_A | eval a=atan(offset) | eval b=atanh(offset / 10) | eval c=tan(offset / 10) | eval d=tanh(offset / 10) | eval e=atan2(offset / 10, offset / 20)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " + 
        "StructField(sourcetype,StringType,true), StructField(a,DoubleType,true), StructField(b,DoubleType,true), StructField(c,DoubleType,true), StructField(d,DoubleType,true), " +
        "StructField(e,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a' atan
            Dataset<Row> resA = res.select("a");
            //resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get column 'b' atanh
            Dataset<Row> resB = res.select("b");
            //resB.show();
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // c tan
            Dataset<Row> resC = res.select("c");
            //resC.show();
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // d tanh
            Dataset<Row> resD = res.select("d");
            //resD.show();
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // e atan2
            Dataset<Row> resE = res.select("e");
            //resE.show();
            List<Double> lstE = resE.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get source column
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(srcLst.size(), lst.size());
            assertEquals(srcLst.size(), lstB.size());
            assertEquals(srcLst.size(), lstC.size());
            assertEquals(srcLst.size(), lstD.size());
            assertEquals(srcLst.size(), lstE.size());
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            List<Double> expectedLstB = new ArrayList<>();
            List<Double> expectedLstC = new ArrayList<>();
            List<Double> expectedLstD = new ArrayList<>();
            List<Double> expectedLstE = new ArrayList<>();
            
            org.apache.commons.math3.analysis.function.Atanh atanhFunction =
            		new org.apache.commons.math3.analysis.function.Atanh();
            
            for (Long val : srcLst) {
            	expectedLst.add(Math.atan(Double.valueOf(val))); // atan
            	expectedLstB.add(atanhFunction.value(Double.valueOf((double)val/10d))); // atanh
            	expectedLstC.add(Math.tan(Double.valueOf((double)val/10d))); // tan
            	expectedLstD.add(Math.tanh(Double.valueOf((double)val/10d))); // tanh
            	expectedLstE.add(Math.atan2(Double.valueOf((double)val/10d), Double.valueOf((double)val/20d))); // atan
            }            
            assertEquals(expectedLst, lst);
            assertEquals(expectedLstB, lstB);
            assertEquals(expectedLstC, lstC);
            assertEquals(expectedLstD, lstD);
            assertEquals(expectedLstE, lstE);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method hypot(x,y)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalHypotCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=hypot(offset, offset)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
    	"StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " + 
        "StructField(sourcetype,StringType,true), StructField(a,DoubleType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            //resA.show();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());
            
            // Get source column
            Dataset<Row> resOffset = res.select("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(srcLst.size(), lst.size());

            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();

            for (Long val : srcLst) {
            	expectedLst.add(Math.hypot(Double.valueOf(val), Double.valueOf(val))); 
            }            
            
            assertEquals(expectedLst, lst);

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval function cidrmatch
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCidrmatchCatalystTest() throws Exception {
        String q = "index=index_A | eval a=cidrmatch(ip, \"192.168.2.0/24\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,BooleanType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
            		true, false, true
            		);
            
            
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method coalesce(x, ...)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCoalesceCatalystTest() throws Exception {
        String q = "index=index_A | eval a=coalesce(null(), index) | eval b=coalesce(index, null()) | eval c=coalesce(null())";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        			"StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        			"StructField(source,StringType,true), StructField(sourcetype,StringType,true), " + 
        			"StructField(a,StringType,true), StructField(b,StringType,true), StructField(c,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            //resA.show();
            List<Row> lst = resA.collectAsList();
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            //resB.show();
            List<Row> lstB = resB.collectAsList();
            
            // Get column 'c'
            Dataset<Row> resC = res.select("c").orderBy("c").distinct();
            //resC.show();
            List<Row> lstC = resC.collectAsList();
           
            // Compare values to expected
            assertEquals(1, lst.size());
            assertEquals(1, lstB.size());
            assertEquals(1, lstC.size());
            assertEquals("index_A", lst.get(0).getString(0));
            assertEquals("index_A", lstB.get(0).getString(0));
            assertEquals(null, lstC.get(0).getString(0));
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method in(field, value_list)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalInCatalystTest() throws Exception {
        String q = "index=index_A | eval a=in(ip,\"192.168.2.1\",\"127.0.0.91\", \"127.0.0.1\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,BooleanType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
            		true, false, true
            		);
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method like(text, pattern)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalLikeCatalystTest() throws Exception {
        String q = "index=index_A | eval a=like(ip,\"192.168.3%\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,BooleanType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
            		false, true, false
            		);
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method match(subject, regex)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMatchCatalystTest() throws Exception {
        String q = "index=index_A | eval a=match(ip,\"^\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,BooleanType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a");
            resA.show();
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());
            
            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
            		true, true, true
            		);
            
            assertEquals(expectedLst, lst);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvfind(mvfield, "regex")
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMvfindCatalystTest() throws Exception {
        String q = "index=index_A | eval a=mvfind(mvappend(\"random\",\"192.168.1.1\",\"192.168.10.1\"),\"^\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,IntegerType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<Integer> lst = resA.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals(1, lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvindex(mvfield, startindex [,endindex])
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMvindexCatalystTest() throws Exception {
        String q = "index=index_A | eval a=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"), 2) " +
        		   				 "| eval b=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"), 2, 3) ";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " +
        		"StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,false),true), StructField(b,ArrayType(StringType,false),true))";
        		String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals("[mv3, mv4, mv5]", lst.get(0).toString());
            assertEquals("[mv3, mv4]", lstB.get(0).toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvjoin(mvfield, str)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMvjoinCatalystTest() throws Exception {
        String q = "index=index_A | eval a=mvjoin(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"), \";;\") " +
        		   				 "<!--| eval b=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"), 2, 3)--> ";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            res.show(false);
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals("mv1;;mv2;;mv3;;mv4;;mv5", lst.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvrange(start, end, step)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMvrangeCatalystTest() throws Exception {
        String q = "index=index_A | eval a=mvrange(1514834731,1524134919,\"7d\")" +
        		   				 "| eval b=mvrange(1, 10, 2)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), " + 
        		   "StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), " + 
        		   "StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " + 
        		   "StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,false),true), StructField(b,ArrayType(StringType,false),true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            //res.show(false);
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals("[1514834731, "
            		+ "1515439531, "
            		+ "1516044331, "
            		+ "1516649131, "
            		+ "1517253931, "
            		+ "1517858731, "
            		+ "1518463531, "
            		+ "1519068331, "
            		+ "1519673131, "
            		+ "1520277931, "
            		+ "1520882731, "
            		+ "1521487531, "
            		+ "1522092331, "
            		+ "1522697131, "
            		+ "1523301931, "
            		+ "1523906731]", lst.get(0).toString());
            assertEquals("[1, "
            		+ "3, "
            		+ "5, "
            		+ "7, "
            		+ "9]", lstB.get(0).toString());
            } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvsort(mvfield)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMvsortCatalystTest() throws Exception {
        String q = "index=index_A | eval a=mvsort(mvappend(\"6\", \"4\", \"Aa\", \"Bb\", \"aa\", \"cd\", \"g\", \"b\", \"10\", \"11\", \"100\"))";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), " + 
        		   "StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), " + 
        		   "StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " + 
        		   "StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,false),false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            //res.show(false);
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show(false);
            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals("[10, "
            		+ "100, "
            		+ "11, "
            		+ "4, "
            		+ "6, "
            		+ "Aa, "
            		+ "Bb, "
            		+ "aa, "
            		+ "b, "
            		+ "cd, "
            		+ "g]",lst.get(0).toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvzip(x,y,"z")
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMvzipCatalystTest() throws Exception {
        String q = "index=index_A | eval mv1=mvappend(\"mv1-1\",\"mv1-2\",\"mv1-3\") | eval mv2=mvappend(\"mv2-1\",\"mv2-2\",\"mv2-3\")"
        		+ "| eval a=mvzip(mv1, mv2) | eval b=mvzip(mv1, mv2, \"=\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), " + 
        		   "StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), " + 
        		   "StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " + 
        		   "StructField(sourcetype,StringType,true), StructField(mv1,ArrayType(StringType,false),false), StructField(mv2,ArrayType(StringType,false),false), StructField(a,ArrayType(StringType,false),true), StructField(b,ArrayType(StringType,false),true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try
        {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            //res.show(false);
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            //resA.show(false);
            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            //resB.show(false);
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals("[mv1-1,mv2-1, "
            		+ "mv1-2,mv2-2, "
            		+ "mv1-3,mv2-3]", lst.get(0).toString());
            assertEquals("[mv1-1=mv2-1, "
            		+ "mv1-2=mv2-2, "
            		+ "mv1-3=mv2-3]", lstB.get(0).toString());
            
        } 
        catch (Exception ex)
        {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method commands(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalCommandsCatalystTest() throws Exception {
        String q = "index=index_A | eval a=commands(\"search foo | stats count | sort count\") " +
        		   "| eval b=commands(\"eval a=random() | eval b=a % 10 | stats avg(b) as avg min(b) as min max(b) as max var(b) as var | table avg min max var\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), " + 
        		   "StructField(host,StringType,true), StructField(index,StringType,true), StructField(ip,StringType,true), " + 
        		   "StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " + 
        		   "StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,false),true), StructField(b,ArrayType(StringType,false),true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_ips.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try
        {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema);//,visitor.getTraceBuffer().toString());
            //res.show(false);
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show(false);
            
			List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            resB.show(false);
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());
            
            // Compare values to expected
            assertEquals("[search, stats, sort]", lst.get(0).toString()); 
            assertEquals("[eval, eval, stats, table]", lstB.get(0).toString());
        } 
        catch (Exception ex)
        {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval isbool(x) / isint(x) / isnum(x) / isstr(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalIsTypeCatalystTest() throws Exception {
        String q = "index=index_A | eval isBoolean = isbool(true()) | eval isNotBoolean = isbool(1) "
        			+ "| eval isInt = isint(1) | eval isNotInt = isint(\"a\") "
        			+ "| eval isNum = isnum(5.4) | eval isNotNum = isnum(false()) "
        			+ "| eval isStr = isstr(\"a\") | eval isNotStr = isstr(3)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(isBoolean,BooleanType,true), StructField(isNotBoolean,BooleanType,true), StructField(isInt,BooleanType,true), StructField(isNotInt,BooleanType,true), StructField(isNum,BooleanType,true), StructField(isNotNum,BooleanType,true), StructField(isStr,BooleanType,true), StructField(isNotStr,BooleanType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Boolean
            Dataset<Row> ds_isBoolean = res.select("isBoolean").orderBy("isBoolean").distinct();
            List<Row> lst_isBoolean = ds_isBoolean.collectAsList();
            assertTrue(lst_isBoolean.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNotBoolean = res.select("isNotBoolean").orderBy("isNotBoolean").distinct();
            List<Row> lst_isNotBoolean = ds_isNotBoolean.collectAsList();
            assertFalse(lst_isNotBoolean.get(0).getBoolean(0));
            
            
            // Integer
            Dataset<Row> ds_isInt = res.select("isInt").orderBy("isInt").distinct();
            List<Row> lst_isInt = ds_isInt.collectAsList();
            assertTrue(lst_isInt.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNotInt = res.select("isNotInt").orderBy("isNotInt").distinct();
            List<Row> lst_isNotInt = ds_isNotInt.collectAsList();
            assertFalse(lst_isNotInt.get(0).getBoolean(0));
            
            
            // Numeric
            Dataset<Row> ds_isNum = res.select("isNum").orderBy("isNum").distinct();
            List<Row> lst_isNum = ds_isNum.collectAsList();
            assertTrue(lst_isNum.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNotNum = res.select("isNotNum").orderBy("isNotNum").distinct();
            List<Row> lst_isNotNum = ds_isNotNum.collectAsList();
            assertFalse(lst_isNotNum.get(0).getBoolean(0));
            
            
            // String
            Dataset<Row> ds_isStr = res.select("isStr").orderBy("isStr").distinct();
            List<Row> lst_isStr = ds_isStr.collectAsList();
            assertTrue(lst_isStr.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNotStr = res.select("isNotStr").orderBy("isNotStr").distinct();
            List<Row> lst_isNotStr = ds_isNotStr.collectAsList();
            assertFalse(lst_isNotStr.get(0).getBoolean(0));
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval isnull(x) and isnotnull(x)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalIsNullCatalystTest() throws Exception {
        String q = "index=index_A | eval a = isnull(null()) | eval b = isnull(true()) | eval c = isnotnull(null()) | eval d = isnotnull(true())";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,BooleanType,false), StructField(b,BooleanType,false), StructField(c,BooleanType,false), StructField(d,BooleanType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Boolean
            Dataset<Row> ds_isNull = res.select("a").orderBy("a").distinct();
            List<Row> lst_isNull = ds_isNull.collectAsList();
            assertTrue(lst_isNull.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNotNull = res.select("b").orderBy("b").distinct();
            List<Row> lst_isNotNull = ds_isNotNull.collectAsList();
            assertFalse(lst_isNotNull.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNull2 = res.select("c").orderBy("c").distinct();
            List<Row> lst_isNull2 = ds_isNull2.collectAsList();
            assertFalse(lst_isNull2.get(0).getBoolean(0));
            
            Dataset<Row> ds_isNotNull2 = res.select("d").orderBy("d").distinct();
            List<Row> lst_isNotNull2 = ds_isNotNull2.collectAsList();
            assertTrue(lst_isNotNull2.get(0).getBoolean(0));
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval typeof(x)
    // TODO uncomment eval d= ... when eval supports non-existing fields
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalTypeofCatalystTest() throws Exception {
        String q = "index=index_A | eval a = typeof(12) | eval b = typeof(\"string\") | eval c = typeof(1==2) <!--| eval d = typeof(badfield)-->";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,false), StructField(b,StringType,false), StructField(c,StringType,false), StructField(d,StringType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
//            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // number
            Dataset<Row> dsNumber = res.select("a").orderBy("a").distinct();
            List<Row> dsNumberLst = dsNumber.collectAsList();
            assertEquals("Number", dsNumberLst.get(0).getString(0));
            
            // string
            Dataset<Row> dsString = res.select("b").orderBy("b").distinct();
            List<Row> dsStringLst = dsString.collectAsList();
            assertEquals("String", dsStringLst.get(0).getString(0));
            
            // boolean
            Dataset<Row> dsBoolean = res.select("c").orderBy("c").distinct();
            List<Row> dsBooleanLst = dsBoolean.collectAsList();
            assertEquals("Boolean", dsBooleanLst.get(0).getString(0));
            
            // invalid
//            Dataset<Row> dsInvalid = res.select("d").orderBy("d").distinct();
//            List<Row> dsInvalidLst = dsInvalid.collectAsList();
//            assertEquals("Invalid", dsInvalidLst.get(0).getString(0));
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvappend(x, ...)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseMvappendCatalystTest() throws Exception {
        String q = "index=index_A | eval a = mvappend(\"Hello\",\"World\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,"
        		+ "StringType,true), StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,false),false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            Dataset<Row> resMvAppend = res.select("a").orderBy("a").distinct();
            List<Row> lst = resMvAppend.collectAsList();

            assertEquals("[Hello, World]", lst.get(0).getList(0).toString());
            assertEquals(1, lst.size());
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvcount(mvfield)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseMvcountCatalystTest() throws Exception {
        String q = "index=index_A | eval one_value = mvcount(mvappend(offset)) | eval two_values = mvcount(mvappend(index, offset))";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(one_value,IntegerType,true), " + 
        "StructField(two_values,IntegerType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get results
            Dataset<Row> res1V = res.select("one_value").orderBy("one_value").distinct();
            Dataset<Row> res2V = res.select("two_values").orderBy("two_values").distinct();
            
            // Collect results to list
            List<Row> lst1V = res1V.collectAsList();
            List<Row> lst2V = res2V.collectAsList();

            // Assert to expected values
            assertEquals(1, lst1V.get(0).getInt(0));
            assertEquals(2, lst2V.get(0).getInt(0));
 
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvdedup(mvfield)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseMvdedupCatalystTest() throws Exception {
        String q = "index=index_A | eval a = mvdedup(mvappend(\"1\",\"2\",\"3\",\"1\",\"2\",\"4\"))";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), " + 
        		"StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), " +
        		"StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,false),true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lstA = resA.collectAsList();
            assertEquals("[1, 2, 3, 4]", lstA.get(0).getList(0).toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method mvfilter(x)
    // TODO
    @Disabled
	@Test
    public void parseMvfilterCatalystTest() throws Exception {
        String q = "index=index_A | eval email = mvappend(\"aa@bb.example.test\",\"aa@yy.example.test\",\"oo@ii.example.test\",\"zz@uu.example.test\",\"auau@uiui.example.test\") | eval a = mvfilter( email != \"aa@bb.example.test\" )";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
//            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            //res.show();
            
            Dataset<Row> resEmail = res.select("email");
            resEmail.show(false);
            
            Dataset<Row> resA = res.select("a");
            resA.show(false);
//            List<Row> lstA = resA.collectAsList();
//            assertEquals("1\n2\n3\n4", lstA.get(0).getString(0));

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval strptime()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalStrptimeCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=strptime(\"2018-08-13 11:22:33\",\"%Y-%m-%d %H:%M:%S\") " +
    			   "| eval b=strptime(\"2018-08-13 11:22:33 11 AM PST\",\"%Y-%m-%d %T %I %p %Z\") ";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true), StructField(b,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            resA.show(false);
            List<Row> lstA = resA.collectAsList();
            
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            resB.show(false);
            List<Row> lstB = resB.collectAsList();
            
            // Assert equals with expected
            assertEquals(1534159353L, lstA.get(0).getLong(0));
            assertEquals(1534188153L, lstB.get(0).getLong(0));

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // FIXME Test eval strftime()
    @Disabled
	@Test
    public void parseEvalStrftimeCatalystTest() throws Exception {
	     String q = "index=index_A <!--| eval a=strftime(1534159353,\"%Y-%m-%d %H:%M:%S\") " +
	 			   "| eval b=strftime(1534188153,\"%Y-%m-%d %T %I %p %Z\") --> | eval c=strftime(_time, \"%Y-%m-%d %H:%M:%S\")";

         StructType expectedSchema = new StructType(new StructField[] {
                 new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                 new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                 new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("a", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("b", DataTypes.StringType, true, new MetadataBuilder().build()),
                 new StructField("c", DataTypes.StringType, true, new MetadataBuilder().build())
         });
	     String result = null;
	     
	     CharStream inputStream = CharStreams.fromString(q);
	     DPLLexer lexer = new DPLLexer(inputStream);
	     DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
	     ParseTree tree = parser.root();
	     DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);

        LOGGER.debug(tree.toStringTree(parser));
	     
	     // Use this file for  dataset initialization
	     String testFile = "src/test/resources/eval_test_data1.json";
	     Dataset<Row> inDs = spark.read().json(testFile);
	     ctx.setDs(inDs);
	     ctx.setEarliest("-1Y");
	     
	     DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
	     try {
	         CatalystNode n = (CatalystNode) visitor.visit(tree);
	         result = visitor.getLogicalPart();
	         Dataset<Row> res = n.getDataset();

	        // assertEquals(expectedSchema, res.schema());
	         res.show(false);
	         
	      /*   Dataset<Row> resA = res.select("a").orderBy("a").distinct();
	         resA.show(false);
	         List<Row> lstA = resA.collectAsList();
	         
	         Dataset<Row> resB = res.select("b").orderBy("b").distinct();
	         resB.show(false);
	         List<Row> lstB = resB.collectAsList();

             Dataset<Row> resC = res.select("c").orderBy("c").distinct();
             resC.show(false);
             List<Row> lstC = resC.collectAsList();*/

             // Assert equals with expected
	        // assertEquals("2018-08-13 11:22:33", lstA.get(0).getString(0));
	        // assertEquals("2018-08-13 19:22:33 07 PM UTC", lstB.get(0).getString(0));
            // assertEquals("2000-12-31 23:01:01", lstC.get(0).getString(0)); // _time in GMT+3, result UTC
	
	     } catch (Exception ex) {
	         ex.printStackTrace();
	         throw ex;
	     }
    }
    
    // Test eval method split(field,delimiter)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalSplitCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=split(\"a;b;c;d;e;f;g;h\",\";\") " +
    			   "| eval b=split(\"1,2,3,4,5,6,7,8,9,10\",\",\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,ArrayType(StringType,true),false), StructField(b,ArrayType(StringType,true),false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            //resA.show(false);
            List<Row> lstA = resA.collectAsList();
            
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            //resB.show(false);
            List<Row> lstB = resB.collectAsList();
            
            // Assert equals with expected
            assertEquals("[a, b, c, d, e, f, g, h]", lstA.get(0).getList(0).toString());
            assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]", lstB.get(0).getList(0).toString());

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method relative_time(unixtime, modifier)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalRelative_timeCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=relative_time(1645092037, \"-7d\") " +
    			   "| eval b=relative_time(1645092037,\"@d\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true), StructField(b,LongType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            //resA.show(false);
            List<Row> lstA = resA.collectAsList();
            
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            //resB.show(false);
            List<Row> lstB = resB.collectAsList();
            
            // Assert equals with expected
            assertEquals(1644487237L, lstA.get(0).getLong(0));
            assertEquals(1645048800L, lstB.get(0).getLong(0));

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method min(x, ...) and max(x, ...)
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalMinMaxCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=min(offset, offset - 2, offset - 3, offset - 4, offset - 5, offset) | eval b=max(offset, offset - 1, offset + 5) ";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,true), StructField(b,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            //assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            Dataset<Row> resA = res.select("a");
            List<Row> lstA = resA.collectAsList();
            
            Dataset<Row> resB = res.select("b");
            List<Row> lstB = resB.collectAsList();
            
            Dataset<Row> srcDs = res.select("offset");
            List<Row> srcLst = srcDs.collectAsList();
            
            // Assert equals with expected
            for (int i = 0; i < srcLst.size(); i++) {
            	assertEquals(srcLst.get(i).getLong(0) - i, Long.parseLong(lstA.get(i).getString(0)));
            	assertEquals(srcLst.get(i).getLong(0) + i, Long.parseLong(lstB.get(i).getString(0)));
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval json_valid()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalJSONValidCatalystTest() throws Exception {
        String q = " index=index_A | eval a=json_valid(_raw) | eval b=json_valid(json_field)";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(json_field,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(xml_field,StringType,true), StructField(a,BooleanType,true), " + 
        "StructField(b,BooleanType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for dataset initialization
        String testFile = "src/test/resources/eval_test_json.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();
            
            // Get column 'a'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();
            
            assertFalse(lst.get(0).getBoolean(0)); // _raw IS NOT json
            assertTrue(lstB.get(0).getBoolean(0)); // json_field IS json
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test spath() with JSON
    @Disabled
	@Test
    // FIXME broken due to spath udf changes
    public void parseEvalSpathJSONCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=spath(json_field, \"name\") | eval b=spath(json_field,\"invalid_spath\")";
    	String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
    	        "StructField(index,StringType,true), StructField(json_field,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
    	        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(xml_field,StringType,true), StructField(a,StringType,true), " + 
    	        "StructField(b,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for dataset initialization
        String testFile = "src/test/resources/eval_test_json.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a"); //.orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();
            
            
            assertEquals("John A", lst.get(0).getString(0));
            assertEquals("John", lst.get(1).getString(0));
            assertEquals("John B", lst.get(2).getString(0));
            assertEquals("John C", lst.get(3).getString(0));
            assertEquals("John D", lst.get(4).getString(0));
            assertEquals("John E", lst.get(5).getString(0));
            assertEquals("John F", lst.get(6).getString(0));
            assertEquals("John G", lst.get(7).getString(0));
            assertEquals("John H", lst.get(8).getString(0));
            
            assertEquals(null, lstB.get(0).getString(0));
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test spath() with XML
    // //person[age=30]/name/text()
    // FIXME broken due to spath udf changes
    @Disabled
	@Test
    public void parseEvalSpathXMLCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=spath(xml_field, \"people.person.name\")";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
        "StructField(index,StringType,true), StructField(json_field,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(xml_field,StringType,true), StructField(a,StringType,true))"; //, " + 
        //"StructField(b,StringType,true))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for dataset initialization
        String testFile = "src/test/resources/eval_test_json.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
//            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
          //  assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a"); //.orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();
            
            // Get column 'b'
//            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
//            List<Row> lstB = resB.collectAsList();
            
            
            assertEquals("John", lst.get(0).getString(0));
            assertEquals("John", lst.get(1).getString(0));
            assertEquals("John", lst.get(2).getString(0));
            assertEquals("John", lst.get(3).getString(0));
            assertEquals("John", lst.get(4).getString(0));
            assertEquals("John", lst.get(5).getString(0));
            assertEquals("John", lst.get(6).getString(0));
            assertEquals("John", lst.get(7).getString(0));
            assertEquals("John", lst.get(8).getString(0));
            
            //assertEquals(null, lstB.get(0).getString(0));
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval method exact()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalExactCatalystTest() throws Exception {
    	String q = "index=index_A | eval a=8.250 * 0.2 | eval b=exact(8.250 * 0.2)";
    	String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,DoubleType,false), StructField(b,DoubleType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            //res.show();
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();
            
            // with and without exact() should be the same
            assertEquals(lst, lstB);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
   
    // Test eval method searchmatch()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEvalSearchmatchCatalystTest() throws Exception {
    	String q = "index=index_A | eval test=searchmatch(\"index=index_A\") | eval test2=searchmatch(\"index=index_B\") | eval test3=searchmatch(\"offset<10 index=index_A sourcetype=*\")";
    	String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), " + 
    	        "StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), " + 
    	        "StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(test,BooleanType,false), " + 
    	        "StructField(test2,BooleanType,false), StructField(test3,BooleanType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show();
            
            // Get column 'test'
            Dataset<Row> resA = res.select("test");
            List<Row> lst = resA.collectAsList();
            
            // Get column 'test2'
            Dataset<Row> resB = res.select("test2");
            List<Row> lstB = resB.collectAsList();
            
            // Get column 'test3'
            Dataset<Row> resC = res.select("test3");
            List<Row> lstC = resC.collectAsList();
            
            /* eval test = searchmatch("index=index_A") |
             * eval test2 = searchmatch("index=index_B") | 
             * eval test3 = searchmatch("offset<10 index=index_A sourcetype=*") 
            */
            
            for (int i = 0; i < lst.size(); i++) {
            	// eval test results in all TRUE
            	assertTrue(lst.get(i).getBoolean(0)); 
            	// eval test2 results in all FALSE
            	assertFalse(lstB.get(i).getBoolean(0));
            	
            	
            	// eval test3, values between i=0..8 and 16... are TRUE, otherwise FALSE
            	if (i < 9 || i > 15) {
            		assertTrue(lstC.get(i).getBoolean(0));
            	}
            	else {
            		assertFalse(lstC.get(i).getBoolean(0));
            	}
            }
             
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    // Test eval now() and time()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void parseEval_Now_Time_CatalystTest() throws Exception {
        String q = " index=index_A | eval a=now() | eval b=time()";
        String e = "StructType(StructField(_raw,StringType,true), StructField(_time,TimestampType,true), StructField(host,StringType,true), StructField(index,StringType,true), StructField(offset,LongType,true), StructField(partition,StringType,true), StructField(source,StringType,true), StructField(sourcetype,StringType,true), StructField(a,LongType,false))";
        String result = null;
        
        CharStream inputStream = CharStreams.fromString(q);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        DPLParserCatalystContext ctx = new DPLParserCatalystContext(spark);
        
        // Use this file for  dataset initialization
        String testFile = "src/test/resources/eval_test_data1.json";
        Dataset<Row> inDs = spark.read().json(testFile);
        ctx.setDs(inDs);
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        try {
            CatalystNode n = (CatalystNode) visitor.visit(tree);
            result = visitor.getLogicalPart();
            Dataset<Row> res = n.getDataset();
            
            String resSchema = res.schema().toString();
            //assertEquals(e, resSchema); //,visitor.getTraceBuffer().toString());
            res.show(1, false);
            
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();
            
            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();
            
            // the current time will be slightly off so make sure it's close enough
            long timeInSec = System.currentTimeMillis() / 1000L;
            boolean isOk = timeInSec - 5L <= lst.get(0).getLong(0) && timeInSec + 5L >= lst.get(0).getLong(0); 
            
            // Make sure the result of time() is in format 0000000000.000000 with regex
            Pattern p = Pattern.compile("\\d+\\.\\d{6}");
            Matcher m = p.matcher(lstB.get(0).getString(0));
            
            assertTrue(isOk);
            assertTrue(m.matches());
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Disabled
	@Test
    public void parseEvalSplitTest() throws Exception {
        String q = "index=voyager | eval l = split (\"a,b,d,e,f\",\",\")";
        String e = "SELECT split(\"a,b,d,e,f\", \",\") AS l FROM `temporaryDPLView` WHERE index LIKE \"voyager\"";
        String result = null;
        assertEquals(e,result);
    }

    @Disabled
	@Test
    public void parseEvalStrTimeTest() throws Exception {
        String q = "index=voyager | eval l = strftime (\"1593612703049000000\",\"%Y-%m-%dT%H:%M:%S\")";
        String e = "SELECT from_unixtime(\"1593612703049000000\", \"%Y-%m-%dT%H:%M:%S\") AS l FROM `temporaryDPLView` WHERE index LIKE \"voyager\"";
        String result = null;
        assertEquals(e,result);
    }

    @Disabled
	@Test
    public void parseEvalStrTime1Test() throws Exception {
        String q,e,result;
        q = "index=voyager | eval l = strptime (\"2020-07-02T10:25:11\",\"%Y-%m-%dT%H:%M:%S\")";
        e = "SELECT to_unix_timestamp(\"2020-07-02T10:25:11\", \"%Y-%m-%dT%H:%M:%S\") AS l FROM `temporaryDPLView` WHERE index LIKE \"voyager\"";
        result = null;
        //LOGGER.info("DPL      =<" + q + ">");
        //utils.printDebug(e,result);
        assertEquals(e,result);
    }

}
