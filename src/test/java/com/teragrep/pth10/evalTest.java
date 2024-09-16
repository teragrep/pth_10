/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
package com.teragrep.pth10;

import com.teragrep.pth10.ast.commands.evalstatement.UDFs.EvalArithmetic;
import com.teragrep.pth10.ast.commands.evalstatement.UDFs.EvalOperation;
import com.teragrep.pth_03.antlr.DPLLexer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class evalTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(evalTest.class);

    private StreamingTestUtil streamingTestUtil;

    private static TimeZone originalTimeZone = null;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterAll
    void recoverTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }


    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMultipleStatementsTest() {
        String q = "index=index_A | eval a = 1, b = 2";
        String testFile = "src/test/resources/eval_test_data1*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true)," +
                "StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true)," +
                "StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true)," +
                "StructField(a,IntegerType,false),StructField(b,IntegerType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            List<String> listOfA = res.select("a").distinct().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            List<String> listOfB = res.select("b").distinct().collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertEquals(1, listOfA.size());
            assertEquals("1", listOfA.get(0));
            assertEquals(1, listOfB.size());
            assertEquals("2", listOfB.get(0));
        });
    }


    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalLenCatalystTest() {
        String q = "index=index_A | eval lenField = len(_raw)";
        String testFile = "src/test/resources/subsearchData*.json"; // * to make the file into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(lenField,IntegerType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct lenField and sort it by value
            Dataset<Row> orderedDs = res.select("lenField").orderBy("lenField").distinct();
            List<Integer> lst = orderedDs.collectAsList().stream().map(r->r.getInt(0)).sorted().collect(Collectors.toList());
            // we should get 3 distinct values
            assertEquals(3,lst.size());
            // Compare values
            assertEquals(158,lst.get(0));
            assertEquals(187,lst.get(1));
            assertEquals(210,lst.get(2));
        });
    }

    // Test upper(x) lower(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalUpperLowerCatalystTest() {
        String q = "index=index_A | eval a=upper(\"hello world\") | eval b=lower(\"HELLO WORLD\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,false),StructField(b,StringType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            assertEquals("HELLO WORLD", lstA.get(0));
            assertEquals("hello world", lstB.get(0));
        });
    }

    // test eval method urldecode()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalUrldecodeCatalystTest() {
        String q = "index=index_A | eval a=urldecode(\"http%3A%2F%2Fwww.example.com%2Fdownload%3Fr%3Dlatest\") | eval b=urldecode(\"https%3A%2F%2Fwww.longer-domain-here.example.com%2Fapi%2Fv1%2FgetData%3Fmode%3Dall%26type%3Dupdate%26random%3Dtrue\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            assertEquals("http://www.example.com/download?r=latest", lstA.get(0));
            assertEquals("https://www.longer-domain-here.example.com/api/v1/getData?mode=all&type=update&random=true", lstB.get(0));
        });
    }

    // test ltrim() rtrim() trim()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTrimCatalystTest() {
        String q = "index=index_A | eval a=ltrim(\" \t aabbccdd \") | eval b=ltrim(\"  zZaabcdzz \",\" zZ\") " +
        		 	"| eval c=rtrim(\"\t abcd  \t\") | eval d=rtrim(\" AbcDeF g\",\"F g\") | eval e=trim(\"\tabcd\t\") | eval f=trim(\"\t zzabcdzz \t\",\"\t zz\")";
        String testFile = "src/test/resources/eval_test_data1*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,false),StructField(b,StringType,false)," +
                "StructField(c,StringType,false),StructField(d,StringType,false),StructField(e,StringType,false),StructField(f,StringType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test eval method replace(x,y,z)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalReplaceCatalystTest() {
        String q = "index=index_A | eval a=replace(\"Hello world\", \"He\", \"Ha\") | eval b=replace(a, \"world\", \"welt\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,false),StructField(b,StringType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lstA = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<String> lstB = resB.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            assertEquals("Hallo world", lstA.get(0));
            assertEquals("Hallo welt", lstB.get(0));
        });
    }

    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSubstringCatalystTest() {
        String q = "index=index_A | eval str = substr(_raw,1,14)";
        String testFile = "src/test/resources/subsearchData*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(str,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct lenField and sort it by value
            Dataset<Row> orderedDs = res.select("str").orderBy("str").distinct();
            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values
            assertEquals(1,lst.size());
            // Compare values
            assertEquals("127.0.0.123:45",lst.get(0).getString(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSubstringNoLengthParamCatalystTest() {
        String q = "index=index_A | eval str = substr(_raw,185)";
        String testFile = "src/test/resources/subsearchData*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(str,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct lenField and sort it by value
            Dataset<Row> orderedDs = res.select("str").orderBy("str").distinct();
            List<String> lst = orderedDs.collectAsList().stream().map(r->r.getString(0)).sorted().collect(Collectors.toList());
            // we should get 3 distinct values
            assertEquals(3,lst.size());
            // Compare values
            assertEquals("",lst.get(0));
            assertEquals("com",lst.get(1));
            assertEquals("com cOmPuter02.example.com", lst.get(2));
        });
    }

    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalIfCatalystTest() {
		String q = "index=index_A | eval val2=if((false() OR true()),\"a\", \"b\")";
        String testFile = "src/test/resources/subsearchData*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(val2,ArrayType(StringType,true),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct val2 and sort it by value
            Dataset<Row> orderedDs = res.select("val2").orderBy("val2").distinct();

            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values
            assertEquals(1,lst.size());
            // Compare values
            assertEquals(Collections.singletonList("a"),lst.get(0).getList(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalIfMultiValueCatalystTest() {
        String q = "index=index_A | eval mvf=mvappend(\"\") |eval val2=if(mvf==\"\",\"t\",\"f\"))";
        String testFile = "src/test/resources/subsearchData*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(mvf,ArrayType(StringType,false),false),StructField(val2,ArrayType(StringType,true),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct val2 and sort it by value
            Dataset<Row> orderedDs = res.select("val2").orderBy("val2").distinct();

            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values
            assertEquals(1,lst.size());
            // Compare values
            assertEquals(Collections.singletonList("t"),lst.get(0).getList(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalIfMultiValueAsResultCatalystTest() {
        String q = "index=index_A | eval mvf=mvappend(\"\") |eval val2=if(mvf==\"\",mvappend(\"tr\",\"ue\"),\"f\"))";
        String testFile = "src/test/resources/subsearchData*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(mvf,ArrayType(StringType,false),false),StructField(val2,ArrayType(StringType,true),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct val2 and sort it by value
            Dataset<Row> orderedDs = res.select("val2").orderBy("val2").distinct();

            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values
            assertEquals(1,lst.size());
            // Compare values
            assertEquals(Arrays.asList("tr", "ue"),lst.get(0).getList(0));
        });
    }

    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalIfCatalyst1Test() {
		String q = "index=index_A | eval val2=if( 1 < 2  , substr(_raw,165,100) , \"b\")";
        String testFile = "src/test/resources/subsearchData*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(val2,ArrayType(StringType,true),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> orderedDs = res.select("val2").orderBy("val2").distinct();

            List<String> lst = orderedDs.collectAsList().stream()
                    .map(r->r.getList(0).get(0).toString()).sorted().collect(Collectors.toList());
            // we should get 5 distinct values
            assertEquals(5, lst.size());

            // Compare values
            assertEquals("",lst.get(0));
            assertEquals(" computer01.example.com",lst.get(1));
            assertEquals(" computer01.example.com cOmPuter02.example.com",lst.get(2));
            assertEquals(" computer02.example.com",lst.get(3));
            assertEquals(" computer03.example.com",lst.get(4));

        });
    }

    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalLen1Test() {
        String q = "index=index_A | eval a=if(substr(_raw,0,11)=\"127.0.0.123\",len( _raw),0)";
        String testFile = "src/test/resources/subsearchData*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            //  Get only distinct field and sort it by value
            Dataset<Row> orderedDs = res.select("a").orderBy("a").distinct();
            List<String> lst = orderedDs.collectAsList().stream().map(r->r.getList(0).get(0).toString()).sorted().collect(Collectors.toList());

            // check result count
            assertEquals(3,lst.size()); // column a has 3 different values, as the data has 3 different lengths of _raw values
            // Compare values
            assertEquals("158",lst.get(0));
            assertEquals("187",lst.get(1));
            assertEquals("210",lst.get(2));
        });
    }

    // Test eval function null()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalNullCatalystTest() {
        String q = "index=index_A | eval a=null()";
        String testFile = "src/test/resources/subsearchData*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(origin,StringType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get only distinct field 'a' and sort it by value
            Dataset<Row> orderedDs = res.select("a").orderBy("a").distinct();
            List<Row> lst = orderedDs.collectAsList();
            // we should get 1 distinct values (all should be null)
            assertEquals(1,lst.size());
            // Compare values (is it null?)
            assertEquals(this.streamingTestUtil.getCtx().nullValue.value(),lst.get(0).get(0));
        });
    }

    // Test eval function pow()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalPowCatalystTest() {
        String q = "index=index_A | eval a=pow(offset,2)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order it by the values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Double> expectedLst = Arrays.asList(
                    1.0, 1.0, 1.0, 1.0, 4.0, 9.0, 16.0, 25.0, 36.0,
                    49.0, 64.0, 81.0, 100.0, 121.0, 144.0,
                    169.0, 196.0, 225.0, 256.0);

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval function nullif()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalNullifCatalystTest() {
        String q = "index=index_A | eval a=nullif(offset,_raw)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
                    "1","1","1","1","10","11","12","13","14","15","16","2","3","4","5","6","7","8","9");

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval function abs()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalAbsCatalystTest() {
        String q = "index=index_A | eval a=abs(offset)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order it by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Long> expectedLst = Arrays.asList(1L,1L,1L,1L,2L,3L,4L,5L,6L,7L,8L,9L,10L,11L,12L,13L,14L,15L,16L);

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method ceiling(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCeilingCatalystTest() {
        String q = "index=index_A | eval a=ceiling(offset+0.5)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Get column 'offset' and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
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
        });
    }

    // Test eval method exp(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalExpCatalystTest() {
        String q = "index=index_A | eval a=exp(offset)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'offset' and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("a");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, srcLst.size());
            assertEquals(19, lst.size());
            // Compare values to expected
            int i = 0;

            for (Long val : srcLst) {
                Double vExp = Math.exp(val.doubleValue());
                Double vGot = lst.get(i++);
                if (!vExp.toString().substring(0,10).equals(vGot.toString().substring(0,10))) {
                    fail(vExp + "!=" + vGot);
                }

            }
        });
    }

    // Test eval method floor(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalFloorCatalystTest() {
        String q = "index=index_A | eval a=floor(offset+0.5)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by value
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
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
        });
    }

    // Test eval method ln(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalLnCatalystTest() {
        String q = "index=index_A | eval a=ln(offset)";
        String testFile = "src/test/resources/eval_test_data1*.json";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'offset' and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            int i = 0;

            for (Long val : srcLst) {
                Double vExp = Math.log(val.doubleValue());
                Double vReal = lst.get(i++);
                if (!vExp.toString().substring(0, Math.min(vExp.toString().length(),10))
                        .equals(vReal.toString().substring(0,Math.min(vReal.toString().length(),10)))) {
                    fail(vExp + "!=" + vReal);
                }
            }
        });
    }

    // Test eval method log(x,y)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalLogCatalystTest() {
        String q = "index=index_A | eval a=log(offset, 10)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to  make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // for rounding since there are small deviations between spark log10 and java log10
            final DecimalFormat df = new DecimalFormat("0.00000000");

            // Get column 'a' and order by value
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();

            // Round both column 'a' contents and the expected, as spark log10 and java log10
            // return slightly different values. Making sure it is within margin of error.
            for (int i = 0; i < lst.size(); i++){
                Double v = lst.get(i);
                v = Double.valueOf(df.format(v));
                lst.set(i, v);
            }

            // Calculate java log10 and round to 8 decimal precision,
            // and compare to spark log10 rounded to 8 decimals.
            for (Long val : srcLst) {
                Double v = Math.log10(val.doubleValue());
                expectedLst.add(Double.valueOf(df.format(v)));
            }

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method log(x,y)
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalLogWithoutBaseParamCatalystTest() {
        String q = "index=index_A | eval a=log(offset)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to  make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // for rounding since there are small deviations between spark log10 and java log10
            final DecimalFormat df = new DecimalFormat("0.00000000");

            // Get column 'a', which is the column of the result, and order by value
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'offset', which is the column used as log() function input, and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in (log input param -> log output)
            assertEquals(19, lst.size());

            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();
            // Round both column 'a' contents and the expected, as spark log10 and java log10
            // return slightly different values. Making sure it is within margin of error.
            for (int i = 0; i < lst.size(); i++){
                // Get double value from log() input number
                Double v = lst.get(i);
                // Round to 8 decimal precision
                v = Double.valueOf(df.format(v));
                // add to list
                lst.set(i, v);
            }

            // Calculate java log10 and round to 8 decimal precision,
            // and compare to spark log10 rounded to 8 decimals.
            for (Long val : srcLst) {
                Double v = Math.log10(val.doubleValue());
                expectedLst.add(Double.valueOf(df.format(v)));
            }

            assertEquals(expectedLst, lst);
        });
    }

    // Test random()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalRandomCatalystTest() {
        String q = "index=index_A | eval a=random()";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,IntegerType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Integer> lst = resA.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());

            // Check that random() produced a result within set limits
            assertTrue(lst.get(0) <= Math.pow(2d, 31d) - 1);
            assertTrue(lst.get(0) >= 0);
        });
    }

    // Test eval method pi()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalPiCatalystTest() {
        String q = "index=index_A | eval a=pi()";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            assertEquals(3.14159265358d, lst.get(0));
        });
    }

    // Test eval method round(x,y)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalRoundCatalystTest() {
        String q = "index=index_A | eval a=round(1.545) | eval b=round(5.7432, 3)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true),StructField(b,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Double> lstA = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            assertEquals(2d, lstA.get(0));
            assertEquals(5.743d, lstB.get(0));
        });
    }

    // Test eval method sigfig(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSigfigCatalystTest() {
        String q = "index=index_A | eval a=sigfig(1.00 * 1111) | eval b=sigfig(offset - 1.100) | eval c=sigfig(offset * 1.234) | eval d=sigfig(offset / 3.245)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true),StructField(b,DoubleType,true)," +
                "StructField(c,DoubleType,true),StructField(d,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            /*
             *  eval a=sigfig(1.00 * 1111) | eval b=sigfig(offset - 1.100) | eval c=sigfig(offset * 1.234) | eval d=sigfig(offset / 3.245)
             */

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
        });
    }

    // Test eval method sqrt(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSqrtCatalystTest() {
        String q = "index=index_A | eval a=sqrt(offset)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get col offset and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Assert
            for (int i = 0; i < srcLst.size(); i++) {
                assertEquals(Math.sqrt(srcLst.get(i)),lst.get(i));
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSumTest() {
        String q = "index=index_A | eval a=sum(offset, 1, 3)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Get col offset and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Assert
            assertEquals(19, srcLst.size());
            int loopsExecuted = 0;
            for (int i = 0; i < srcLst.size(); i++) {
                loopsExecuted++;
                assertEquals(srcLst.get(i) + 1 + 3, lst.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSumWithStringsTest() { // should use the string in the sum if it is numerical, ignore otherwise
        String q = "index=index_A | eval a=sum(\"foo\", offset, \"2\", index)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get col offset and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Assert
            assertEquals(19, srcLst.size());
            int loopsExecuted = 0;
            for (int i = 0; i < srcLst.size(); i++) {
                loopsExecuted++;
                assertEquals(srcLst.get(i) + 2, lst.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSumWithDoubleTest() {
        String q = "index=index_A | eval a=sum(offset, 2.6, 3.5)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get col offset and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Assert
            assertEquals(19, srcLst.size());
            int loopsExecuted = 0;
            for (int i = 0; i < srcLst.size(); i++) {
                loopsExecuted++;
                assertEquals(srcLst.get(i) + 2.6 + 3.5, lst.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSumWithArithmeticalOperation() {
        String q = "index=index_A | eval a=sum(offset, 2 + 5)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a' and order by values
            Dataset<Row> resA = res.select("a").orderBy("a");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get col offset and order by values
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // Assert
            assertEquals(19, srcLst.size());
            int loopsExecuted = 0;
            for (int i = 0; i < srcLst.size(); i++) {
                loopsExecuted++;
                assertEquals(srcLst.get(i) + 2 + 5, lst.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    // Test eval concat ab+cd
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalConcatCatalystTest() {
        String q = "index=index_A | eval a=\"ab\"+\"cd\"";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // we should get 1
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals("abcd", lst.get(0));
        });
    }

    // Test eval plus
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalPlusCatalystTest() {
        String q = "index=index_A | eval a=0.1+1.4";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // we should get 1
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals(1.5d, Double.parseDouble(lst.get(0)));
        });
    }

    // Test eval minus
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMinusCatalystTest() {
        String q = "index=index_A | eval a = offset - 1";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");

            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
                assertEquals(lstOffset.get(i)-1, Double.parseDouble(lst.get(i)));
            }
        });
    }

    // Test eval multiply
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMultiplyCatalystTest() {
        String q = "index=index_A | eval a = offset * offset";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");

            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
                assertEquals(lstOffset.get(i) * lstOffset.get(i),Long.parseLong(lst.get(i)));
            }
        });
    }

    // Test eval divide
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalDivideCatalystTest() {
        String q = "index=index_A | eval a = offset / offset";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");

            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
                assertEquals((double)lstOffset.get(i)/(double)lstOffset.get(i),Double.parseDouble(lst.get(i)));
            }
        });
    }

    // Test eval mod (%)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalModCatalystTest() {
        String q = "index=index_A | eval a = offset % 2";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");

            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
            List<Long> lstOffset = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get 19
            assertEquals(19, lst.size());
            // Compare values to expected
            for (int i = 0; i < lst.size(); i++) {
                assertEquals(lstOffset.get(i) % 2, Double.parseDouble(lst.get(i)));
            }
        });
    }

    // Test cryptographic functions: md5, sha1, sha256, sha512
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCryptographicCatalystTest() {
        String q = "index=index_A | eval md5=md5(_raw) | eval sha1=sha1(_raw) | eval sha256=sha256(_raw) | eval sha512=sha512(_raw)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(md5,StringType,true),StructField(sha1,StringType,true),StructField(sha256,StringType,true),StructField(sha512,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column '_raw'
            Dataset<Row> resRaw = res.select("_raw").orderBy("offset");
            List<String> lstRaw = resRaw.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'md5'
            Dataset<Row> resmd5 = res.select("md5").orderBy("offset");
            List<String> lstMd5 = resmd5.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'sha1'
            Dataset<Row> ress1 = res.select("sha1").orderBy("offset");
            List<String> lstS1 = ress1.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'sha256'
            Dataset<Row> ress256 = res.select("sha256").orderBy("offset");
            List<String> lstS256 = ress256.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'sha512'
            Dataset<Row> ress512 = res.select("sha512").orderBy("offset");
            List<String> lstS512 = ress512.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Assert expected to result

            // Amount of data
            assertEquals(lstRaw.size(),lstMd5.size());
            assertEquals(lstRaw.size(),lstS1.size());
            assertEquals(lstRaw.size(),lstS256.size());
            assertEquals(lstRaw.size(),lstS512.size());

            // Contents
            for (int i = 0; i < lstRaw.size(); ++i) {
                assertEquals(DigestUtils.md5Hex(lstRaw.get(i)),lstMd5.get(i));
                assertEquals(DigestUtils.sha1Hex(lstRaw.get(i)),lstS1.get(i));
                assertEquals(DigestUtils.sha256Hex(lstRaw.get(i)),lstS256.get(i));
                assertEquals(DigestUtils.sha512Hex(lstRaw.get(i)),lstS512.get(i));
            }
        });
    }

    // Test eval function case(x1,y1,x2,y2, ..., xn, yn)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCaseCatalystTest() {
        String q = "index=index_A | eval a=case(offset < 2, \"Less than two\", offset > 2, \"More than two\", offset == 2, \"Exactly two\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
                    "Less than two", "Less than two","Less than two","Less than two","Exactly two","More than two","More than two","More than two",
                    "More than two","More than two","More than two","More than two","More than two",
                    "More than two","More than two","More than two","More than two","More than two",
                    "More than two");
            assertEquals(expectedLst, lst);
        });
    }

    // Test eval function validate(x1,y1,x2,y2, ..., xn, yn)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalValidateCatalystTest() {
        String q = "index=index_A | eval a=validate(offset < 10, \"Not less than 10\", offset < 9, \"Not less than 9\", offset < 6, \"Not less than 6\", offset > 0, \"Not more than 0\", offset == 0, \"Not 0\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<String> lst = resA.collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
                    "Not 0","Not 0","Not 0","Not 0","Not 0","Not 0","Not 0","Not 0",
                    "Not less than 6","Not less than 6","Not less than 6","Not less than 9",
                    "Not less than 10","Not less than 10","Not less than 10","Not less than 10",
                    "Not less than 10","Not less than 10","Not less than 10"
            );

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method tostring(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTostring_NoOptionalArgument_CatalystTest() {
        String q = "index=index_A | eval a=tostring(true())";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> orderedDs = res.select("a").orderBy("a").distinct();
            List<String> lst = orderedDs.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());


            // we should get one result because all = "true"
            assertEquals(1, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList("true");

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method tostring(x,y="hex")
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTostring_Hex_CatalystTest() {
        String q = "index=index_A | eval a=tostring(offset, \"hex\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // check amount of results
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = new ArrayList<>();

            for (Long item : srcLst) {
                expectedLst.add("0x".concat(Integer.toHexString(item.intValue()).toUpperCase()));
            }

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method tostring(x,y="duration")
    // Has to have UTC as SparkSession's timezone
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTostring_Duration_CatalystTest() {
        String q = "index=index_A | eval a=tostring(offset, \"duration\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' and order by offset
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Get column 'offset' and order by value
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());
            // check amount of results
            assertEquals(19, lst.size());
            // Compare values to expected
            List<String> expectedLst = new ArrayList<>();

            for (Long item : srcLst) {
                expectedLst.add("00:00:".concat(item < 10 ? "0".concat(item.toString()) : item.toString()));
            }

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method tostring(x,y="commas")
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTostring_Commas_CatalystTest() {
        String q = "index=index_A | eval a=tostring(12345.6789, \"commas\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // we should get one result
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals("12,345.68", lst.get(0));
        });
    }

    // Test eval method tonumber(numstr, base)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTonumberCatalystTest() {
        String q = "index=index_A | eval a=tonumber(\"0A4\", 16)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get one result
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals(164L, lst.get(0));
        });
    }

    // Test eval method tonumber(numstr)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTonumberNoBaseArgumentCatalystTest() {
        String q = "index=index_A | eval a=tonumber(\"12345\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Long> lst = resA.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get one result
            assertEquals(1, lst.size());
            // Compare values to expected
            assertEquals(12345L, lst.get(0));
        });
    }

    // Test eval function acos, acosh, cos, cosh
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCosCatalystTest() {
        String q = "index=index_A | eval a=acos(offset / 10) | eval b=acosh(offset) | eval c=cos(offset / 10) | eval d=cosh(offset / 10)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true),StructField(b,DoubleType,true),StructField(c,DoubleType,true),StructField(d,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Without orderBy collectAsList will change the order randomly. Order every column by offset.
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // c, cos
            Dataset<Row> resC = res.select("c").orderBy("offset");
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // d, cosh
            Dataset<Row> resD = res.select("d").orderBy("offset");
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get source column
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
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
        });
    }

    // Test eval function asin, asinh, sin, sinh
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSinCatalystTest() {
        String q = "index=index_A | eval a=asin(offset / 10) | eval b=asinh(offset) | eval c=sin(offset / 10) | eval d=sinh(offset / 10)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true),StructField(b,DoubleType,true)," +
                "StructField(c,DoubleType,true),StructField(d,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'c'
            Dataset<Row> resC = res.select("c").orderBy("offset");
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'd'
            Dataset<Row> resD = res.select("d").orderBy("offset");
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get source column
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
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
        });
    }

    // Test eval function tan, tanh, atan, atanh, atan2
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTanCatalystTest() {
        String q = "index=index_A | eval a=atan(offset) | eval b=atanh(offset / 10) | eval c=tan(offset / 10) | eval d=tanh(offset / 10) | eval e=atan2(offset / 10, offset / 20)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,DoubleType,true),StructField(b,DoubleType,true),StructField(c,DoubleType,true),StructField(d,DoubleType,true),StructField(e,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a' atan
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get column 'b' atanh
            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Double> lstB = resB.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // c tan
            Dataset<Row> resC = res.select("c").orderBy("offset");
            List<Double> lstC = resC.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // d tanh
            Dataset<Row> resD = res.select("d").orderBy("offset");
            List<Double> lstD = resD.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // e atan2
            Dataset<Row> resE = res.select("e").orderBy("offset");
            List<Double> lstE = resE.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get source column
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(srcLst.size(),lst.size());
            assertEquals(srcLst.size(),lstB.size());
            assertEquals(srcLst.size(),lstC.size());
            assertEquals(srcLst.size(),lstD.size());
            assertEquals(srcLst.size(),lstE.size());
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
                expectedLstE.add(Math.atan2(Double.valueOf((double)val/10d),Double.valueOf((double)val/20d))); // atan
            }
            assertEquals(expectedLst, lst);
            assertEquals(expectedLstB, lstB);
            assertEquals(expectedLstC, lstC);
            assertEquals(expectedLstD, lstD);
            assertEquals(expectedLstE, lstE);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAvgTest() {
        String q = "index=index_A | eval a=avg(offset, 1, 2)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,IntegerType,true))";

        streamingTestUtil.performDPLTest(q,testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a'
            Dataset<Row> a = res.select("a").orderBy("offset");
            List<Integer> aList = a.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());

            // Get offset column
            Dataset<Row> offset = res.select("offset").orderBy("offset");
            List<Long> offsetList = offset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            assertEquals(19, offsetList.size());
            int loopsExecuted = 0;
            for (int i = 0; i < offsetList.size(); i++) {
                loopsExecuted++;
                Assertions.assertEquals(Math.round((offsetList.get(i) + 1 + 2) / 3.0), (long) aList.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAvgWithStringsTest() { // Should ignore non-numerical Strings
        String q = "index=index_A | eval a=avg(\"foo\", offset, \"1\", \"bar\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,IntegerType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a'
            Dataset<Row> a = res.select("a").orderBy("offset");
            List<Integer> aList = a.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());

            // Get offset column
            Dataset<Row> offset = res.select("offset").orderBy("offset");
            List<Long> offsetList = offset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            assertEquals(19, offsetList.size());
            int loopsExecuted = 0;
            for (int i = 0; i < offsetList.size(); i++) {
                loopsExecuted++;
                Assertions.assertEquals(Math.round((offsetList.get(i) + 1) / 2.0), (long) aList.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAvgWithDoublesTest() {
        String q = "index=index_A | eval a=avg(offset, 1.5, 3.5)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,IntegerType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a'
            Dataset<Row> a = res.select("a").orderBy("offset");
            List<Integer> aList = a.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());

            // Get offset column
            Dataset<Row> offset = res.select("offset").orderBy("offset");
            List<Long> offsetList = offset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            assertEquals(19, offsetList.size());
            int loopsExecuted = 0;
            for (int i = 0; i < offsetList.size(); i++) {
                loopsExecuted++;
                Assertions.assertEquals(Math.round((offsetList.get(i) + 1.5 + 3.5) / 3.0), (long) aList.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAvgWithArithmeticsTest() {
        String q = "index=index_A | eval a=avg(offset, 1 + 4, 5 + 6)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,IntegerType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            // Get column 'a'
            Dataset<Row> a = res.select("a").orderBy("offset");
            List<Integer> aList = a.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());

            // Get offset column
            Dataset<Row> offset = res.select("offset").orderBy("offset");
            List<Long> offsetList = offset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            assertEquals(19, offsetList.size());
            int loopsExecuted = 0;
            for (int i = 0; i < offsetList.size(); i++) {
                loopsExecuted++;
                Assertions.assertEquals(Math.round((offsetList.get(i) + 5 + 11) / 3.0), (long) aList.get(i));
            }
            assertEquals(19, loopsExecuted);
        });
    }

    // Test eval method hypot(x,y)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalHypotCatalystTest() {
    	String q = "index=index_A | eval a=hypot(offset, offset)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,DoubleType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Double> lst = resA.collectAsList().stream().map(r -> r.getDouble(0)).collect(Collectors.toList());

            // Get source column
            Dataset<Row> resOffset = res.select("offset").orderBy("offset");
            List<Long> srcLst = resOffset.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(srcLst.size(),lst.size());

            // Compare values to expected
            List<Double> expectedLst = new ArrayList<>();

            for (Long val : srcLst) {
                expectedLst.add(Math.hypot(Double.valueOf(val),Double.valueOf(val)));
            }

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval function cidrmatch
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCidrmatchCatalystTest() {
        String q = "index=index_A | eval a=cidrmatch(ip, \"192.168.2.0/24\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,BooleanType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
                    true, false, true
            );

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method coalesce(x, ...)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCoalesceCatalystTest() {
        String q = "index=index_A | eval a=coalesce(null(),index) | eval b=coalesce(index, null()) | eval c=coalesce(null())";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true)," +
                "StructField(a,StringType,true),StructField(b,StringType,true),StructField(c,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            // Get column 'c'
            Dataset<Row> resC = res.select("c").orderBy("c").distinct();
            List<Row> lstC = resC.collectAsList();

            // Compare values to expected
            assertEquals(1, lst.size());
            assertEquals(1, lstB.size());
            assertEquals(1, lstC.size());
            assertEquals("index_A", lst.get(0).getString(0));
            assertEquals("index_A", lstB.get(0).getString(0));
            assertEquals(null, lstC.get(0).getString(0));
        });
    }

    // Test eval method in(field, value_list)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalInCatalystTest() {
        String q = "index=index_A | eval a=in(ip,\"192.168.2.1\",\"127.0.0.91\", \"127.0.0.1\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,BooleanType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
                    true, false, true
            );

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method like(text, pattern)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalLikeCatalystTest() {
        String q = "index=index_A | eval a=like(ip,\"192.168.3%\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,BooleanType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
                    false, true, false
            );

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method match(subject, regex)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMatchCatalystTest() {
        String q = "index=index_A | eval a=match(ip,\"^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,BooleanType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Boolean> lst = resA.collectAsList().stream().map(r -> r.getBoolean(0)).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<Boolean> expectedLst = Arrays.asList(
                    true, true, true
            );

            assertEquals(expectedLst, lst);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMatch2CatalystTest() {
        String q = "index=index_A | eval a=if(match(ip,\"3\"),1,0)";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true)," +
                "StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true)," +
                "StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<String> lst = resA.collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            // we should get the same amount of values back as we put in
            assertEquals(3, lst.size());
            // Compare values to expected
            List<String> expectedLst = Arrays.asList(
                    "0", "1", "0"
            );

            assertEquals(expectedLst, lst);
        });
    }

    // Test eval method mvfind(mvfield, "regex")
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMvfindCatalystTest() {
        String q = "index=index_A | eval a=mvfind(mvappend(\"random\",\"192.168.1.1\",\"192.168.10.1\"),\"^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,IntegerType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Integer> lst = resA.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());

            // Compare values to expected
            assertEquals(1, lst.get(0));
        });
    }

    // Test eval method mvindex(mvfield, startindex [,endindex])
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMvindexCatalystTest() {
        String q = "index=index_A | eval a=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"),2) " +
        		   				 "| eval b=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"),2, 3)" +
                                 "| eval c=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"),-1)";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),true),StructField(b,ArrayType(StringType,false),true),StructField(c,ArrayType(StringType,false),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Get column 'c'
            Dataset<Row> resC = res.select("c").orderBy("c").distinct();
            List<List<Object>> lstC = resC.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Compare values to expected
            assertEquals("[mv3]", lst.get(0).toString());
            assertEquals("[mv3, mv4]", lstB.get(0).toString());
            assertEquals("[mv5]", lstC.get(0).toString());
        });
    }

    // Test eval method mvjoin(mvfield, str)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMvjoinCatalystTest() {
        String q = "index=index_A | eval a=mvjoin(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"),\";;\") " +
        		   				 "<!--| eval b=mvindex(mvappend(\"mv1\",\"mv2\",\"mv3\",\"mv4\",\"mv5\"),2, 3)--> ";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<String> lst = resA.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());

            // Compare values to expected
            assertEquals("mv1;;mv2;;mv3;;mv4;;mv5", lst.get(0));
        });
    }

    // Test eval method mvrange(start, end, step)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMvrangeCatalystTest() {
        String q = "index=index_A | eval a=mvrange(1514834731,1524134919,\"7d\")" +
        		   				 "| eval b=mvrange(1, 10, 2)";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),true),StructField(b,ArrayType(StringType,false),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test eval method mvsort(mvfield)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMvsortCatalystTest() {
        String q = "index=index_A | eval a=mvsort(mvappend(\"6\", \"4\", \"Aa\", \"Bb\", \"aa\", \"cd\", \"g\", \"b\", \"10\", \"11\", \"100\"))";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true)," +
                "StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true)," +
                "StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
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
        });
    }

    // Test eval method mvzip(x,y,"z")
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMvzipCatalystTest() {
        String q = "index=index_A | eval mv1=mvappend(\"mv1-1\",\"mv1-2\",\"mv1-3\") | eval mv2=mvappend(\"mv2-1\",\"mv2-2\",\"mv2-3\")"
        		+ "| eval a=mvzip(mv1, mv2) | eval b=mvzip(mv1, mv2, \"=\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(mv1,ArrayType(StringType,false),false),StructField(mv2,ArrayType(StringType,false),false),StructField(a,ArrayType(StringType,false),true),StructField(b,ArrayType(StringType,false),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Compare values to expected
            assertEquals("[mv1-1,mv2-1, "
                    + "mv1-2,mv2-2, "
                    + "mv1-3,mv2-3]", lst.get(0).toString());
            assertEquals("[mv1-1=mv2-1, "
                    + "mv1-2=mv2-2, "
                    + "mv1-3=mv2-3]", lstB.get(0).toString());
        });
    }

    // Test eval method commands(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalCommandsCatalystTest() {
        String q = "index=index_A | eval a=commands(\"search foo | stats count | sort count\") " +
        		   "| eval b=commands(\"eval a=random() | eval b=a % 10 | stats avg(b) as avg min(b) as min max(b) as max var(b) as var | table avg min max var\")";
        String testFile = "src/test/resources/eval_test_ips*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true)," +
                "StructField(host,StringType,true),StructField(index,StringType,true),StructField(ip,StringType,true)," +
                "StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),true),StructField(b,ArrayType(StringType,false),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();

            List<List<Object>> lst = resA.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<List<Object>> lstB = resB.collectAsList().stream().map(r -> r.getList(0)).collect(Collectors.toList());

            // Compare values to expected
            assertEquals("[search, stats, sort]", lst.get(0).toString());
            assertEquals("[eval, eval, stats, table]", lstB.get(0).toString());
        });
    }

    // Test eval isbool(x) / isint(x) / isnum(x) / isstr(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalIsTypeCatalystTest() {
        String q = "index=index_A | eval isBoolean = isbool(true()) | eval isNotBoolean = isbool(1) "
        			+ "| eval isInt = isint(1) | eval isNotInt = isint(\"a\") "
        			+ "| eval isNum = isnum(5.4) | eval isNotNum = isnum(false()) "
        			+ "| eval isStr = isstr(\"a\") | eval isNotStr = isstr(3)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(isBoolean,BooleanType,true),StructField(isNotBoolean,BooleanType,true),StructField(isInt,BooleanType,true),StructField(isNotInt,BooleanType,true),StructField(isNum,BooleanType,true),StructField(isNotNum,BooleanType,true),StructField(isStr,BooleanType,true),StructField(isNotStr,BooleanType,true))";


        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test eval isnull(x) and isnotnull(x)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalIsNullCatalystTest() {
        String q = "index=index_A | eval a = isnull(null()) | eval b = isnull(true()) | eval c = isnotnull(null()) | eval d = isnotnull(true())";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,BooleanType,false),StructField(b,BooleanType,false),StructField(c,BooleanType,false),StructField(d,BooleanType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test eval typeof(x)
    // TODO uncomment eval d= ... when eval supports non-existing fields
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalTypeofCatalystTest() {
        String q = "index=index_A | eval a = typeof(12) | eval b = typeof(\"string\") | eval c = typeof(1==2) <!--| eval d = typeof(badfield)-->";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true),StructField(c,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test eval method mvappend(x, ...)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseMvappendCatalystTest() {
        String q = "index=index_A | eval a = mvappend(\"Hello\",\"World\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,"
                + "StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resMvAppend = res.select("a").orderBy("a").distinct();
            List<Row> lst = resMvAppend.collectAsList();

            assertEquals("[Hello, World]", lst.get(0).getList(0).toString());
            assertEquals(1, lst.size());
        });
    }

    // Test eval method mvcount(mvfield)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseMvcountCatalystTest() {
        String q = "index=index_A | eval one_value = mvcount(mvappend(offset)) | eval two_values = mvcount(mvappend(index, offset))";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(one_value,StringType,true)," +
                "StructField(two_values,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get results
            Dataset<Row> res1V = res.select("one_value").orderBy("one_value").distinct();
            Dataset<Row> res2V = res.select("two_values").orderBy("two_values").distinct();

            // Collect results to list
            List<Row> lst1V = res1V.collectAsList();
            List<Row> lst2V = res2V.collectAsList();

            // Assert to expected values
            assertEquals("1", lst1V.get(0).get(0));
            assertEquals("2", lst2V.get(0).get(0));
        });
    }

    // Test eval method mvdedup(mvfield)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseMvdedupCatalystTest() {
        String q = "index=index_A | eval a = mvdedup(mvappend(\"1\",\"2\",\"3\",\"1\",\"2\",\"4\"))";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true)," +
                "StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true)," +
                "StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lstA = resA.collectAsList();
            assertEquals("[1, 2, 3, 4]", lstA.get(0).getList(0).toString());
        });
    }

    // Test eval method mvfilter(x)
    @Disabled
	@Test
    public void parseMvfilterCatalystTest() {
        String q = "index=index_A | eval email = mvappend(\"aa@bb.example.test\",\"aa@yy.example.test\",\"oo@ii.example.test\",\"zz@uu.example.test\",\"auau@uiui.example.test\") | eval a = mvfilter( email != \"aa@bb.example.test\" )";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resEmail = res.select("email");

            Dataset<Row> resA = res.select("a");
//            List<Row> lstA = resA.collectAsList();
//            assertEquals("1\n2\n3\n4", lstA.get(0).getString(0));
        });
    }

    // Test eval strptime()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalStrptimeCatalystTest() {
    	String q = "index=index_A | eval a=strptime(\"2018-08-13 11:22:33\",\"%Y-%m-%d %H:%M:%S\") " +
    			   "| eval b=strptime(\"2018-08-13 11:22:33 11 AM PST\",\"%Y-%m-%d %T %I %p %Z\") ";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true),StructField(b,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            // Assert equals with expected
            assertEquals(1534159353L, lstA.get(0).getLong(0));
            assertEquals(1534184553L, lstB.get(0).getLong(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalStrftimeCatalystTest() {
        String q = "index=index_A | eval a=strftime(1534159353,\"%Y-%m-%d %H:%M:%S\") " +
                "| eval b=strftime(1534188153,\"%Y-%m-%d %T %I %p %Z\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lstA = resA.collectAsList();
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            //Assert equals with expected
            assertEquals("2018-08-13 11:22:33", lstA.get(0).getString(0));
            assertEquals("2018-08-13 19:22:33 07 PM UTC", lstB.get(0).getString(0));
        });
    }

    // Test eval method split(field,delimiter)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSplitCatalystTest() {
    	String q = "index=index_A | eval a=split(\"a;b;c;d;e;f;g;h\",\";\") " +
    			   "| eval b=split(\"1,2,3,4,5,6,7,8,9,10\",\",\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,false),false),StructField(b,ArrayType(StringType,false),false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            // Assert equals with expected
            assertEquals("[a, b, c, d, e, f, g, h]", lstA.get(0).getList(0).toString());
            assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]", lstB.get(0).getList(0).toString());
        });
    }

    // Test eval method relative_time(unixtime, modifier)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalRelative_timeCatalystTest() {
    	String q = "index=index_A | eval a=relative_time(1645092037, \"-7d\") " +
    			   "| eval b=relative_time(1645092037,\"@d\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,true),StructField(b,LongType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            // Assert equals with expected
            assertEquals(1644487237L, lstA.get(0).getLong(0));
            assertEquals(1645048800L, lstB.get(0).getLong(0));
        });
    }

    // Test eval method min(x, ...) and max(x, ...)
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMinMaxCatalystTest() {
    	String q = "index=index_A | eval a=min(offset, offset - 2, offset - 3, offset - 4, offset - 5, offset) | eval b=max(offset, offset - 1, offset + 5) ";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Row> lstB = resB.collectAsList();

            Dataset<Row> srcDs = res.select("offset").orderBy("offset");
            List<Row> srcLst = srcDs.collectAsList();

            // Assert equals with expected
            for (int i = 0; i < srcLst.size(); i++) {
                assertEquals(srcLst.get(i).getLong(0) - 5, Double.parseDouble(lstA.get(i).getString(0)));
                assertEquals(srcLst.get(i).getLong(0) + 5, Double.parseDouble(lstB.get(i).getString(0)));
            }
        });
    }

    // Test eval method min(x, ...) and max(x, ...) with String
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMinMaxWithStringCatalystTest() {
        String q = "index=index_A | eval a=min(offset, \"foo\") | eval b=max(offset, \"foo\") ";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Row> lstB = resB.collectAsList();

            Dataset<Row> srcDs = res.select("offset").orderBy("offset");
            List<Row> srcLst = srcDs.collectAsList();

            // Assert equals with expected
            for (int i = 0; i < srcLst.size(); i++) {
                assertEquals(srcLst.get(i).getLong(0),Long.parseLong(lstA.get(i).getString(0)));
                assertEquals("foo", lstB.get(i).getString(0));
            }
        });
    }

    // Test eval method min(x, ...) and max(x, ...) with String
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMinMaxWithStringNumbersCatalystTest() {
        String q = "index=index_A | eval a=min(\"9\", \"10\", \"foo\") | eval b=max(\"9\", \"10\", \"foo\") ";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Row> lstB = resB.collectAsList();

            // Assert equals with expected
            for (int i = 0; i < lstA.size(); i++) {
                assertEquals("10", lstA.get(i).getString(0));
                assertEquals("foo", lstB.get(i).getString(0));
            }
        });
    }

    // Test eval method min(x, ...) and max(x, ...) with String
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalMinMaxWithStringDecimalsCatalystTest() {
        String q = "index=index_A | eval a=min(\"10.0\", \"4.7\") | eval b=max(\"10.0\", \"4.7\") ";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Row> lstA = resA.collectAsList();

            Dataset<Row> resB = res.select("b").orderBy("offset");
            List<Row> lstB = resB.collectAsList();

            // Assert equals with expected
            for (int i = 0; i < lstA.size(); i++) {
                assertEquals("4.7", lstA.get(i).getString(0));
                assertEquals("10.0", lstB.get(i).getString(0));
            }
        });
    }

    // Test eval json_valid()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalJSONValidCatalystTest() {
        String q = " index=index_A | eval a=json_valid(_raw) | eval b=json_valid(json_field)";
        String testFile = "src/test/resources/eval_test_json*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(json_field,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(xml_field,StringType,true),StructField(a,BooleanType,true)," +
                "StructField(b,BooleanType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();

            // Get column 'a'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            assertFalse(lst.get(0).getBoolean(0)); // _raw IS NOT json
            assertTrue(lstB.get(0).getBoolean(0)); // json_field IS json
        });
    }

    // Test spath() with JSON
    @Disabled
	@Test
    // FIXME broken due to spath udf changes
    public void parseEvalSpathJSONCatalystTest() {
    	String q = "index=index_A | eval a=spath(json_field, \"name\") | eval b=spath(json_field,\"invalid_spath\")";
        String testFile = "src/test/resources/eval_test_json*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(json_field,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(xml_field,StringType,true),StructField(a,StringType,true)," +
                "StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test spath() with XML
    // //person[age=30]/name/text()
    // FIXME broken due to spath udf changes
    @Disabled
	@Test
    public void parseEvalSpathXMLCatalystTest() {
    	String q = "index=index_A | eval a=spath(xml_field, \"people.person.name\")";
        String testFile = "src/test/resources/eval_test_json*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(json_field,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(xml_field,StringType,true),StructField(a,StringType,true))"; //, " +

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());

            //"StructField(b,StringType,true))";

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
        });
    }

    // Test eval method exact()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalExactCatalystTest() {
    	String q = "index=index_A | eval a=8.250 * 0.2 | eval b=exact(8.250 * 0.2)";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true),StructField(b,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("a").distinct();
            List<Row> lst = resA.collectAsList();

            // Get column 'b'
            Dataset<Row> resB = res.select("b").orderBy("b").distinct();
            List<Row> lstB = resB.collectAsList();

            // with and without exact() should be the same
            assertEquals(lst, lstB);
        });
    }

    // Test eval method searchmatch()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSearchmatchCatalystTest() {
    	String q = "index=index_A | eval test=searchmatch(\"index=index_A\") | eval test2=searchmatch(\"index=index_B\") | eval test3=searchmatch(\"offset<10 index=index_A sourcetype=a*\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(test,BooleanType,false)," +
                "StructField(test2,BooleanType,false),StructField(test3,BooleanType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'test'
            Dataset<Row> resA = res.select("test").orderBy("offset");
            List<Row> lst = resA.collectAsList();

            // Get column 'test2'
            Dataset<Row> resB = res.select("test2").orderBy("offset");
            List<Row> lstB = resB.collectAsList();

            // Get column 'test3'
            Dataset<Row> resC = res.select("test3").orderBy("offset");
            List<Row> lstC = resC.collectAsList();

            /* eval test = searchmatch("index=index_A") |
             * eval test2 = searchmatch("index=index_B") |
             * eval test3 = searchmatch("offset<10 index=index_A sourcetype=a*")
             */

            // assert that all rows are kept
            assertEquals(19, lst.size());
            assertEquals(19, lstB.size());
            assertEquals(19, lstC.size());

            int loopCount = 0;
            for (int i = 0; i < lst.size(); i++) {
                // eval test results in all TRUE
                assertTrue(lst.get(i).getBoolean(0));
                // eval test2 results in all FALSE
                assertFalse(lstB.get(i).getBoolean(0));


                // eval test3, values between i=0..6 are TRUE, otherwise FALSE
                if (i < 6) {
                    assertTrue(lstC.get(i).getBoolean(0));
                }
                else {
                    assertFalse(lstC.get(i).getBoolean(0));
                }
                loopCount++;
            }
            assertEquals(19, loopCount);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalSearchmatchImplicitRawCatalystTest() {
        String q = "index=index_A | eval test=searchmatch(\"*cOmPuter02.example.com*\")";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true)," +
                "StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true)," +
                "StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(test,BooleanType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'test'
            Dataset<Row> resA = res.select("_raw", "test").orderBy("offset");
            List<Row> lst = resA.collectAsList();

            // assert that all rows are kept
            assertEquals(19, lst.size());

            // check that _raw column contains the expected string, and it matches the result of searchmatch
            int loopCount = 0;
            for (Row r : lst) {
                assertEquals(r.getString(0).toLowerCase().contains("computer02.example.com"), r.getBoolean(1));
                loopCount++;
            }
            assertEquals(19, loopCount);
        });
    }

    // Test eval now() and time()
    @Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEval_Now_Time_CatalystTest() {
        String q = " index=index_A | eval a=now() | eval b=time()";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,LongType,false),StructField(b,StringType,false))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
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
        });
    }

    // Test that eval arithmetics only works for Strings with the + operator
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalArithmeticsWithStringTest() {
        Exception exceptionMinus = assertThrows(IllegalArgumentException.class, () -> {
            new EvalArithmetic().call("string", "-", "string");
        });

        Exception exceptionMultiply = assertThrows(IllegalArgumentException.class, () -> {
            new EvalArithmetic().call("string", "*", "string");
        });

        Exception exceptionDivide = assertThrows(IllegalArgumentException.class, () -> {
            new EvalArithmetic().call("string", "/", "string");
        });

        Exception exceptionRemainder = assertThrows(IllegalArgumentException.class, () -> {
            new EvalArithmetic().call("string", "%", "string");
        });

        String expectedMessage = "Eval arithmetics only allow Strings for the + operator.";

        assertEquals(expectedMessage, exceptionMinus.getMessage());
        assertEquals(expectedMessage, exceptionMultiply.getMessage());
        assertEquals(expectedMessage, exceptionDivide.getMessage());
        assertEquals(expectedMessage, exceptionRemainder.getMessage());
    }

    // Test that eval arithmetic + operation concatenates Strings
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void parseEvalArithmeticsWithString_2_Test() {
        String q = "index=index_A | eval a=offset+\"string\"";
        String testFile = "src/test/resources/eval_test_data1*.json"; // * to make the path into a directory path
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,StringType,true))";

        streamingTestUtil.performDPLTest(q, testFile, res -> {
            assertEquals(schema, res.schema().toString());
            // Get column 'a'
            Dataset<Row> resA = res.select("a").orderBy("offset");
            List<Row> lst = resA.collectAsList();

            // Start from i = 3 because there are multiple 1's in offset
            for (int i = 1; i < 17; i++) {
                assertEquals(i + "string", lst.get(i+2).getString(0));
            }
        });
    }

    // Tests whether eval arithmetics work after spath
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_arithmetics() {
        String query = "index=index_A | spath path= json | eval a = json - 1";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,StringType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
                List<String> a = ds.select("a").orderBy("id").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> expected = new ArrayList<>(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
                assertEquals(expected, a);
            }
        );
    }

    // Tests whether eval arithmetics work after spath
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_arithmetics2() {
        String query = "index=index_A | spath path= json | eval a = json + 1";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,StringType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
                List<String> a = ds.select("a").orderBy("id").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> expected = new ArrayList<>(Arrays.asList("2", "3", "4", "5", "6", "7", "8", "9", "10", "11"));
                assertEquals(expected, a);
            }
        );
    }

    // Tests whether eval arithmetics work after spath
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_arithmetics3() {
        String query = "index=index_A | spath path= json | eval a = json / 2";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,StringType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
                List<String> a = ds.select("a").orderBy("id").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> expected = new ArrayList<>(Arrays.asList("0.5", "1", "1.5", "2", "2.5", "3", "3.5", "4", "4.5", "5"));
                assertEquals(expected, a);
            }
        );
    }

    // Tests whether eval arithmetics work after spath
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_arithmetics4() {
        String query = "index=index_A | spath path= json | eval a = json / 3";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,StringType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
                List<String> a = ds.select("a").orderBy("id").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> expected = new ArrayList<>(Arrays.asList("0.3333333", "0.6666667", "1", "1.3333333", "1.6666667", "2", "2.3333333", "2.6666667", "3", "3.3333333"));
                assertEquals(expected, a);
            }
        );
    }

    // Tests whether eval arithmetics work after spath
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_arithmetics5() {
        String query = "index=index_A | spath path= json | eval a = json * 5";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,StringType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
                List<String> a = ds.select("a").orderBy("id").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                List<String> expected = new ArrayList<>(Arrays.asList("5", "10", "15", "20", "25", "30", "35", "40", "45", "50"));
                assertEquals(expected, a);
            }
        );
    }

    // Tests whether eval arithmetics work after spath
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_arithmetics6() {
        String query = "index=index_A | spath path= json | eval a = json % 2";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,StringType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
                    List<String> a = ds.select("a").orderBy("id").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
                    List<String> expected = new ArrayList<>(Arrays.asList("1", "0", "1", "0", "1", "0", "1", "0", "1", "0"));
                    assertEquals(expected, a);
                }
        );
    }

    // Eval comparisons with mixed types (string and number) isn't allowed and should throw an exception
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationExceptionTest() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new EvalOperation().call("string", DPLLexer.EVAL_LANGUAGE_MODE_GT, 4);
        });
        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> {
            new EvalOperation().call(0, DPLLexer.EVAL_LANGUAGE_MODE_EQ, "string");
        });
        assertDoesNotThrow(() -> {
            new EvalOperation().call(5, DPLLexer.EVAL_LANGUAGE_MODE_EQ, 4.5);
        });
        assertDoesNotThrow(() -> {
            new EvalOperation().call("string", DPLLexer.EVAL_LANGUAGE_MODE_EQ, "another string");
        });

        String expectedMessage = "Eval comparisons only allow using two numbers or two strings.";
        assertEquals(expectedMessage, exception.getMessage());
        assertEquals(expectedMessage, exception2.getMessage());
    }

    // Tests EvalOperation equals
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationEqTest() {
        String query = "index=index_A | eval a = if(offset == 1, \"true\", \"false\") | eval b = if(sourcetype == \"A:X:0\", \"true\", \"false\")";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true),StructField(b,ArrayType(StringType,true),true))";
        String testFile = "src/test/resources/eval_test_data1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());
            List<String> b = ds.select("b").orderBy("_time").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            for (int i = 0; i < a.size(); i++) {
                if (i < 4) {
                    assertEquals("true", a.get(i));
                } else {
                    assertEquals("false", a.get(i));
                }

                if (i < 3) {
                    assertEquals("true", b.get(i));
                } else {
                    assertEquals("false", b.get(i));
                }
            }
        });
    }

    // Tests EvalOperation not equals
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationNeqTest() {
        String query = "index=index_A | eval a = if(offset != 1, \"true\", \"false\") | eval b = if(sourcetype != \"A:X:0\", \"true\", \"false\")";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true),StructField(b,ArrayType(StringType,true),true))";
        String testFile = "src/test/resources/eval_test_data1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());
            List<String> b = ds.select("b").orderBy("_time").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            for (int i = 0; i < a.size(); i++) {
                if (i < 4) {
                    assertEquals("false", a.get(i));
                } else {
                    assertEquals("true", a.get(i));
                }

                if (i < 3) {
                    assertEquals("false", b.get(i));
                } else {
                    assertEquals("true", b.get(i));
                }
            }
        });
    }

    // Tests EvalOperation greater than
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationGtTest() {
        String query = "index=index_A | eval a = if(offset > 1, \"true\", \"false\") | eval b = if(sourcetype > \"A:X:0\", \"true\", \"false\")";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true),StructField(b,ArrayType(StringType,true),true))";
        String testFile = "src/test/resources/eval_test_data1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());
            List<String> b = ds.select("b").orderBy("_time").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            for (int i = 0; i < a.size(); i++) {
                if (i < 4) {
                    assertEquals("false", a.get(i));
                } else {
                    assertEquals("true", a.get(i));
                }

                if (i < 3) {
                    assertEquals("false", b.get(i));
                } else {
                    assertEquals("true", b.get(i));
                }
            }
        });
    }

    // Tests EvalOperation greater than or equal to
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationGteTest() {
        String query = "index=index_A | eval a = if(offset >= 2, \"true\", \"false\") | eval b = if(sourcetype >= \"b:X:0\", \"true\", \"false\")";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true),StructField(b,ArrayType(StringType,true),true))";
        String testFile = "src/test/resources/eval_test_data1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());
            List<String> b = ds.select("b").orderBy("_time").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            for (int i = 0; i < a.size(); i++) {
                if (i < 4) {
                    assertEquals("false", a.get(i));
                } else {
                    assertEquals("true", a.get(i));
                }

                if (i < 3 || i > 14) {
                    assertEquals("false", b.get(i));
                } else {
                    assertEquals("true", b.get(i));
                }
            }
        });
    }

    // Tests EvalOperation less than
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationLtTest() {
        String query = "index=index_A | eval a = if(offset < 2, \"true\", \"false\") | eval b = if(sourcetype < \"b:X:0\", \"true\", \"false\")";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true),StructField(b,ArrayType(StringType,true),true))";
        String testFile = "src/test/resources/eval_test_data1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());
            List<String> b = ds.select("b").orderBy("_time").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            for (int i = 0; i < a.size(); i++) {
                if (i < 4) {
                    assertEquals("true", a.get(i));
                } else {
                    assertEquals("false", a.get(i));
                }

                if (i < 3 || i > 14) {
                    assertEquals("true", b.get(i));
                } else {
                    assertEquals("false", b.get(i));
                }
            }
        });
    }

    // Tests EvalOperation less than or equal to
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalOperationLteTest() {
        String query = "index=index_A | eval a = if(offset <= 2, \"true\", \"false\") | eval b = if(sourcetype <= \"b:X:0\", \"true\", \"false\")";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(a,ArrayType(StringType,true),true),StructField(b,ArrayType(StringType,true),true))";
        String testFile = "src/test/resources/eval_test_data1*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());
            List<String> b = ds.select("b").orderBy("_time").collectAsList().stream().map(r -> r.getList(0).get(0).toString()).collect(Collectors.toList());

            for (int i = 0; i < a.size(); i++) {
                if (i < 5) {
                    assertEquals("true", a.get(i));
                } else {
                    assertEquals("false", a.get(i));
                }

                if (i < 6 || i > 14) {
                    assertEquals("true", b.get(i));
                } else {
                    assertEquals("false", b.get(i));
                }
            }
        });
    }

    // Tests EvalOperation after spath (spath makes all data into String)
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void evalAfterSpath_ComparisonTest() {
        String query = "index=index_A | spath path= json | eval a= json > 40";
        String schema = "StructType(StructField(_raw,StringType,true),StructField(_time,TimestampType,true),StructField(host,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true),StructField(json,StringType,true),StructField(a,BooleanType,true))";
        String testFile = "src/test/resources/spath/spathTransformationTest_numeric2*.json";

        streamingTestUtil.performDPLTest(query, testFile, ds -> {
            assertEquals(schema, ds.schema().toString());
            List<String> a = ds.select("a").orderBy("offset").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            List<String> expected = new ArrayList<>(Arrays.asList("false", "false", "false", "false", "true", "true", "true", "true", "true", "true"));

            assertEquals(expected, a);
        });
    }
}
