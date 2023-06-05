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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class statsTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(statsTransformationTest.class);

    DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor catalystVisitor;
    // Use this file for dataset initialization
    String testFile = "src/test/resources/statsTransformationTestData.json";
    //String testFile = "src/test/resources/statsTransf_Values_TestData.json";
    SparkSession spark = null;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
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


    // --- SQL emit mode tests ---
    
    // --- XML emit mode tests ---
    
    // --- Catalyst emit mode tests ---

    // Explanation: 
    // UDAF = User Defined Aggregate Function, deprecated in spark 3.x, not performant enough in many cases
    // aggregator = custom aggregator, replaces UDAF in spark 3.x and above, performance vastly improved compared to UDAF
    // spark = uses built-in spark function
    
    /*
     * -- Command --	-- Status --
     * exactperc() 		aggregator
     * perc() 			spark(with PercentileApprox.java)
     * upperperc()		-
     * rate() 			aggregator
     * earliest() 		aggregator
     * earliest_time() 	aggregator
     * values() 		aggregator
     * list() 			aggregator
     * median() 		aggregator
     * mode() 			aggregator
     * min() 			spark
     * max() 			spark
     * stdev stdevp() 	spark
     * sum() 			spark
     * sumsq() 			spark
     * dc() 			aggregator
     * estdc() 			spark
     * estdc_error() 	udaf*
     * range() 			spark
     * count() 			spark
     * avg() 			spark
     * var varp() 		spark
     * first() 			spark
     * last() 			spark
     * latest() 		aggregator
     * latest_time() 	aggregator
     */
    
    // Test exactpercX()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggExactPerc_Test() {
    	performDPLTest("index=index_A | stats exactperc50(offset) AS perc_offset", Collections.singletonList("6.5"), "perc_offset");
    }
    
    // Test percX()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggPerc_Test() {
    	performDPLTest("index=index_A | stats perc50(offset) AS perc_offset", Collections.singletonList("6"), "perc_offset");
    }
    
    // Test rate()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggRate_Test() {
    	performDPLTest("index=index_A | stats rate(offset) AS rate_offset", Collections.singletonList("3.2425553062149416E-8"), "rate_offset");
    }
    
    // Test earliest()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggEarliest_Test() {
    	performDPLTest("index=index_A | stats earliest(offset) AS earliest_offset", Collections.singletonList("1"), "earliest_offset");
    }
    
    // Test earliest_time()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggEarliestTime_Test() {
    	performDPLTest("index=index_A | stats earliest_time(offset) AS earliest_time_offset", Collections.singletonList("978310861"), "earliest_time_offset");
    }
    
    // Test values()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggValues_Test() {
    	performDPLTest("index=index_A | stats values(offset) AS values_offset", Collections.singletonList("1\n10\n11\n2\n3\n4\n5\n6\n7\n8\n9"), "values_offset");
    }
    
    // Test list()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggList_Test() {
    	performDPLTest("index=index_A | stats list(offset) AS list_offset", Collections.singletonList("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n11"), "list_offset");
    }
    
    // Test median()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggMedian_Test() {
    	performDPLTest("index=index_A | stats median(offset) AS median_offset", Collections.singletonList("6.5"), "median_offset");
    }
    
	// Test mode()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggMode_Test() {
        performDPLTest("index=index_A | stats mode(offset) AS mode_offset", Collections.singletonList("11"), "mode_offset");
    }
    
    // Test min()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggMin_Test() {
    	performDPLTest("index=index_A | stats min(offset) AS min_offset", Collections.singletonList("1"), "min_offset");
    }
    
    // Test max()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggMax_Test() {
    	performDPLTest("index=index_A | stats max(offset) AS max_offset", Collections.singletonList("11"), "max_offset");
    }
    
    // Test stdev()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggStdev_Test() {
    	performDPLTest("index=index_A | stats stdev(offset) AS stdev_offset", Collections.singletonList("3.4761089357690347"), "stdev_offset");
    }
    
    // Test stdevp()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggStdevp_Test() {
    	performDPLTest("index=index_A | stats stdevp(offset) AS stdevp_offset", Collections.singletonList("3.3281209246193093"), "stdevp_offset");
    }
    
    // Test sum()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggSum_Test() {
    	performDPLTest("index=index_A | stats sum(offset) AS sum_offset", Collections.singletonList("77"), "sum_offset");
    }
    
    // Test sumsq()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggSumsq_Test() {
    	performDPLTest("index=index_A | stats sumsq(offset) AS sumsq_offset", Collections.singletonList("627.0"), "sumsq_offset");
    }
    
    // Test dc()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggDc_Test() {
    	performDPLTest("index=index_A | stats dc(offset) AS dc_offset", Collections.singletonList("11"), "dc_offset");
    }
    
    // Test estdc()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggEstdc_Test() {
    	performDPLTest("index=index_A | stats estdc(offset) AS estdc_offset", Collections.singletonList("11"), "estdc_offset");
    }
    
    // Test estdc_error()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggEstdc_error_Test() {
    	performDPLTest("index=index_A | stats estdc_error(offset) AS estdc_error_offset", Collections.singletonList("0.0"), "estdc_error_offset");
    }
    
    // Test range()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggRange_Test() {
    	performDPLTest("index=index_A | stats range(offset) AS range_offset", Collections.singletonList("10"), "range_offset");
    }
    
    // Test count()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggCount_Test() {
    	performDPLTest("index=index_A | stats count(offset) AS count_offset", Collections.singletonList("12"), "count_offset");
    }
    
    // Test avg()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggAvg_Test() {
    	performDPLTest("index=index_A | stats avg(offset)", Collections.singletonList("6.416666666666667"), "avg(offset)");
    }
    
    // Test mean()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggMean_Test() {
    	performDPLTest("index=index_A | stats mean(offset)", Collections.singletonList("6.416666666666667"), "mean(offset)");
    }
    
    // Test var()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggVar_Test() {
    	performDPLTest("index=index_A | stats var(offset) AS var_offset", Collections.singletonList("12.083333333333332"), "var_offset");
    }
    
    // Test varp()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggVarp_Test() {
    	performDPLTest("index=index_A | stats varp(offset) AS varp_offset", Collections.singletonList("11.076388888888888"), "varp_offset");
    }
    
    // Test multiple aggregations at once
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_Agg_MultipleTest() {
    	performDPLTest("index=index_A | stats var(offset) AS var_offset avg(offset) AS avg_offset", null, "var_offset, avg_offset");
    }
    
    // Test BY field,field
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_Agg_ByTest() {
    	performDPLTest("index=index_A | stats avg(offset) AS avg_offset BY sourcetype,host", null, "sourcetype, host, avg_offset");
    }
    
    // Test first()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggFirst_Test() {
    	performDPLTest("index=index_A | stats first(offset) AS first_offset", Collections.singletonList("1"), "first_offset");
    }
    
    // Test last()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggLast_Test() {
    	performDPLTest("index=index_A | stats last(offset) AS last_offset", Collections.singletonList("11"), "last_offset");
    }
    
    // Test latest()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggLatest_Test() {
    	performDPLTest("index=index_A | stats latest(offset) AS latest_offset", Collections.singletonList("11"), "latest_offset");
    }
    
    // Test latest_time()
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void statsTransform_AggLatestTime_Test() {
    	performDPLTest("index=index_A | stats latest_time(offset) AS latest_time_offset", Collections.singletonList("1286709610"), "latest_time_offset");
    }
    
    /**
     * Performs a DPL query and tests that the resulting Dataset contains the given targetColumn with expected values
     * @param query DPL Query
     * @param expectedValues Expected values as List of Strings. If this is null, the contents will not be asserted/checked
     * @param targetColumn Target column, for example: avg(offset) AS avg_offset would mean that the target column is avg_offset
     */
    void performDPLTest(String query, List<String> expectedValues, String targetColumn) {
        ctx.setEarliest("-1Y");
        
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        
        Dataset<Row> res = n.getDataset();
        
        LOGGER.info("Results after query: " + query + " ; Schema: " + res.toString());
        LOGGER.info("-------------------");
        res.show(15, false);

        // check if result contains the expected column, and only that column
        assertEquals(Arrays.toString(res.columns()), "[" + targetColumn + "]");
                
        // Destination field from result dataset<row>
        if (expectedValues != null) {
        	List<String> destAsList = res.select(targetColumn).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
        	// assert dest field contents as equals with expected contents
            assertEquals(expectedValues, destAsList); 
        }
    }
}
