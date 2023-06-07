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
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConvertTransformationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConvertTransformationTest.class);

	DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor catalystVisitor;
    // Use this file for dataset initialization
    String testFile = "src/test/resources/convertTfData.json";
    //String testFile = "src/test/resources/statsTransf_Values_TestData.json";
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


    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        catalystVisitor = null;
    }
    
    @org.junit.jupiter.api.AfterAll
    void recover() {
    	TimeZone.setDefault(originalTimeZone);
    }

    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void convert1_ctime() {
    	// "%m/%d/%Y %H:%M:%S";
    	performDPLTest(
    			"index=index_A | convert ctime(offset) AS new",
    			"_raw, _time, dur, host, index, offset, partition, source, sourcetype, new",
    			ds -> {
    				List<String> listOfResults = ds.select("new").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				for (String s : listOfResults) {
    					// match 00/00/0000 00:00:00
    					Matcher m = Pattern.compile("\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}").matcher(s);
    					
    					assertTrue(m.find());
    				}
    			}
    	);
    }
    
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void convert2_ctime() {
    	performDPLTest(
    			"index=index_A | convert ctime(offset)",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype",
    			ds -> {
    				List<String> listOfResults = ds.select("offset").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				for (String s : listOfResults) {
    					// match 00/00/0000 00:00:00
    					Matcher m = Pattern.compile("\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}").matcher(s);
    					
    					assertTrue(m.find());
    				}
    			}
    	);
    }
    
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void convert3_mktime() {
    	performDPLTest(
    			"index=index_A | convert timeformat=\"%Y-%m-%d'T'%H:%M:%S.%f%z\" mktime(_time) as epochTime",
    			"_raw, _time, dur, host, index, offset, partition, source, sourcetype, epochTime",
    			ds -> {
    				List<String> listOfResults = ds.select("epochTime").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				List<String> expectedResults = Arrays.asList(
    						"978303661",
    						"1012608122",
    						"1046653383",
    						"1081040644",
    						"1115258705",
    						"1149563166",
    						"1183781227",
    						"1218172088",
    						"1252476549",
    						"1286694610",
    						"1286698210",
    						"1286698810");
    				
    				assertEquals(expectedResults, listOfResults);
    			}
    	);
    	
    	
    }
    
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void convert4_dur2sec() {
    	performDPLTest(
    			"index=index_A | convert dur2sec(dur) as dur_sec",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, dur_sec",
    			ds -> {
    				List<String> listOfResults = ds.select("dur_sec").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				List<String> expectedResults = Arrays.asList(
    						"45296",
    						"24202",
    						"1403",
    						"7432",
    						"22",
    						"3723",
    						"2400",
    						"3661",
    						"3600",
    						"195792",
    						"0",
    						"5430");
    				
    				assertEquals(expectedResults, listOfResults);
    			}
    	);
    	
    	
    }
    
    @Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
    void convert5_memk() {
    	performDPLTest(
    			"index=index_A | strcat offset \"m\" offsetM | strcat offset \"k\" offsetK | strcat offset \"g\" offsetG | convert memk(offsetM) as memk_M memk(offsetK) as memk_K memk(offsetG) as memk_G memk(offset) as memk_def",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, offsetM, offsetK, offsetG, memk_M, memk_K, memk_G, memk_def",
    			ds -> {
    				List<String> resDef = ds.select("memk_def").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				List<String> resK = ds.select("memk_K").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				List<String> resM = ds.select("memk_M").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				List<String> resG = ds.select("memk_G").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
    				List<String> expDef = Arrays.asList(
    						"1.0",
    						"2.0",
    						"3.0",
    						"4.0",
    						"5.0",
    						"6.0",
    						"7.0",
    						"8.0",
    						"9.0",
    						"10.0",
    						"11.0",
    						"11.0");
    				List<String> expM = Arrays.asList(
    						"1024.0",
    						"2048.0",
    						"3072.0",
    						"4096.0",
    						"5120.0",
    						"6144.0",
    						"7168.0",
    						"8192.0",
    						"9216.0",
    						"10240.0",
    						"11264.0",
    						"11264.0");
    				List<String> expG = Arrays.asList(
    						"1048576.0",
    						"2097152.0",
    						"3145728.0",
    						"4194304.0",
    						"5242880.0",
    						"6291456.0",
    						"7340032.0",
    						"8388608.0",
    						"9437184.0",
    						"1.048576E7",
    						"1.1534336E7",
    						"1.1534336E7");
    				
    				assertEquals(expDef, resDef);
    				assertEquals(expDef, resK); // def is same as K
    				assertEquals(expM, resM);
    				assertEquals(expG, resG);
    			}
    	);
    }
    	
    	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
        void convert6_mstime() {
        	performDPLTest(
        			"index=index_A | strcat \"\" \"47.\" \"329\" mst | strcat \"32:\" \"47.\" \"329\" mst2 | convert mstime(mst) as res mstime(mst2) as res2",
					"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, mst2, res, res2",
        			ds -> {
        				List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
        				List<String> listOfResults2 = ds.select("res2").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
        				List<String> expectedResults = Collections.singletonList(
								"47329");
        				List<String> expectedResults2 = Collections.singletonList(
								"1967329");
        				
        				assertEquals(expectedResults, listOfResults);
        				assertEquals(expectedResults2, listOfResults2);
        			}
        	);
    	}
    	
    	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
        void convert7_rmcomma() {
        	performDPLTest(
        			"index=index_A | strcat \"\" \"47,\" \"329\" mst | strcat \"32,\" \"47,\" \"329\" mst2 | convert rmcomma(mst) as res rmcomma(mst2) as res2",
					"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, mst2, res, res2",
        			ds -> {
        				List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
        				List<String> listOfResults2 = ds.select("res2").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
        				List<String> expectedResults = Collections.singletonList(
								"47329");
        				List<String> expectedResults2 = Collections.singletonList(
								"3247329");
        				
        				assertEquals(expectedResults, listOfResults);
        				assertEquals(expectedResults2, listOfResults2);
        			}
        	);
    	}
    	
    	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
        void convert8_rmunit() {
        	performDPLTest(
        			"index=index_A | strcat \"329\" \"abc\" as mst | convert rmunit(mst) as res",
					"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
        			ds -> {
        				List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

        				List<String> expectedResults = Collections.singletonList(
								"329");
        				
        				assertEquals(expectedResults, listOfResults);
        			}
        	);
    	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit2() {
		performDPLTest(
				"index=index_A | strcat \"329.45\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"329.45");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit3() {
		performDPLTest(
				"index=index_A | strcat \".54e2\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							".54E2");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit4() {
		performDPLTest(
				"index=index_A | strcat \"-0.54e2\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"-0.54E2");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit5() {
		performDPLTest(
				"index=index_A | strcat \"-0.21.54e2\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit6() {
		performDPLTest(
				"index=index_A | strcat \"+21.54e23\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"+21.54E23");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit7() {
		performDPLTest(
				"index=index_A | strcat \"+21.54e-23\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"+21.54E-23");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert8_rmunit8() {
		performDPLTest(
				"index=index_A | strcat \"+21.54e+23\" \"abc\" as mst | convert rmunit(mst) as res",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, mst, res",
				ds -> {
					List<String> listOfResults = ds.select("res").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"+21.54E+23");

					assertEquals(expectedResults, listOfResults);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert9_auto() {
		performDPLTest(
				"index=index_A | strcat \"329\" \"\" with_results |strcat \"329\" \"aa\" no_results | convert auto(with_results) | convert auto(no_results)",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, with_results, no_results",
				ds -> {
					List<String> listOfResults = ds.select("with_results").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
					List<String> listOfResults2 = ds.select("no_results").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"329.0");

					List<String> expectedResults2 = Collections.singletonList(
							"329aa");

					assertEquals(expectedResults, listOfResults);
					assertEquals(expectedResults2, listOfResults2);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert10_num() {
		performDPLTest(
				"index=index_A | strcat \"329\" \"\" with_results |strcat \"329\" \"aa\" no_results | convert num(with_results) | convert num(no_results)",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, with_results, no_results",
				ds -> {
					List<String> listOfResults = ds.select("with_results").limit(1).collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
					List<String> listOfResults2 = ds.select("no_results").limit(1).collectAsList().stream().map(r -> {
						return r.getAs(0) == null ? "null" : r.getAs(0).toString();
					}).collect(Collectors.toList());

					List<String> expectedResults = Collections.singletonList(
							"329.0");

					List<String> expectedResults2 = Collections.singletonList(
							"null");

					assertEquals(expectedResults, listOfResults);
					assertEquals(expectedResults2, listOfResults2);
				}
		);
	}

	@Test
	@EnabledIfSystemProperty(named="runSparkTest", matches="true")
	void convert11_none() {
		performDPLTest(
				"index=index_A | convert dur2sec(\"dur|offset\") AS dur_sec none(offset)",
				"_raw, _time, dur, host, index, offset, partition, source, sourcetype, dur_sec",
				ds -> {
					List<String> listOfResults = ds.select("dur_sec").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
					List<String> expectedResults = Arrays.asList(
							"45296",
							"24202",
							"1403",
							"7432",
							"22",
							"3723",
							"2400",
							"3661",
							"3600",
							"195792",
							"0",
							"5430");

					assertEquals(expectedResults, listOfResults);
				}
		);


	}
    
    
    /**
     * Performs a DPL query and tests that the resulting Dataset contains the given targetColumn with expected values
     * @param query DPL Query
     */
    void performDPLTest(String query, String targetColumn, Consumer<Dataset<Row>> assertions) {
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
        if (targetColumn != null) {
        	assertEquals(Arrays.toString(res.columns()), "[" + targetColumn + "]");
        }
        else {
        	LOGGER.error("Test did not contain a targetColumn, no assertions");
        }

        // call consumer with result dataset
        assertions.accept(res);
    }
}
