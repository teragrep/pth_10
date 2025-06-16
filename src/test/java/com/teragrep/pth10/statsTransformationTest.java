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
package com.teragrep.pth10;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class statsTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(statsTransformationTest.class);

    // Use this file for dataset initialization
    String testFile = "src/test/resources/statsTransformationTestData*.jsonl"; // * to make the path into a directory path
    private StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
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
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggExactPerc_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats exactperc50(offset) AS perc_offset", testFile, ds -> {
            Assertions.assertEquals("[perc_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("perc_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("6.5"), destAsList);
        });
    }

    // Test percX()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggPerc_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats perc50(offset) AS perc_offset", testFile, ds -> {
            Assertions.assertEquals("[perc_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("perc_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("6"), destAsList);
        });
    }

    // Test rate()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggRate_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats rate(offset) AS rate_offset", testFile, ds -> {
            Assertions.assertEquals("[rate_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("rate_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("3.2425553062149416E-8"), destAsList);
        });
    }

    // Test earliest()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggEarliest_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats earliest(offset) AS earliest_offset", testFile, ds -> {
            Assertions.assertEquals("[earliest_offset]", Arrays.toString(ds.columns()));
            List<String> destAsList = ds
                    .select("earliest_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("1"), destAsList);
        });
    }

    // Test earliest() and latest() combination
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggEarliestAndLatestCombo_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats earliest(offset), latest(offset)", testFile, ds -> {
            Assertions.assertEquals("[earliest(offset), latest(offset)]", Arrays.toString(ds.columns()));
            List<Row> destAsList = ds.select("earliest(offset)", "latest(offset)").collectAsList();
            Assertions.assertEquals("1", destAsList.get(0).getString(0));
            Assertions.assertEquals("11", destAsList.get(0).getString(1));
        });
    }

    // Test earliest() with no data
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggEarliestNoData_Test() {
        streamingTestUtil
                .performDPLTest("index=index_XYZ | stats earliest(offset) AS earliest_offset", testFile, ds -> {
                    Assertions.assertEquals("[earliest_offset]", Arrays.toString(ds.columns()));

                    List<String> destAsList = ds
                            .select("earliest_offset")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Collections.singletonList(""), destAsList);
                });
    }

    // Test latest() with no data
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggLatestNoData_Test() {
        streamingTestUtil.performDPLTest("index=index_XYZ | stats latest(offset) AS latest_offset", testFile, ds -> {
            Assertions.assertEquals("[latest_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("latest_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(""), destAsList);
        });
    }

    // Test earliest_time()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggEarliestTime_Test() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats earliest_time(offset) AS earliest_time_offset", testFile, ds -> {
                    Assertions.assertEquals("[earliest_time_offset]", Arrays.toString(ds.columns()));

                    List<String> destAsList = ds
                            .select("earliest_time_offset")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Collections.singletonList("978310861"), destAsList);
                });
    }

    // Test values()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggValues_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats values(offset) AS values_offset", testFile, ds -> {
            Assertions.assertEquals("[values_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("values_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("1\n10\n11\n2\n3\n4\n5\n6\n7\n8\n9"), destAsList);
        });
    }

    // Test list()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggList_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats list(offset) AS list_offset", testFile, ds -> {
            Assertions.assertEquals("[list_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("list_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n11"), destAsList);
        });
    }

    // Test median()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggMedian_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats median(offset) AS median_offset", testFile, ds -> {
            Assertions.assertEquals("[median_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("median_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("6.5"), destAsList);
        });
    }

    // Test mode()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggMode_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats mode(offset) AS mode_offset", testFile, ds -> {
            Assertions.assertEquals("[mode_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("mode_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11"), destAsList);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransformAggModeTestEmptyDataset() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-10-10T11:20:10.100+03:00 | stats mode(offset)", testFile, ds -> {
                            Assertions.assertEquals("[mode(offset)]", Arrays.toString(ds.columns()));

                            List<Object> destAsList = ds
                                    .select("mode(offset)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            Assertions.assertEquals(Collections.singletonList(""), destAsList);
                        }
                );
    }

    // Test min()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggMin_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats min(offset) AS min_offset", testFile, ds -> {
            Assertions.assertEquals("[min_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("min_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("1"), destAsList);
        });
    }

    // Test max()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggMax_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats max(offset) AS max_offset", testFile, ds -> {
            Assertions.assertEquals("[max_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("max_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11"), destAsList);
        });
    }

    // Test stdev()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggStdev_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats stdev(offset) AS stdev_offset", testFile, ds -> {
            Assertions.assertEquals("[stdev_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("stdev_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("3.4761089357690347"), destAsList);
        });
    }

    // Test stdevp()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggStdevp_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats stdevp(offset) AS stdevp_offset", testFile, ds -> {
            Assertions.assertEquals("[stdevp_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("stdevp_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("3.3281209246193093"), destAsList);
        });
    }

    // Test sum()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggSum_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats sum(offset) AS sum_offset", testFile, ds -> {
            Assertions.assertEquals("[sum_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("sum_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("77"), destAsList);
        });
    }

    // Test sum() with MV field input
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggSum_mvField_Test() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | eval mv = mvappend(offset, offset+1) | stats sum(mv) AS sum_mv", testFile,
                        ds -> {
                            Assertions.assertEquals("[sum_mv]", Arrays.toString(ds.columns()));

                            List<String> destAsList = ds
                                    .select("sum_mv")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Collections.singletonList("166"), destAsList);
                        }
                );
    }

    // Test sum() with MV field input
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggSum_mvField_GH261_Test() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A offset < 3" + "| eval atk = if(offset=0, 1, 0) "
                                + "| eval def = if(offset=1, 2, 1) " + "| eval spy = if(offset=2, 4, 6)"
                                + "| stats sum(atk) AS attack, sum(def) AS defend, sum(spy) as spying",
                        testFile, ds -> {
                            List<String> atk = ds
                                    .select("attack")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> def = ds
                                    .select("defend")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            List<String> spy = ds
                                    .select("spying")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());

                            // should be one of each
                            Assertions.assertEquals(1, atk.size());
                            Assertions.assertEquals(1, def.size());
                            Assertions.assertEquals(1, spy.size());
                            // aggregate results
                            Assertions.assertEquals("0", atk.get(0));
                            Assertions.assertEquals("3", def.get(0));
                            Assertions.assertEquals("10", spy.get(0));
                        }
                );
    }

    // Test sumsq()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggSumsq_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats sumsq(offset) AS sumsq_offset", testFile, ds -> {
            Assertions.assertEquals("[sumsq_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("sumsq_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("627.0"), destAsList);
        });
    }

    // Test dc()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggDc_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats dc(offset) AS dc_offset", testFile, ds -> {
            Assertions.assertEquals("[dc_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("dc_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11"), destAsList);
        });
    }

    // Test dc() with NULL data
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggDc_NoData_Test() {
        // rex4j is used to produce nulls here
        streamingTestUtil
                .performDPLTest(
                        "| makeresults | eval raw=\"kissa@1\"| rex4j field=raw \"koira@(?<koira>\\d)\" | stats dc(koira)",
                        testFile, ds -> {
                            Assertions.assertEquals("[dc(koira)]", Arrays.toString(ds.columns()));

                            List<String> destAsList = ds
                                    .select("dc(koira)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> r.getAs(0).toString())
                                    .collect(Collectors.toList());
                            Assertions.assertEquals(Collections.singletonList("0"), destAsList);
                        }
                );
    }

    // Test estdc()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggEstdc_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats estdc(offset) AS estdc_offset", testFile, ds -> {
            Assertions.assertEquals("[estdc_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("estdc_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11"), destAsList);
        });
    }

    // Test estdc_error()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggEstdc_error_Test() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats estdc_error(offset) AS estdc_error_offset", testFile, ds -> {
                    Assertions.assertEquals("[estdc_error_offset]", Arrays.toString(ds.columns()));

                    List<String> destAsList = ds
                            .select("estdc_error_offset")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Collections.singletonList("0.0"), destAsList);
                });
    }

    // Test range()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggRange_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats range(offset) AS range_offset", testFile, ds -> {
            Assertions.assertEquals("[range_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("range_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("10"), destAsList);
        });
    }

    // Test count()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggCount_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats count(offset) AS count_offset", testFile, ds -> {
            Assertions.assertEquals("[count_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("count_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("12"), destAsList);
        });
    }

    // Test avg()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggAvg_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats avg(offset)", testFile, ds -> {
            Assertions.assertEquals("[avg(offset)]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("avg(offset)")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("6.416666666666667"), destAsList);
        });
    }

    // Test mean()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggMean_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats mean(offset)", testFile, ds -> {
            Assertions.assertEquals("[mean(offset)]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("mean(offset)")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("6.416666666666667"), destAsList);
        });
    }

    // Test var()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggVar_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats var(offset) AS var_offset", testFile, ds -> {
            Assertions.assertEquals("[var_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("var_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("12.083333333333332"), destAsList);
        });
    }

    // Test varp()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggVarp_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats varp(offset) AS varp_offset", testFile, ds -> {
            Assertions.assertEquals("[varp_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("varp_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11.076388888888888"), destAsList);
        });
    }

    // Test multiple aggregations at once
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_Agg_MultipleTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | stats var(offset) AS var_offset avg(offset) AS avg_offset", testFile, ds -> {
                            Assertions.assertEquals("[var_offset, avg_offset]", Arrays.toString(ds.columns()));
                        }
                );
    }

    // Test BY field,field
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_Agg_ByTest() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats avg(offset) AS avg_offset BY sourcetype,host", testFile, ds -> {
                    Assertions.assertEquals("[sourcetype, host, avg_offset]", Arrays.toString(ds.columns()));
                });
    }

    // Test first()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggFirst_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats first(offset) AS first_offset", testFile, ds -> {
            Assertions.assertEquals("[first_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("first_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("1"), destAsList);
        });
    }

    // Test last()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggLast_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats last(offset) AS last_offset", testFile, ds -> {
            Assertions.assertEquals("[last_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("last_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11"), destAsList);
        });
    }

    // Test latest()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggLatest_Test() {
        streamingTestUtil.performDPLTest("index=index_A | stats latest(offset) AS latest_offset", testFile, ds -> {
            Assertions.assertEquals("[latest_offset]", Arrays.toString(ds.columns()));

            List<String> destAsList = ds
                    .select("latest_offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList("11"), destAsList);
        });
    }

    // Test latest_time()
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void statsTransform_AggLatestTime_Test() {
        streamingTestUtil
                .performDPLTest("index=index_A | stats latest_time(offset) AS latest_time_offset", testFile, ds -> {
                    Assertions.assertEquals("[latest_time_offset]", Arrays.toString(ds.columns()));

                    List<String> destAsList = ds
                            .select("latest_time_offset")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());
                    Assertions.assertEquals(Collections.singletonList("1286709610"), destAsList);
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testQueryWithoutDatasource() {
        String query = "| stats count";

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("count")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, res.count()); // 1 row of data
            Assertions.assertEquals("0", listOfRaw.get(0));
        });
    }
}
