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

import org.apache.spark.sql.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class logicalOperationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(logicalOperationTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/logicalOperationTestData*.json"; // * to make the path into a directory path
    private StreamingTestUtil streamingTestUtil;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testUnquotedSearchString() {
        String query = "index = index_A raw 01";

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(1, res.count()); // 1 row of data
            assertEquals("\"raw 01\"", listOfRaw.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testQuotedSearchString() {
        String query = "index=index_C \"raw 10\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(1, res.count());
            assertEquals("\"raw 10\"", listOfRaw.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWildcardsWithOr() {
        String query = "index=index_A (*raw* *01*) OR (*raw* *02*)";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count()); // 2 row of data
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 02\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSearchStringWithOr() {
        String query = "index=index_A 01 OR 02";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count()); // 2 row of data
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 02\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void wildcardBloomCheckTest() {
        String q = "index=xyz ab*";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            assertTrue(this.streamingTestUtil.getCtx().isWildcardSearchUsed());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void wildcardBloomCheckTest2() {
        String q = "index=xyz ab";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            assertFalse(this.streamingTestUtil.getCtx().isWildcardSearchUsed());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMultipleIndexWithIn() {
        String query = "index IN ( index_A index_B )";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfIndex = res
                    .select("index")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(7, res.count()); // 7 row of data
            assertEquals("index_A", listOfIndex.get(0));
            assertEquals("index_A", listOfIndex.get(1));
            assertEquals("index_A", listOfIndex.get(2));
            assertEquals("index_B", listOfIndex.get(3));
            assertEquals("index_B", listOfIndex.get(4));
            assertEquals("index_B", listOfIndex.get(5));
            assertEquals("index_B", listOfIndex.get(6));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMultipleIndexWithComma() {
        String query = "index IN (index_A,index_B,index_C)";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfIndex = res
                    .select("index")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(10, res.count()); // 10 row of data
            assertEquals("index_A", listOfIndex.get(0));
            assertEquals("index_A", listOfIndex.get(1));
            assertEquals("index_A", listOfIndex.get(2));
            assertEquals("index_B", listOfIndex.get(3));
            assertEquals("index_B", listOfIndex.get(4));
            assertEquals("index_B", listOfIndex.get(5));
            assertEquals("index_B", listOfIndex.get(6));
            assertEquals("index_C", listOfIndex.get(7));
            assertEquals("index_C", listOfIndex.get(8));
            assertEquals("index_C", listOfIndex.get(9));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSearchStringWithAnd() {
        String query = "index=index_B raw AND sourcetype = B:Y:0";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count()); // 2 rows of data
            assertEquals("\"raw 04\"", listOfRaw.get(0));
            assertEquals("\"raw 05\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseRawUUIDCatalystTest() {
        String q = "index=abc sourcetype=\"cd:ef:gh:0\"  \"1848c85bfe2c4323955dd5469f18baf6\"";
        String testFile = "src/test/resources/uuidTestData*.json"; // * to make the path into a directory path
        this.streamingTestUtil.performDPLTest(q, testFile, res -> {

            // Get raw field and check results. Should be only 1 match
            Dataset<Row> selected = res.select("_raw");
            //selected.show(false);
            List<String> lst = selected
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .sorted()
                    .collect(Collectors.toList());
            // check result count
            assertEquals(3, lst.size());
            // Compare values
            assertEquals("uuid=1848c85bfe2c4323955dd5469f18baf6  computer01.example.com", lst.get(1));
            assertEquals("uuid=1848c85bfe2c4323955dd5469f18baf6666  computer01.example.com", lst.get(2));
            assertEquals("uuid=*!<1848c85bFE2c4323955dd5469f18baf6<  computer01.example.com", lst.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseWithQuotesInsideQuotesCatalystTest() {
        String q = "index=abc \"\\\"latitude\\\": -89.875, \\\"longitude\\\": 24.125\"";
        String testFile = "src/test/resources/latitudeTestData*.json"; // * to make the path into a directory path

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            // Get raw field and check results. Should be only 1 match
            Dataset<Row> selected = res.select("_raw");
            //selected.show(false);
            List<Row> lst = selected.collectAsList();
            // check result count
            assertEquals(2, lst.size());
            // Compare values
            assertEquals("\"latitude\": -89.875, \"longitude\": 24.125", lst.get(0).getString(0));
            assertEquals("\"latitude\": -89.875, \"longitude\": 24.125", lst.get(1).getString(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSearchWithParenthesis() {
        String query = "index=index_A (raw 01)";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(1, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testSearchWithMultipleParenthesis() {
        String query = "index=index_A raw ((raw AND 01) OR 02)";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count()); // 2 rows of data
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 02\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithHost() {
        String query = "index = index_A host = computer01.example.com";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(1, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithHostInQuotes() {
        String query = "index = index_B host = \"computer*\" 04";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(1, res.count());
            assertEquals("\"raw 04\"", listOfRaw.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithMultipleHostsAndSourcetype() {
        String query = "index = index_B (host = computer04.example.com OR host = computer05.example.com OR host = computer06.example.com) AND sourcetype = B:Y:0";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count());
            assertEquals("\"raw 04\"", listOfRaw.get(0));
            assertEquals("\"raw 05\"", listOfRaw.get(1));
        });
    }

    @Test
    @Disabled(
            value = "search does not allow having sourcetype in this format: sourcetype IN (sourcetyp1, sourcetype2), pth-10 issue #282"
    )
    public void testWithMultipleSourcetypes() {
        String query = "\"index=index_* sourcetype IN (B:X:0, B:Y:0, C:X:0)\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfSourcetype = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(5, res.count());
            assertEquals("B:X:0", listOfSourcetype.get(0));
            assertEquals("B:X:0", listOfSourcetype.get(1));
            assertEquals("B:Y:0", listOfSourcetype.get(2));
            assertEquals("B:Y:0", listOfSourcetype.get(3));
            assertEquals("C:X:0", listOfSourcetype.get(4));
        });

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWildcardWithIndexHostSourcetype() {
        String query = "index = index_* host=computer* sourcetype = *X* ";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(4, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 06\"", listOfRaw.get(1));
            assertEquals("\"raw 07\"", listOfRaw.get(2));
            assertEquals("\"raw 08\"", listOfRaw.get(3));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithLatest() {
        String query = "index = index_A latest=\"2003-03-03T03:03:03.030+03:00\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 02\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithLatestEarliest() {
        String query = "index = index_A earliest=\"2001-01-01T00:00:00.000+03:00\" latest=\"2004-04-04T00:00:00.000+03:00\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(3, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 02\"", listOfRaw.get(1));
            assertEquals("\"raw 03\"", listOfRaw.get(2));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithIndexEarliestLatest() {
        String query = "index=index_A _index_earliest=\"2001-01-01T00:00:00.000+03:00\" _index_latest=\"2003-03-03T03:03:03.030+03:00\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
            assertEquals("\"raw 02\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithNOT() {
        String query = "index=index_B sourcetype=B:* NOT sourcetype=B:X:0";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count());
            assertEquals("\"raw 04\"", listOfRaw.get(0));
            assertEquals("\"raw 05\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testQuotedIntSearch() {
        String query = "index=index_A \"01\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(1, res.count());
            assertEquals("\"raw 01\"", listOfRaw.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testWithParenthesisAndWilcard() {
        String q = "index=index_B source=\"\\(*:computer04.*(com)*\\)\" OR source=\"\\(*7.*(com)*\\)\"";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(2, res.count());
            assertEquals("\"raw 04\"", listOfRaw.get(0));
            assertEquals("\"raw 07\"", listOfRaw.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testMultipleIndexSourcetypeWithOr() {
        String query = "(index=index_* sourcetype=B:*:0 raw 04) OR (index=index_* sourcetype!=*Y* NOT \"raw 01\" NOT \"raw 03\") earliest=\"2001-01-01T00:00:00.000+03:00\" latest=\"2011-11-11T00:00:00.000+03:00\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> listOfRaw = res
                    .select("_raw")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            assertEquals(4, res.count());
            assertEquals("\"raw 04\"", listOfRaw.get(0));
            assertEquals("\"raw 06\"", listOfRaw.get(1));
            assertEquals("\"raw 07\"", listOfRaw.get(2));
            assertEquals("\"raw 08\"", listOfRaw.get(3));
        });
    }
}
