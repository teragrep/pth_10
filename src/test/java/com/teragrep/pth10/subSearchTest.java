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

import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class subSearchTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(subSearchTest.class);

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

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSubSearchLimitOne() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=1 host | fields + host]";
        String testFile = "src/test/resources/subsearchData*.jsonl";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            String e = "RLIKE(index, (?i)^index_A$)";
            // Check that sub-query get executed and result is used as query parameter
            Assertions.assertEquals(e, this.streamingTestUtil.getCtx().getSparkQuery());

            // Should have all the columns, fields command in subsearch shouldn't affect the main search
            Assertions.assertEquals(9, res.columns().length);

            // Check that the first (and only) value is correct
            Assertions.assertEquals("computer01.example.com", res.select("host").collectAsList().get(0).getString(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSubSearchLimitGTOne() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host]";
        String testFile = "src/test/resources/subsearchData*.jsonl"; // * to make the path into a directory path

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            String e = "RLIKE(index, (?i)^index_A$)";

            // Check that sub-query get executed and result is used as query parameter
            Assertions.assertEquals(e, this.streamingTestUtil.getCtx().getSparkQuery());

            // Should have all the columns, fields command in subsearch shouldn't affect the main search
            Assertions.assertEquals(9, res.columns().length);

            List<String> lst = res
                    .select("host")
                    .distinct()
                    .orderBy("host")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Assertions.assertEquals(2, lst.size());

            // check that the rows have correct values
            Assertions.assertEquals("computer01.example.com", lst.get(0));
            Assertions.assertEquals("computer02.example.com", lst.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSubSearchAndAggr() {
        String q = "index = index_A [ search sourcetype= A:X:0 | top limit=3 host | fields + host] | stats count by host";
        String testFile = "src/test/resources/subsearchData*.jsonl"; // * to make the path into a directory path

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            List<Row> host = res.select("host").collectAsList();
            List<Row> count = res.select("count").collectAsList();
            String[] expectedHost = new String[] {
                    "computer01.example.com", "computer02.example.com"
            };
            String[] expectedCount = new String[] {
                    "2", "4"
            };

            Assertions.assertEquals(2, host.size());
            Assertions.assertEquals(2, count.size());
            Assertions.assertArrayEquals(expectedHost, host.stream().map(r -> r.getAs(0).toString()).toArray());
            Assertions.assertArrayEquals(expectedCount, count.stream().map(r -> r.getAs(0).toString()).toArray());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testMultipleSubSearch() {
        String q = "index = index_A [ search sourcetype=*X:0 | top limit=10 host | fields + host] [ search sourcetype=b:X:0 | top limit=5 host | fields + host]";
        String testFile = "src/test/resources/subsearchData*.jsonl"; // * to make the path into a directory path

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            List<String> hostList = res
                    .select("host")
                    .distinct()
                    .orderBy("host")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .collect(Collectors.toList());

            Assertions.assertEquals(1, hostList.size());
            Assertions.assertEquals("computer02.example.com", hostList.get(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSearchWithTopLimit2AndFieldsTransformation() {
        String q = "sourcetype=A:X:0| top limit=2 host | fields + host";
        String testFile = "src/test/resources/xmlWalkerTestDataStreaming";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            String head = res.head().getString(0);
            List<Row> lst = res.collectAsList();
            // Correct  item count
            Assertions.assertEquals(2, lst.size());
            Assertions.assertEquals("computer01.example.com", lst.get(0).getString(0));
            Assertions.assertEquals("computer02.example.com", lst.get(1).getString(0));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSearchWithNoAggs() {
        String q = "index = index_A AND computer01.example.com AND computer02.example.com";
        String testFile = "src/test/resources/subsearchData*.jsonl"; // * to make the path into a directory path

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
            Assertions.assertFalse(aggregates);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testSearchWithTopLimit1AndFieldsTransformation() {
        String q = "sourcetype=c:X:0| top limit=1 host | fields + host";
        String testFile = "src/test/resources/subsearchData*.jsonl"; // * to make the path into a directory path

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            List<Row> lst = res.collectAsList();
            lst.forEach(item -> {
                LOGGER.info("item value={}", item.getString(0));
            });
            // Correct  item count
            Assertions.assertEquals(1, lst.size());
            Assertions.assertEquals("computer03.example.com", lst.get(0).getString(0));
            boolean aggregates = this.streamingTestUtil.getCatalystVisitor().getAggregatesUsed();
            Assertions.assertFalse(aggregates);
        });
    }

}
