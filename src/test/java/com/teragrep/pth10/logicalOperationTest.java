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

import com.teragrep.pth10.ast.DefaultTimeFormat;
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
    String testFile = "src/test/resources/xmlWalkerTestData*.json"; // * to make the path into a directory path
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

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseDPLTest() throws AnalysisException {
        String q = "index=kafka_topic conn error eka OR toka kolmas";
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%conn%' AND _raw LIKE '%error%' AND _raw LIKE '%eka%' OR _raw LIKE '%toka%' AND _raw LIKE '%kolmas%'";
        String result = utils.getQueryAnalysis(q);
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseDPLCatalystTest() {
        String q = "index=kafka_topic *conn* *error* *eka* OR *toka* *kolmas*";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "(RLIKE(index, (?i)^kafka_topic$) AND (((RLIKE(_raw, (?i)^.*\\Qconn\\E.*) AND RLIKE(_raw, (?i)^.*\\Qerror\\E.*)) AND (RLIKE(_raw, (?i)^.*\\Qeka\\E.*) OR RLIKE(_raw, (?i)^.*\\Qtoka\\E.*))) AND RLIKE(_raw, (?i)^.*\\Qkolmas\\E.*)))";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);
        });
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseOrTest() throws AnalysisException {
        String q = "index=kafka_topic a1 OR a2";
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%a1%' OR _raw LIKE '%a2%'";
        String result = utils.getQueryAnalysis(q);
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseOrCatalystTest() {
        String q = "index=kafka_topic a1 OR a2";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "(RLIKE(index, (?i)^kafka_topic$) AND (RLIKE(_raw, (?i)^.*\\Qa1\\E.*) OR RLIKE(_raw, (?i)^.*\\Qa2\\E.*)))";

            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);
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
    public void parseIndexInCatalystTest() {
        String q = "index IN ( index_A index_B )";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "(RLIKE(index, (?i)^index_a) OR RLIKE(index, (?i)^index_b))";

            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);
        });
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseAndTest() throws AnalysisException {
        String q = "index=kafka_topic a1 AND a2";
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%a1%' AND _raw LIKE '%a2%'";
        String result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseAndCatalystTest() {
        String q = "index=kafka_topic a1 AND a2";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String e = "(RLIKE(index, (?i)^kafka_topic$) AND (RLIKE(_raw, (?i)^.*\\Qa1\\E.*) AND RLIKE(_raw, (?i)^.*\\Qa2\\E.*)))";

            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);
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
            String e = "(RLIKE(index, (?i)^abc$) AND (RLIKE(sourcetype, (?i)^cd:ef:gh:0) AND RLIKE(_raw, (?i)^.*\\Q1848c85bfe2c4323955dd5469f18baf6\\E.*)))";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);

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
            String e = "(RLIKE(index, (?i)^abc$) AND RLIKE(_raw, (?i)^.*\\Q\"latitude\": -89.875, \"longitude\": 24.125\\E.*))";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);

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

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseAnd1Test() throws AnalysisException {
        String q, e, result;
        q = "index=kafka_topic a1 a2";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%a1%' AND _raw LIKE '%a2%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseMultipleParenthesisTest() throws AnalysisException {
        String q = "index=kafka_topic conn ( ( error AND toka) OR kolmas )";
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%conn%' AND ((_raw LIKE '%error%' AND _raw LIKE '%toka%') OR _raw LIKE '%kolmas%')";
        String result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseParenthesisWithOrTest() throws AnalysisException {
        String q, e, result;
        q = "index=kafka_topic conn AND ( ( error AND toka ) OR ( kolmas AND n4 ))";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _raw LIKE '%conn%' AND ((_raw LIKE '%error%' AND _raw LIKE '%toka%') OR (_raw LIKE '%kolmas%' AND _raw LIKE '%n4%'))";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseSimpleParenthesisTest() throws AnalysisException {
        String q, e, result;
        q = "index=kafka_topic ( conn )";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND (_raw LIKE '%conn%')";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseHostTest() throws AnalysisException {
        String q = "index = archive_memory host = \"localhost\" Deny";
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND _raw LIKE '%Deny%'";
        String result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseHost1Test() throws AnalysisException {
        String q, e, result;
        q = "index = archive_memory ( host = \"localhost\" OR host = \"test\" ) AND sourcetype = \"memory\" Deny";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND (host LIKE \"localhost\" OR host LIKE \"test\") AND sourcetype LIKE \"memory\" AND _raw LIKE '%Deny%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseHost2Test() throws AnalysisException {
        String q, e, result;
        q = "index = archive_memory host = \"localhost\" host = \"test\" host = \"test1\" Deny";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND host LIKE \"test\" AND host LIKE \"test1\" AND _raw LIKE '%Deny%'";
        result = utils.getQueryAnalysis(q);
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseHost3Test() throws AnalysisException {
        String q, e, result;
        // missing AND in query
        q = "index = archive_memory host = \"localhost\" host = \"test\" Deny";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND host LIKE \"test\" AND _raw LIKE '%Deny%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseHost4Test() throws AnalysisException {
        String q, e, result;
        q = "index = archive_memory host = \"localhost\" host = \"test\" host = \"test1\" Deny";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"localhost\" AND host LIKE \"test\" AND host LIKE \"test1\" AND _raw LIKE '%Deny%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseHost5Test() throws AnalysisException {
        String q, e, result;
        // Same but missing AND in query		
        q = "index = archive_memory host = \"one\" host = \"two\" host = \"tree\" number";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"archive_memory\" AND host LIKE \"one\" AND host LIKE \"two\" AND host LIKE \"tree\" AND _raw LIKE '%number%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void streamListWithoutQuotesTest() throws AnalysisException {
        String q, e, result;
        q = "index = memory-test latest=\"05/10/2022:09:11:40\" host= sc-99-99-14-25 sourcetype= log:f17:0 Latitude";
        long latestEpoch = new DefaultTimeFormat().getEpoch("05/10/2022:09:11:40");
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory-test\" AND _time <= from_unixtime("
                + latestEpoch
                + ") AND host LIKE \"sc-99-99-14-25\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void streamListWithoutQuotes1Test() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index = MEMORY-test latest=\"05/10/2022:09:11:40\" host= SC-99-99-14-20 sourcetype= LOG:F17:0 Latitude";
        long latestEpoch2 = new DefaultTimeFormat().getEpoch("05/10/2022:09:11:40");
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory-test\" AND _time <= from_unixtime("
                + latestEpoch2
                + ") AND host LIKE \"sc-99-99-14-20\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalNotTest() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=f17 sourcetype=log:f17:0 _index_earliest=\"12/31/1970:10:15:30\" _index_latest=\"12/31/2022:10:15:30\" NOT rainfall_rate";
        long earliestEpoch = new DefaultTimeFormat().getEpoch("12/31/1970:10:15:30");
        long latestEpoch = new DefaultTimeFormat().getEpoch("12/31/2022:10:15:30");
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND sourcetype LIKE \"log:f17:0\" AND _time >= from_unixtime("
                + earliestEpoch + ") AND _time <= from_unixtime(" + latestEpoch
                + ") AND NOT _raw LIKE '%rainfall_rate%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalNot1Test() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=cpu sourcetype=log:cpu:0 NOT src";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cpu\" AND sourcetype LIKE \"log:cpu:0\" AND NOT _raw LIKE '%src%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalQuotedCompoundTest() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=f17 \"ei yhdys sana\"";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%ei yhdys sana%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalUnQuotedCompoundTest() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=f17 ei yhdys sana";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%ei%' AND _raw LIKE '%yhdys%' AND _raw LIKE '%sana%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalUnQuotedCompound2Test() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=f17 ei AND yhdys sana";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%ei%' AND _raw LIKE '%yhdys%' AND _raw LIKE '%sana%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalQuotedIntTest() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=f17 \"1.2\"";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%1.2%'";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalUnQuotedIntTest() throws AnalysisException {
        String q, e, result;
        // Test to_lower() for inex,host,sourcetyype
        q = "index=f17 1.2";
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"f17\" AND _raw LIKE '%1.2%'";
        result = utils.getQueryAnalysis(q);
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseLikeWithParenthesisCatalystTest() {
        String q = "index=access_log earliest=\"01/21/2022:10:00:00\" latest=\"01/21/2022:11:59:59\" \"*(3)www(7)example(3)com(0)*\" OR \"*(4)mail(7)example(3)com(0)*\"";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            String e = "(RLIKE(index, (?i)^access_log$) AND (((_time >= from_unixtime(1642752000, yyyy-MM-dd HH:mm:ss)) AND (_time < from_unixtime(1642759199, yyyy-MM-dd HH:mm:ss))) AND (RLIKE(_raw, (?i)^.*\\Q(3)www(7)example(3)com(0)\\E.*) OR RLIKE(_raw, (?i)^.*\\Q(4)mail(7)example(3)com(0)\\E.*))))";

            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            assertEquals(e, result);
        });
    }
}
