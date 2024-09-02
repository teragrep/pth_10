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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class fieldTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(fieldTransformationTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestDataStreaming";
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

    @Disabled(value = "Should be converted to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseFieldsTransformTest() {
        String q = "index=cinnamon | fields meta.*";
        String e = "SELECT meta.* FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" )";
        String result = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCatTest() {
        String q = "index=index_B | fields _time";
        this.streamingTestUtil.performDPLTest(q, this.testFile, ds -> {
            List<String> expectedValues = new ArrayList<>();
            expectedValues.add("2006-06-06T06:06:06.060+03:00");
            expectedValues.add("2007-07-07T07:07:07.070+03:00");
            expectedValues.add("2008-08-08T08:08:08.080+03:00");
            expectedValues.add("2009-09-09T09:09:09.090+03:00");
            expectedValues.add("2010-10-10T10:10:10.100+03:00");

            List<String> dsAsList = ds
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .sorted()
                    .collect(Collectors.toList());
            Collections.sort(expectedValues);

            assertEquals(5, dsAsList.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                assertEquals(expectedValues.get(i), dsAsList.get(i));
            }

            assertEquals("[_time: string]", ds.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCat2Test() {
        String q = "index=index_B | fields _time host";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            assertEquals(5, res.count());
            assertEquals("[_time: string, host: string]", res.toString());
        });
    }

    /*
      _raw, _time, host, index, offset, partition, source, sourcetype
     */
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCatDropTest() {
        this.streamingTestUtil.performDPLTest("index=index_B | fields - host", this.testFile, res -> {
            assertEquals(5, res.count());
            // check that we drop only host-column
            String schema = res.schema().toString();
            assertEquals(
                    "StructType(StructField(_raw,StringType,true),StructField(_time,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true))",
                    schema
            );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCatDropSeveralTest() {
        this.streamingTestUtil.performDPLTest("index=index_B | fields - host index partition", this.testFile, res -> {
            assertEquals(5, res.count());
            String schema = res.schema().toString();
            assertEquals(
                    "StructType(StructField(_raw,StringType,true),StructField(_time,StringType,true),StructField(id,LongType,true),StructField(offset,LongType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true))",
                    schema
            );
        });
    }

    @Disabled(value = "Should be converteed to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseFieldsTransform1Test() {
        String q, e, result;
        q = "index=cinnamon Denied | fields meta.*,_raw";
        e = "SELECT meta.*,_raw FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _raw LIKE '%Denied%' )";
        result = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Disabled(value = "Should be converteed to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseFieldsTransform2Test() {
        String q, e, result;

        q = "index=cinnamon Denied Port | fields meta.*,_raw";
        e = "SELECT meta.*,_raw FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _raw LIKE '%Denied%' AND _raw LIKE '%Port%' )";
        result = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Disabled(value = "Should be converteed to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseFieldsTransformAddTest() {
        String q, e, result;

        q = "index=cinnamon Denied Port | fields + meta.*,_raw";
        e = "SELECT meta.*,_raw FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _raw LIKE '%Denied%' AND _raw LIKE '%Port%' )";
        result = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        utils.printDebug(e, result);
        assertEquals(e, result);
    }

    @Disabled(value = "Should be converteed to a dataframe test")
    @Test // disabled on 2022-05-16 TODO convert to dataframe test
    public void parseFieldsTransformDropTest() {
        String q, e, result;
        q = "index=cinnamon Denied Port | fields - meta.*, _raw";
        e = "SELECT DROPFIELDS(meta.*,_raw) FROM ( SELECT * FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _raw LIKE '%Denied%' AND _raw LIKE '%Port%' )";
        result = Assertions.assertDoesNotThrow(() -> utils.getQueryAnalysis(q));
        utils.printDebug(e, result);
        assertEquals(e, result);
    }
}
