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

import com.teragrep.pth10.ast.DPLTimeFormat;
import com.teragrep.pth10.ast.DefaultTimeFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class whereTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(whereTest.class);

    // proper tests -v ----------------------------------------

    private final String testFile = "src/test/resources/regexTransformationTest_data*.json"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });

    private StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
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

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseWhereTest() throws Exception {
        String q, e, result, uuid;
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as count by _time | where count > 70";
        long indexEarliestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS count FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch + ") GROUP BY _time ) WHERE count > 70";
        result = utils.getQueryAnalysis(q);
        /*
        LOGGER.info("SQL ="+result);
        LOGGER.info("EXP ="+e);
         */
        Assertions.assertEquals(e, result, q);
    }

    /**
     * <!-- index = cinnamon _index_earliest="04/16/2020:10:25:40" | chart count(_raw) as count by _time | where count >
     * 70 --> <root> <search root=true> <logicalStatement> <AND> <index operation="EQUALS" value="cinnamon" />
     * <index_earliest operation="GE" value="1587021940" /> </AND> <transformStatements> <transform>
     * <divideBy field="_time"> <chart field="_raw" fieldRename="count" function="count" /> <transform> <where>
     * <evalCompareStatement field="count" operation="GT" value="70" /> </where> </transform> </chart> </divideBy>
     * </transform> </transformStatements> </logicalStatement> </search> </root> --------------------------- <root>
     * <transformStatements> <where> <evalCompareStatement field="count" operation="GT" value="70" />
     * <transformStatement> <divideBy field="_time"> <chart field="_raw" fieldRename="count" function="count">
     * <transformStatement> <search root="true"> <logicalStatement> <AND> <index operation="EQUALS" value="cinnamon" />
     * <index_earliest operation="GE" value="1587021940" /> </AND> </logicalStatement> </search> </transformStatement>
     * </chart> </divideBy> </transformStatement> </where> </transformStatements> </root> scala-sample create dataframe
     * (Result of search-transform when root=true) val df = spark.readStream.load().option("query","<AND><index
     * operation=\"EQUALS\" value=\"cinnamon\" /><index_earliest operation=\"GE\" value=\"1587021940\" /></AND>")
     * process that ( processing resulting dataframe) val resultingDataSet =
     * df.groupBy(col("`_time`")).agg(functions.count(col("`_raw`")).as("`count`")).where(col("`_raw`").gt(70)); Same
     * using single spark.readStream.load().option("query","<AND><index operation=\"EQUALS\" value=\"cinnamon\"
     * /><index_earliest operation=\"GE\" value=\"1587021940\"
     * /></AND>").groupBy(col("`_time`")).agg(functions.count(col("`_raw`")).as("`count`")).where(col("`_raw`").gt(70));
     * when using treewalker, add "`"-around column names count -> `count`
     */

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalOrWhereTest() throws Exception {
        String q, e, result;
        // test where-clause with logical operation OR
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 OR cnt < 75";
        long indexEarliestEpoch5 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch5 + ") GROUP BY _time ) WHERE cnt > 70 OR cnt < 75";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalAndWhereTest() throws Exception {
        String q, e, result;
        // test where-clause with logical operation AND
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 AND cnt < 75";
        long indexEarliestEpoch4 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch4 + ") GROUP BY _time ) WHERE cnt > 70 AND cnt < 75";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalAndOrWhereTest() throws Exception {
        String q, e, result;
        // test where-clause with logical operation AND, OR
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 AND cnt < 75 OR cnt != 72";
        long indexEarliestEpoch6 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch6 + ") GROUP BY _time ) WHERE cnt > 70 AND cnt < 75 OR cnt != 72";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void chainedTransformWhereTest() throws Exception {
        String q, e, result;
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where  cnt > 70 | where cnt < 75";
        long indexEarliestEpoch2 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch2 + ") GROUP BY _time ) WHERE cnt > 70 ) WHERE cnt < 75";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void logicalWhereTest() throws Exception {
        String q, e, result;
        // test where-clause with logical operation AND, OR with parents
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 )";
        long indexEarliestEpoch8 = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch8
                + ") GROUP BY _time ) WHERE ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 )";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void complexWhereTest() throws Exception {
        String q, e, result;
        // test where-clause with logical operation AND, OR with parents and several
        // logical operation
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt !=35 OR cnt = 65)";
        long indexEarliestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS cnt FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch
                + ") GROUP BY _time ) WHERE ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt != 35 OR cnt = 65 )";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void complexWhereXmlTest() throws Exception {
        String q, e, result;
        // test where-clause with logical operation AND, OR with parents and several
        // logical operation
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt !=35 OR cnt = 65)";
        long indexEarliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "<root><!--index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) as cnt by _time | where ( cnt > 70 AND cnt < 75 ) OR ( cnt > 30 AND cnt < 40 AND cnt !=35 OR cnt = 65)--><search root=\"true\"><logicalStatement><AND><index operation=\"EQUALS\" value=\"cinnamon\"/><index_earliest operation=\"GE\" value=\""
                + indexEarliestEpoch
                + "\"/></AND><transformStatements><transform><divideBy field=\"_time\"><chart field=\"_raw\" fieldRename=\"cnt\" function=\"count\" type=\"aggregate\"><transform><where><OR><AND><evalCompareStatement field=\"cnt\" operation=\"GT\" value=\"70\"/><evalCompareStatement field=\"cnt\" operation=\"LT\" value=\"75\"/></AND><OR><AND><AND><evalCompareStatement field=\"cnt\" operation=\"GT\" value=\"30\"/><evalCompareStatement field=\"cnt\" operation=\"LT\" value=\"40\"/></AND><evalCompareStatement field=\"cnt\" operation=\"NOT_EQUALS\" value=\"35\"/></AND><evalCompareStatement field=\"cnt\" operation=\"EQUALS\" value=\"65\"/></OR></OR></where></transform></chart></divideBy></transform></transformStatements></logicalStatement></search></root>";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void symbolTableTest() throws Exception {
        String q, e, result;
        // test symbol-table, count(raw)->replaced with generated row in form
        // __count_UUID
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) by _time | where  \"count(_raw)\" > 70 | where \"count(_raw)\" < 75";
        long indexEarliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT * FROM ( SELECT _time,count(_raw) AS __count_UUID FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch + ") GROUP BY _time ) WHERE __count_UUID > 70 ) WHERE __count_UUID < 75";
        result = utils.getQueryAnalysis(q);
        // find generated fieldname from result and check that it is like __count_UUID
        String r[] = result.split("__count");
        String uuid = r[1].substring(1, 37);
        if (utils.isUUID(uuid)) {
            // Was generated row-name so accept that as expected one
            e = e.replace("__count_UUID", "__count_" + uuid);
        }
        Assertions.assertEquals(e, result, q);
    }

    @Disabled
    @Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void defaultCountWithLogicalOperationsTest() throws Exception {
        String q, e, result, uuid;
        // test where-clause with logical operation AND, OR and testing symbol-table
        q = "index = cinnamon _index_earliest=\"04/16/2020:10:25:40\" | chart count(_raw) by _time | where 'count(_raw)' > 71 AND 'count(_raw)' < 75 OR 'count(_raw)' != 72";
        long indexEarliestEpoch = new DPLTimeFormat("MM/dd/yyyy:HH:mm:ss").getEpoch("04/16/2020:10:25:40");
        e = "SELECT * FROM ( SELECT _time,count(_raw) AS `count(_raw)` FROM `temporaryDPLView` WHERE index LIKE \"cinnamon\" AND _time >= from_unixtime("
                + indexEarliestEpoch
                + ") GROUP BY _time ) WHERE 'count(_raw)' > 71 AND 'count(_raw)' < 75 OR 'count(_raw)' != 72";
        result = utils.getQueryAnalysis(q);
        Assertions.assertEquals(e, result, q);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void whereTestIntegerColumnLessThan() {
        streamingTestUtil.performDPLTest("index=index_A | where offset < 3", testFile, ds -> {
            Assertions
                    .assertEquals(
                            "[_time, id, _raw, index, sourcetype, host, source, partition, offset]", Arrays
                                    .toString(ds.columns()),
                            "Batch handler dataset contained an unexpected column arrangement !"
                    );

            Assertions.assertEquals(2, ds.collectAsList().size());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void whereTestIntegerColumnLessThanAfterChart() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A " + "| chart avg(offset) as aoffset" + "| chart values(aoffset) as voffset"
                                + "| chart sum(voffset) as soffset" + "| where soffset > 3",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals("[soffset]", Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");

                            Assertions.assertEquals(1, ds.collectAsList().size());
                        }
                );
    }
}
