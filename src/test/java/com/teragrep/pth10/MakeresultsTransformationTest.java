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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.util.List;

/**
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MakeresultsTransformationTest {

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

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void makeresults_BasicQuery_Test() {
        this.streamingTestUtil.performDPLTest("| makeresults", "", ds -> {
            Assertions.assertEquals(new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build())
            }), ds.schema());
            Assertions.assertEquals(1, ds.count());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void makeresults_Annotate_Test() {
        this.streamingTestUtil.performDPLTest("| makeresults annotate=true", "", ds -> {
            Assertions.assertEquals(new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("struck_server", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("struck_server_group", DataTypes.StringType, true, new MetadataBuilder().build())
            }), ds.schema());
            Assertions.assertEquals(1, ds.count());

            // get all rows except '_time'
            List<Row> rows = ds.drop("_time").collectAsList();
            Assertions.assertEquals(1, rows.size());
            // assert all of them to be null
            rows.forEach(row -> {
                Assertions.assertEquals(6, row.length());
                for (int i = 0; i < row.length(); i++) {
                    Assertions.assertEquals(this.streamingTestUtil.getCtx().nullValue.value(), row.get(i));
                }
            });
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void makeresults_Count100_Test() {
        this.streamingTestUtil.performDPLTest("| makeresults count=100", "", ds -> {
            Assertions.assertEquals(new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            }), ds.schema());
            Assertions.assertEquals(100, ds.count());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void makeresults_WithEval_Test() {
        this.streamingTestUtil.performDPLTest("| makeresults | eval a = 1", "", ds -> {
            Assertions.assertEquals(new StructType(new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, true, new MetadataBuilder().build()),
                    new StructField("a", DataTypes.IntegerType, false, new MetadataBuilder().build())
            }), ds.schema());
            Assertions.assertEquals(1, ds.count());
        });
    }
}
