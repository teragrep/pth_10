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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class fieldTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(fieldTransformationTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestDataStreaming";
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
    void testFieldsTransformKeepOne() {
        String q = "index=index_B | fields _time";
        this.streamingTestUtil.performDPLTest(q, this.testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build())
            });
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

            Assertions.assertEquals(5, dsAsList.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                Assertions.assertEquals(expectedValues.get(i), dsAsList.get(i));
            }

            Assertions.assertEquals(expectedSchema, ds.schema());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testFieldsTransformKeepMultiple() {
        String q = "index=index_B | fields _time host";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, res.count());
            Assertions.assertEquals(expectedSchema, res.schema());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testFieldsTransformDropOne() {
        this.streamingTestUtil.performDPLTest("index=index_B | fields - host", this.testFile, res -> {
            // check that we drop only host-column
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, res.count());
            Assertions.assertEquals(expectedSchema, res.schema());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testFieldsTransformDropSeveral() {
        this.streamingTestUtil.performDPLTest("index=index_B | fields - host index partition", this.testFile, res -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("_time", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, res.count());
            Assertions.assertEquals(expectedSchema, res.schema());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testFieldsWithPlus() {
        String query = "index = index_B | fields + offset";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema()); //check schema is correct
        });

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testFieldsWithPlusMultiple() {
        String query = "index = index_B | fields + offset, source, host";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            final StructType expectedSchema = new StructType(new StructField[] {
                    new StructField("offset", DataTypes.LongType, true, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, true, new MetadataBuilder().build())
            });
            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals(expectedSchema, ds.schema()); //check schema is correct
        });

    }

    @Test
    @Disabled(value = "wildcard functionality not implemented, pth-10 issue #275")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void testFieldsWithWildcard() {
        String query = "index = index_B | fields - _*"; // remove internal fields
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            Assertions.assertEquals(5, ds.count());
            Assertions
                    .assertEquals("[index, sourcetype, source, host, partition, offset]", Arrays.toString(ds.columns())); //check schema is correct
        });

    }
}
