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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AddtotalsTransformationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddtotalsTransformationTest.class);
    private final String testFile = "src/test/resources/numberData_0*.json"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
            }
    );

    private StreamingTestUtil streamingTestUtil;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
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
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void addtotals_noparams_test() {
        streamingTestUtil.performDPLTest("index=* | addtotals ", testFile, ds -> {
            List<String> res = ds.select("Total").collectAsList().stream().map(r->r.getAs(0).toString()).sorted().collect(Collectors.toList());
            List<String> expected = Arrays.asList("36.0", "11.0", "1.0", "-9.0", "48.2");
            assertEquals(5, res.size());
            assertEquals(5, expected.size());

            for (String r : res) {
                if (!expected.contains(r)) {
                    fail("Value <" + r + "> was not one of the expected values!");
                }
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void addtotals_colParam_test() {
        streamingTestUtil.performDPLTest("index=* | addtotals col=true", testFile, ds -> {
            List<Double> res = ds.select("_raw").collectAsList().stream().map(r->Double.parseDouble(r.getAs(0).toString())).sorted(Double::compareTo).collect(Collectors.toList());
            List<Double> expected = Arrays.asList(-10d,0d,10d,35d,47.2d,82.2d);
            assertEquals(6, res.size());
            assertEquals(6, expected.size());

            for (Double r : res) {
                if (!expected.contains(r)) {
                    fail("Value <" + r + "> was not one of the expected values!");
                }
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    void addtotals_fieldNames_test() {
        streamingTestUtil.performDPLTest(
            "index=* | addtotals col=true row=true labelfield=x1 fieldname=x2",
            testFile,
            ds -> {
                List<String> fieldsInData = Arrays.asList(ds.schema().fieldNames());
                // source schema + labelfield and fieldname
                assertEquals(testSchema.length() + 2, fieldsInData.size());
                // check that fieldname and labelfield are present in schema
                assertTrue(fieldsInData.contains("x1"));
                assertTrue(fieldsInData.contains("x2"));
                // 5 source rows plus last row for column sums
                assertEquals(6, ds.count());
            }
        );
    }
}
