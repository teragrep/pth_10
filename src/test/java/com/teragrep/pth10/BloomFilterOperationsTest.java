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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BloomFilterOperationsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterOperationsTest.class);
    private final String testFile = "src/test/resources/xmlWalkerTestDataStreaming/bloomTeragrepStep_data*.json";

    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
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
        streamingTestUtil = new StreamingTestUtil(this.testSchema);
        streamingTestUtil.setEnv();
        /*
        Class.forName ("org.h2.Driver");
        this.conn = DriverManager.getConnection("jdbc:h2:~/test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE", "sa", "");
        org.h2.tools.RunScript.execute(conn, new FileReader("src/test/resources/bloomdb/bloomdb.sql"));
         */
    }

    @BeforeEach
    void setUp() {
        streamingTestUtil.setUp();
        /*
        conn.prepareStatement("TRUNCATE TABLE filter_expected_100000_fpp_001").execute();
        conn.prepareStatement("TRUNCATE TABLE filter_expected_1000000_fpp_003").execute();
        conn.prepareStatement("TRUNCATE TABLE filter_expected_2500000_fpp_005").execute();
         */
    }

    @AfterEach
    void tearDown() {
        streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void estimateTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A earliest=2020-01-01T00:00:00z latest=2023-01-01T00:00:00z | teragrep exec tokenizer | teragrep exec bloom estimate",
                        testFile, ds -> {
                            Assertions
                                    .assertEquals("[partition, estimate(tokens)]", Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");
                            List<Integer> results = ds
                                    .select("estimate(tokens)")
                                    .collectAsList()
                                    .stream()
                                    .map(r -> Integer.parseInt(r.get(0).toString()))
                                    .collect(Collectors.toList());

                            Assertions.assertEquals(results.get(0), 1);
                            Assertions.assertTrue(results.get(1) > 1);
                        }
                );
    }
}
