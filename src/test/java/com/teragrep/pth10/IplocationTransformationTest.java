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

import com.teragrep.pth10.ast.commands.transformstatement.iplocation.IplocationGeoIPDataMapper;
import com.teragrep.pth10.ast.commands.transformstatement.iplocation.IplocationRirDataMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IplocationTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IplocationTransformationTest.class);
    private final String testFile = "src/test/resources/IplocationTransformationTest_data*.jsonl"; // * to make the path into a directory path

    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("otherIP", DataTypes.StringType, true, new MetadataBuilder().build())
    });

    private final String[] GEOIP_MINIMAL_COLUMNS = new String[] {
            "country", "region", "city", "lat", "lon"
    };
    private final String[] GEOIP_FULL_COLUMNS = new String[] {
            "country", "region", "city", "metroCode", "continent", "lat", "lon"
    };
    private final String[] RIR_COLUMNS = new String[] {
            "operator", "country"
    };
    private final String[] COUNTRY_COLUMNS = new String[] {
            "country", "continent"
    };

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

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @DisabledIfSystemProperty(
            named = "skipGeoLiteTest",
            matches = "true"
    )
    public void testIplocationGeoLite2City() {
        String mmdbPath = "/usr/share/GeoIP/GeoLite2-City.mmdb";
        String[] expectedCols = GEOIP_MINIMAL_COLUMNS;
        String ipColumn = "source";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil.performDPLTest("index=index_A | iplocation source", this.testFile, ds -> {
            LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

            // GEO DB type, get db mapper
            IplocationGeoIPDataMapper mapper = new IplocationGeoIPDataMapper(
                    mmdbPath,
                    this.streamingTestUtil.getCtx().nullValue,
                    extractMapFromHadoopCfg(
                            this.streamingTestUtil.getCtx().getSparkSession().sparkContext().hadoopConfiguration()
                    )
            );

            // run mapper on ip to assert expected
            List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
            for (Row ip : ips) {
                Map<String, String> result = Assertions
                        .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                for (String col : expectedCols) {
                    Assertions.assertEquals(result.get(col), ip.getAs(ip.fieldIndex(col)));
                }
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testIplocationRirDataSample() {
        String mmdbPath = "src/test/resources/rir-data.sample.mmdb";
        String[] expectedCols = RIR_COLUMNS;
        String ipColumn = "source";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil.performDPLTest("index=index_A | iplocation source", this.testFile, ds -> {
            LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

            // RIR DB type
            IplocationRirDataMapper mapper = new IplocationRirDataMapper(
                    mmdbPath,
                    this.streamingTestUtil.getCtx().nullValue,
                    extractMapFromHadoopCfg(
                            this.streamingTestUtil.getCtx().getSparkSession().sparkContext().hadoopConfiguration()
                    )
            );

            // run mapper on ip to assert expected
            List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
            for (Row ip : ips) {
                Map<String, String> result = Assertions
                        .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                for (String col : expectedCols) {
                    String expected = result.get(col);
                    Assertions.assertEquals(expected, ip.getAs(ip.fieldIndex(col)));
                }
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @DisabledIfSystemProperty(
            named = "skipGeoLiteTest",
            matches = "true"
    )
    public void testIplocationGeoLite2Country() {
        String mmdbPath = "/usr/share/GeoIP/GeoLite2-Country.mmdb";
        String[] expectedCols = COUNTRY_COLUMNS;
        String ipColumn = "source";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil.performDPLTest("index=index_A | iplocation source", this.testFile, ds -> {
            LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

            // GEO DB type, get db mapper
            IplocationGeoIPDataMapper mapper = new IplocationGeoIPDataMapper(
                    mmdbPath,
                    this.streamingTestUtil.getCtx().nullValue,
                    extractMapFromHadoopCfg(
                            this.streamingTestUtil.getCtx().getSparkSession().sparkContext().hadoopConfiguration()
                    )
            );

            // run mapper on ip to assert expected
            List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
            for (Row ip : ips) {
                Map<String, String> result = Assertions
                        .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                for (String col : expectedCols) {
                    Assertions.assertEquals(result.get(col), ip.getAs(ip.fieldIndex(col)));
                }
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @DisabledIfSystemProperty(
            named = "skipGeoLiteTest",
            matches = "true"
    )
    public void testIplocationGeoLite2CityWithAllfieldsTrue() {
        String mmdbPath = "/usr/share/GeoIP/GeoLite2-City.mmdb";
        String[] expectedCols = GEOIP_FULL_COLUMNS;
        String ipColumn = "source";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil.performDPLTest("index=index_A | iplocation allfields=true source", this.testFile, ds -> {
            LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

            // GEO DB type, get db mapper
            IplocationGeoIPDataMapper mapper = new IplocationGeoIPDataMapper(
                    mmdbPath,
                    this.streamingTestUtil.getCtx().nullValue,
                    extractMapFromHadoopCfg(
                            this.streamingTestUtil.getCtx().getSparkSession().sparkContext().hadoopConfiguration()
                    )
            );

            // run mapper on ip to assert expected
            List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
            for (Row ip : ips) {
                Map<String, String> result = Assertions
                        .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                for (String col : expectedCols) {
                    Assertions.assertEquals(result.get(col), ip.getAs(ip.fieldIndex(col)));
                }
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @DisabledIfSystemProperty(
            named = "skipGeoLiteTest",
            matches = "true"
    )
    public void testIplocationGeoLite2CityInvalidIPAddressWithAllfieldsTrue() {
        String mmdbPath = "/usr/share/GeoIP/GeoLite2-City.mmdb";
        String[] expectedCols = GEOIP_FULL_COLUMNS;
        String ipColumn = "otherIP";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil
                .performDPLTest("index=index_A | iplocation allfields=true otherIP", this.testFile, ds -> {
                    LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

                    // GEO DB type, get db mapper
                    IplocationGeoIPDataMapper mapper = new IplocationGeoIPDataMapper(
                            mmdbPath,
                            this.streamingTestUtil.getCtx().nullValue,
                            extractMapFromHadoopCfg(
                                    this.streamingTestUtil
                                            .getCtx()
                                            .getSparkSession()
                                            .sparkContext()
                                            .hadoopConfiguration()
                            )
                    );

                    // run mapper on ip to assert expected
                    List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
                    for (Row ip : ips) {
                        Map<String, String> result = Assertions
                                .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                        for (String col : expectedCols) {
                            Assertions.assertEquals(result.get(col), ip.getAs(ip.fieldIndex(col)));
                        }
                    }
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @DisabledIfSystemProperty(
            named = "skipGeoLiteTest",
            matches = "true"
    )
    public void testIplocationGeoLite2CityInvalidIPAddressWithAllfieldsFalse() {
        String mmdbPath = "/usr/share/GeoIP/GeoLite2-City.mmdb";
        String[] expectedCols = GEOIP_MINIMAL_COLUMNS;
        String ipColumn = "otherIP";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil
                .performDPLTest("index=index_A | iplocation otherIP allfields=false", this.testFile, ds -> {
                    LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

                    // GEO DB type, get db mapper
                    IplocationGeoIPDataMapper mapper = new IplocationGeoIPDataMapper(
                            mmdbPath,
                            this.streamingTestUtil.getCtx().nullValue,
                            extractMapFromHadoopCfg(
                                    this.streamingTestUtil
                                            .getCtx()
                                            .getSparkSession()
                                            .sparkContext()
                                            .hadoopConfiguration()
                            )
                    );

                    // run mapper on ip to assert expected
                    List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
                    for (Row ip : ips) {
                        Map<String, String> result = Assertions
                                .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                        for (String col : expectedCols) {
                            Assertions.assertEquals(result.get(col), ip.getAs(ip.fieldIndex(col)));
                        }
                    }
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    @DisabledIfSystemProperty(
            named = "skipGeoLiteTest",
            matches = "true"
    )
    public void testIplocationRirDataInvalidIPAddress() {
        String mmdbPath = "src/test/resources/rir-data.sample.mmdb";
        String[] expectedCols = RIR_COLUMNS;
        String ipColumn = "otherIP";

        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);
        this.streamingTestUtil
                .performDPLTest("index=index_A | iplocation otherIP allfields=false", this.testFile, ds -> {
                    LOGGER.info("Consumer dataset's schema is <{}>", ds.schema());

                    // RIR DB type
                    IplocationRirDataMapper mapper = new IplocationRirDataMapper(
                            mmdbPath,
                            this.streamingTestUtil.getCtx().nullValue,
                            extractMapFromHadoopCfg(
                                    this.streamingTestUtil
                                            .getCtx()
                                            .getSparkSession()
                                            .sparkContext()
                                            .hadoopConfiguration()
                            )
                    );

                    // run mapper on ip to assert expected
                    List<Row> ips = ds.select(ipColumn, expectedCols).collectAsList();
                    for (Row ip : ips) {
                        Map<String, String> result = Assertions
                                .assertDoesNotThrow(() -> mapper.call(ip.getAs(ip.fieldIndex(ipColumn)), "en", true));

                        for (String col : expectedCols) {
                            String expected = result.get(col);
                            Assertions.assertEquals(expected, ip.getAs(ip.fieldIndex(col)));
                        }
                    }
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testIplocationInvalidMmdbPath() {
        String mmdbPath = "/tmp/this-path-is-invalid/fake.mmdb";
        this.streamingTestUtil.getCatalystVisitor().setIplocationMmdbPath(mmdbPath);

        StreamingQueryException sqe = this.streamingTestUtil
                .performThrowingDPLTest(
                        StreamingQueryException.class, "index=index_A | iplocation allfields=true source",
                        this.testFile, (ds) -> {
                        }
                );

        Assertions
                .assertEquals(
                        "Caused by: java.lang.RuntimeException: Invalid database file path given for iplocation command.",
                        this.streamingTestUtil.getInternalCauseString(sqe.cause(), RuntimeException.class)
                );
    }

    // ----------------------------------------
    // Helper methods
    // ----------------------------------------

    private Map<String, String> extractMapFromHadoopCfg(Configuration hadoopCfg) {
        final Map<String, String> hadoopCfgAsMap = new HashMap<>();

        for (Map.Entry<String, String> me : hadoopCfg) {
            hadoopCfgAsMap.put(me.getKey(), me.getValue());
        }

        return hadoopCfgAsMap;
    }
}
