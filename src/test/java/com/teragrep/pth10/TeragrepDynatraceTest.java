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

import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpClassCallback;
import org.mockserver.verify.VerificationTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.mockserver.model.HttpRequest.request;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TeragrepDynatraceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepDynatraceTest.class);
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
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });

    private StreamingTestUtil streamingTestUtil;
    private ClientAndServer mockServer;
    private final int port = 9001;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
        this.streamingTestUtil.setEnv();
        Configuration cfg = new Configuration();
        cfg.logLevel(Level.INFO);
        mockServer = ClientAndServer.startClientAndServer(cfg, port);
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    @AfterAll
    void stopServer() {
        mockServer.stop();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgDynatraceTest() {
        // respond to metrics ingest
        mockServer
                .when(request().withPath("/metrics/ingest").withMethod("POST").withHeader("Content-Type", "text/plain; charset=utf-8")).respond(HttpClassCallback.callback(DynatraceTestAPICallback.class));

        // send post
        this.streamingTestUtil
                .performDPLTest(
                        "index=* " + "| stats count(_raw) avg(_raw) by sourcetype "
                                + "| teragrep exec dynatrace metric write",
                        testFile, ds -> {
                        }
                );

        // two lines received
        mockServer.verify(request().withPath("/metrics/ingest"), VerificationTimes.exactly(2));
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgDynatraceNoAggregateTest() {
        // respond to metrics ingest
        mockServer
                .when(request().withPath("/metrics/ingest").withMethod("POST").withHeader("Content-Type", "text/plain; charset=utf-8")).respond(HttpClassCallback.callback(DynatraceTestAPICallback.class));

        // send post
        Throwable th = this.streamingTestUtil
                .performThrowingDPLTest(
                        RuntimeException.class, "index=* " + "| teragrep exec dynatrace metric write", testFile, ds -> {
                        }
                );

        // should not work without aggregate
        Assertions.assertTrue(th.getMessage().endsWith("requires a preceding aggregate!"));

        // 0 lines received
        mockServer.verify(request().withPath("/metrics/ingest"), VerificationTimes.never());
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void tgDynatraceNonNumericDataTest() {
        // respond to metrics ingest
        mockServer
                .when(request().withPath("/metrics/ingest").withMethod("POST").withHeader("Content-Type", "text/plain; charset=utf-8")).respond(HttpClassCallback.callback(DynatraceTestAPICallback.class));

        // send post
        Throwable th = this.streamingTestUtil
                .performThrowingDPLTest(
                        StreamingQueryException.class, "| makeresults count=10 " + "| eval _raw = \"string\""
                                + "| stats sum(_raw)" + "| teragrep exec dynatrace metric write",
                        testFile, ds -> {
                        }
                );

        // should not work with non-numeric data
        Assertions.assertEquals("Non-numeric text was provided!", th.getCause().getMessage());

        // 0 lines received
        mockServer.verify(request().withPath("/metrics/ingest"), VerificationTimes.never());
    }
}
