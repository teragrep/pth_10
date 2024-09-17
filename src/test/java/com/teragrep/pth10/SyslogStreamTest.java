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

import com.teragrep.rlp_03.Server;
import com.teragrep.rlp_03.SyslogFrameProcessor;
import org.apache.spark.sql.streaming.StreamingQueryException;
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * Tests for | teragrep exec syslog stream Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SyslogStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogStreamTest.class);

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

    @Disabled(value = "RLP-03 has to be updated") /* FIXME: Update rlp_03 library to work with new rlp_01 version! */
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // teragrep exec syslog stream
    public void syslogStreamSendingTest() {
        final int expectedSyslogs = 10;
        AtomicInteger numberOfSyslogMessagesSent = new AtomicInteger();
        AtomicReferenceArray<String> arrayOfSyslogs = new AtomicReferenceArray<>(expectedSyslogs);

        final Consumer<byte[]> cbFunction = (message) -> {
            LOGGER.debug("Server received the following syslog message:\n <[{}]>\n-----", new String(message));
            Assertions.assertTrue(numberOfSyslogMessagesSent.get() <= expectedSyslogs);
            arrayOfSyslogs.set(numberOfSyslogMessagesSent.getAndIncrement(), new String(message));
        };

        final int port = 9999;
        final Server server = new Server(port, new SyslogFrameProcessor(cbFunction));
        Assertions.assertDoesNotThrow(server::start);

        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec syslog stream host 127.0.0.1 port " + port, testFile, ds -> {
                            LOGGER.debug("Syslog msgs = <{}>", numberOfSyslogMessagesSent.get());
                            Assertions.assertEquals(expectedSyslogs, numberOfSyslogMessagesSent.get());

                            for (int i = 0; i < expectedSyslogs; i++) {
                                String s = arrayOfSyslogs.get(i);
                                for (int j = 0; j < expectedSyslogs; j++) {
                                    if (i == j)
                                        continue;
                                    Assertions.assertFalse(arrayOfSyslogs.compareAndSet(j, s, s));
                                }

                            }
                            Assertions.assertAll("stop server", server::stop);
                        }
                );
    }

    @Disabled(value = "RLP-03 has to be updated") // FIXME: update rlp_03
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // teragrep exec syslog stream, with preceding aggregation command
    public void syslogStreamSendingFailureTest() {
        Assertions
                .assertThrows(
                        StreamingQueryException.class,
                        () -> streamingTestUtil
                                .performDPLTest(
                                        "index=index_A | stats count(_raw) as craw | teragrep exec syslog stream host 127.0.0.1 port 9998",
                                        testFile, ds -> {
                                        }
                                )
                );
    }
}
