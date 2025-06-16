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

import com.teragrep.net_01.channel.socket.PlainFactory;
import com.teragrep.net_01.eventloop.EventLoop;
import com.teragrep.net_01.eventloop.EventLoopFactory;
import com.teragrep.net_01.server.Server;
import com.teragrep.net_01.server.ServerFactory;
import com.teragrep.rlp_03.frame.FrameDelegationClockFactory;
import com.teragrep.rlp_03.frame.delegate.DefaultFrameDelegate;
import com.teragrep.rlp_03.frame.delegate.FrameContext;
import com.teragrep.rlp_03.frame.delegate.FrameDelegate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Tests for | teragrep exec syslog stream Uses streaming datasets
 * 
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SyslogStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogStreamTest.class);

    private final List<String> messages = new ArrayList<>();
    private final int listenPort = 9999;

    private Server server;
    private EventLoop eventLoop;
    private Thread eventLoopThread;
    private ExecutorService executorService;

    private final String testFile = "src/test/resources/regexTransformationTest_data*.jsonl"; // * to make the path into a directory path
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
        messages.clear();
        serverSetup();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
        eventLoop.stop();
        Assertions.assertDoesNotThrow(() -> eventLoopThread.join());
        executorService.shutdown();
        Assertions.assertDoesNotThrow(() -> server.close());
    }

    private void serverSetup() {
        executorService = Executors.newFixedThreadPool(1);

        Consumer<FrameContext> syslogConsumer = new Consumer<FrameContext>() {

            // NOTE: synchronized because frameDelegateSupplier returns this instance for all the parallel connections
            @Override
            public synchronized void accept(FrameContext frameContext) {
                messages.add(frameContext.relpFrame().payload().toString());
            }
        };

        /*
         * New instance of the frameDelegate is provided for every connection
         */
        Supplier<FrameDelegate> frameDelegateSupplier = () -> new DefaultFrameDelegate(syslogConsumer);

        /*
         * EventLoop is used to notice any events from the connections
         */
        EventLoopFactory eventLoopFactory = new EventLoopFactory();
        try {
            eventLoop = eventLoopFactory.create();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        eventLoopThread = new Thread(eventLoop);
        /*
         * eventLoopThread must run, otherwise nothing will be processed
         */
        eventLoopThread.start();

        /*
         * ServerFactory is used to create server instances
         */
        ServerFactory serverFactory = new ServerFactory(
                eventLoop,
                executorService,
                new PlainFactory(),
                new FrameDelegationClockFactory(frameDelegateSupplier)
        );

        try {
            server = serverFactory.create(listenPort);
            System.out.println("server started at port <" + listenPort + ">");
        }
        catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // teragrep exec syslog stream
    public void syslogStreamSendingTest() {
        streamingTestUtil
                .performDPLTest(
                        "index=index_A | teragrep exec syslog stream host 127.0.0.1 port " + listenPort, testFile,
                        ds -> {
                            LOGGER.debug("Syslog msgs = <{}>", messages.size());
                            Assertions.assertEquals(10, messages.size());
                            Assertions.assertEquals(10, new HashSet<>(messages).size());
                        }
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    ) // teragrep exec syslog stream, with preceding aggregation command
    public void syslogStreamSendingFailureTest() {
        RuntimeException rte = streamingTestUtil
                .performThrowingDPLTest(
                        RuntimeException.class,
                        "index=index_A | stats count(_raw) as craw | teragrep exec syslog stream host 127.1.0.1 port 9998",
                        testFile, ds -> {
                        }
                );

        Assertions.assertNotNull(rte);
        Assertions
                .assertEquals(
                        rte.getMessage(),
                        "Step 'TeragrepSyslogStep{relpHost=127.1.0.1, relpPort=9998}' cannot be used after aggregations!"
                );
    }
}
