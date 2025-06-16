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
package com.teragrep.pth10.ast.commands.transformstatement.teragrep;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.SDElement;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.teragrep.rlp_01.RelpBatch;
import com.teragrep.rlp_01.RelpConnection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

/**
 * Streams the given dataset (using the map function of a dataset) as syslog messages via the RELP protocol.<br>
 * Provide the RELP server's hostname/ip and port using the constructor.
 */
public class SyslogStreamer implements MapFunction<Row, Row>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogStreamer.class);
    private RelpConnection sender;
    private Boolean initialized = false;

    // relp window
    private String relpHostAddress = "127.0.0.1";
    private int relpPort = 601;

    // settings for syslog messages
    private String hostname = "localhost";
    private String appName = "teragrep";
    private String realHostname = "";

    // timeouts
    private final int connectionTimeout = 0;
    private final int readTimeout = 0;
    private final int writeTimeout = 0;
    private final int reconnectInterval = 500;

    private boolean started = false;
    // how many reconnection attempts until an exception will be thrown?
    private int failedConnectionAttempts = 0;
    private final int maxFailedConnectionAttempts = 10;

    // Getters and setters
    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    /**
     * Base constructor for the SyslogStreamer. Use the {@link #SyslogStreamer(String, int) secondary constructor} and
     * provide it with the relp server hostname and port instead.
     */
    public SyslogStreamer() {
    }

    /**
     * Constructor for SyslogStreamer, provide the RELP server's hostname and port.
     * 
     * @param relpHost relp server hostname/ip address
     * @param relpPort relp server port
     */
    public SyslogStreamer(String relpHost, int relpPort) {
        this.relpHostAddress = relpHost;
        this.relpPort = relpPort;
    }

    /**
     * Connects to the RELP server <code>relpHost:relpPort</code><br>
     * 
     * @throws RuntimeException if the server is unavailable for more than {@link #maxFailedConnectionAttempts}
     */
    private void connect() {
        boolean notConnected = true;

        while (notConnected) {
            boolean connected = false;
            try {
                realHostname = java.net.InetAddress.getLocalHost().getHostName();
                connected = sender.connect(this.relpHostAddress, this.relpPort);
            }
            catch (Exception e) {
                LOGGER.error("An exception occurred while trying to connect in SyslogStreamer: <{}>", e.getMessage());
            }

            if (connected) {
                LOGGER
                        .info(
                                "SyslogStreamer connected to RELP server host=<[{}]> port=<[{}]> !", relpHostAddress,
                                relpPort
                        );
                failedConnectionAttempts = 0;
                notConnected = false;
            }
            else {
                if (failedConnectionAttempts++ >= maxFailedConnectionAttempts) {
                    throw new RuntimeException(
                            "Connection to RELP server failed more times than allowed. " + "(Maximum "
                                    + maxFailedConnectionAttempts + " times)"
                    );
                }

                try {
                    LOGGER
                            .warn(
                                    "Connection to RELP server was unsuccessful, attempting again in <{}> ms",
                                    reconnectInterval
                            );
                    Thread.sleep(this.reconnectInterval);
                }
                catch (InterruptedException e) {
                    LOGGER.error("An error occurred while waiting for reconnection: <{}>", e.getMessage());
                }
            }
        }
    }

    /**
     * Tears down the RELP sender
     */
    private void tearDown() {
        sender.tearDown();
    }

    /**
     * Disconnects from the RELP server and calls <code>tearDown()</code>
     */
    private void disconnect() {
        boolean disconnected = false;

        try {
            disconnected = sender.disconnect();
        }
        catch (IllegalStateException | IOException | TimeoutException e) {
            LOGGER.error("An exception occurred while attempting to disconnect from RELP server: <{}>", e.getMessage());
        }
        finally {
            this.tearDown();
        }

    }

    /**
     * Starts the RELP sender
     */
    private void start() {
        if (started) {
            return;
        }

        // init events sender
        sender = new RelpConnection();

        sender.setConnectionTimeout(this.connectionTimeout);
        sender.setReadTimeout(this.readTimeout);
        sender.setWriteTimeout(this.writeTimeout);

        started = true;
        this.connect();
    }

    /**
     * Stops the RELP sender if it was started
     */
    private void stop() {
        if (!started) {
            return;
        }

        started = false;
        this.disconnect();
    }

    /**
     * Main mapping function call function. Initializes the RELP sender, connects to the RELP server,<br>
     * and builds a syslog message from each of the rows given to this function.<br>
     * To be used with the dataset map() function.<br>
     * E.g. <code>ds.map(new SyslogStreamer(host, port), ds.exprEnc());</code><br>
     * <br>
     *
     * @param row Input row to send as syslog
     * @return the given input row unchanged
     * @throws Exception If any failure is encountered during the call
     */
    public Row call(Row row) throws Exception {
        if (!initialized) {
            this.start();
            initialized = true;
        }

        final SDElement teragrep_output_48577 = new SDElement("teragrep-output@48577");
        long time = 0L;
        String payload = "";
        boolean timeSetFromColumn = false;

        // generate teragrep-output@48577 SDElement and separate _time and raw into syslog
        for (StructField field : row.schema().fields()) {
            if (field.name().equals("_time")) {
                // Convert timestamp to epoch and use as syslog message time
                Timestamp timeStamp = row.getAs(row.fieldIndex(field.name()));
                time = timeStamp.getTime();
                timeSetFromColumn = true;
            }
            else if (field.name().equals("_raw")) {
                // Get contents of _raw field as payload
                payload = row.getAs(row.fieldIndex(field.name())).toString();
            }
            else {
                // Other fields will be added into teragrep-output@48577 SDElement
                // Check for null
                Object elem = row.getAs(row.fieldIndex(field.name()));
                if (elem == null) {
                    elem = "null";
                }
                teragrep_output_48577.addSDParam(field.name(), elem.toString());
            }
        }

        // If _time column didn't exist, get current time as the syslog message time
        if (!timeSetFromColumn) {
            time = Instant.now().getEpochSecond() * 1000L;
        }

        // build the syslog message
        final SyslogMessage syslogMessage = new SyslogMessage()
                .withTimestamp(time) // _time column as syslog message time
                .withSeverity(Severity.WARNING)
                .withAppName(appName)
                .withHostname(hostname)
                .withFacility(Facility.USER)
                .withSDElement(teragrep_output_48577) // teragrep-output@48577 SDElement
                .withMsg(payload); // _raw column as syslog payload

        // send to server
        this.append(syslogMessage);

        return row;
    }

    /**
     * Appends the given syslog message into the relp batch, and sends it to the server.
     * 
     * @param syslogMessage the message to be appended to the batch, and sent to the server.
     */
    private void append(SyslogMessage syslogMessage) {
        RelpBatch relpBatch = new RelpBatch();
        // insert message into relpBatch
        final long id = relpBatch.insert(syslogMessage.toRfc5424SyslogMessage().getBytes(StandardCharsets.UTF_8));

        // attempt sending message
        boolean notSent = true;
        while (notSent) {
            LOGGER.debug("Attempting to send relpBatch with <id: {}>", id);
            try {
                sender.commit(relpBatch);
            }
            catch (IllegalStateException | IOException | java.util.concurrent.TimeoutException e) {
                LOGGER.error("Error occurred while appending new syslog message! error=<{}>", e.getMessage());
            }

            if (!relpBatch.verifyTransactionAll()) {
                LOGGER.warn("RELPBatch transaction could not be verified, retrying all failed");
                relpBatch.retryAllFailed();
                this.tearDown();
                try {
                    Thread.sleep(this.reconnectInterval);
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Reconnect sleep was interrupted", e);
                }
                this.connect();
            }
            else {
                LOGGER.debug("RELPBatch was sent successfully.");
                notSent = false;
            }
        }
    }
}
