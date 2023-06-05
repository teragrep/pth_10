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

package com.teragrep.pth10.datasources;

import com.teragrep.rlo_06.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Parse RFC5424(Syslog) from Kafka source
 */
public final class ParseRFC5424FromKafka implements MapFunction<Row, Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseRFC5424FromKafka.class);

    // initialization check, constructor may not be used due to Spark serde
    private Boolean initialized = false;

    // subscribed fields as bytebuffer
    private ByteBuffer eventNodeSourceBB;
    private ByteBuffer eventNodeRelayBB;
    private ByteBuffer sourceModuleBB;
    private ByteBuffer hostnameBB;
    private ByteBuffer sourceBB;

    private ByteBuffer teragrepBB;
    private ByteBuffer indexBB;
    private ByteBuffer sourcetypeBB;

    private ByteBuffer sourceStringBB;

    // origin
    private ByteBuffer originSDIDBB;
    private ByteBuffer originSDEhostnameBB;
    private ByteBuffer originStringBB;

    // Currently handled log event from Kafka
    ParserResultset currentResultSet;
    ResultsetAsByteBuffer resultsetAsByteBuffer;

    RFC5424Parser parser;

    private final Boolean ignoreParserFailures;
    private Instant kafkaNotAfterInstant;

    public ParseRFC5424FromKafka(Boolean ignoreParserFailures) {
        this.ignoreParserFailures = ignoreParserFailures;
    }

    public Row call(Row inRow) {
        // Incoming fields: offset, partition, topic, timestamp , value, _syslog
        if(!initialized){
            this.kafkaNotAfterInstant = Instant.now(); // todo configureable
            // match string initializers, used for querying the resultset
            byte[] ens = "event_node_source@48577".getBytes(StandardCharsets.US_ASCII);
            this.eventNodeSourceBB = ByteBuffer.allocateDirect(ens.length);
            this.eventNodeSourceBB.put(ens, 0, ens.length);
            this.eventNodeSourceBB.flip();

            byte[] enr = "event_node_relay@48577".getBytes(StandardCharsets.US_ASCII);
            this.eventNodeRelayBB = ByteBuffer.allocateDirect(enr.length);
            this.eventNodeRelayBB.put(enr, 0, enr.length);
            this.eventNodeRelayBB.flip();

            byte[] sm = "source_module".getBytes(StandardCharsets.US_ASCII);
            this.sourceModuleBB = ByteBuffer.allocateDirect(sm.length);
            this.sourceModuleBB.put(sm, 0, sm.length);
            this.sourceModuleBB.flip();

            byte[] hn = "hostname".getBytes(StandardCharsets.US_ASCII);
            this.hostnameBB = ByteBuffer.allocateDirect(hn.length);
            this.hostnameBB.put(hn, 0, hn.length);
            this.hostnameBB.flip();

            byte[] source = "source".getBytes(StandardCharsets.US_ASCII);
            this.sourceBB = ByteBuffer.allocateDirect(source.length);
            this.sourceBB.put(source, 0, source.length);
            this.sourceBB.flip();

            // teragrep
            byte[] teragrep = "teragrep@48577".getBytes(StandardCharsets.US_ASCII);
            this.teragrepBB = ByteBuffer.allocateDirect(teragrep.length);
            this.teragrepBB.put(teragrep, 0, teragrep.length);
            this.teragrepBB.flip();

            byte[] index = "directory".getBytes(StandardCharsets.US_ASCII);
            this.indexBB = ByteBuffer.allocateDirect(index.length);
            this.indexBB.put(index, 0, index.length);
            this.indexBB.flip();

            byte[] sourcetype = "streamname".getBytes(StandardCharsets.US_ASCII);
            this.sourcetypeBB = ByteBuffer.allocateDirect(sourcetype.length);
            this.sourcetypeBB.put(sourcetype, 0, sourcetype.length);
            this.sourcetypeBB.flip();

            this.sourceStringBB = ByteBuffer.allocateDirect(8*1024 + 1 + 8*1024 + 1 + 8*1024);

            // origin
            byte[] originSDID = "origin@48577".getBytes(StandardCharsets.US_ASCII);
            this.originSDIDBB = ByteBuffer.allocateDirect(originSDID.length);
            this.originSDIDBB.put(originSDID, 0, originSDID.length);
            this.originSDIDBB.flip();

            byte[] originSDEhostname = "hostname".getBytes(StandardCharsets.US_ASCII);
            this.originSDEhostnameBB = ByteBuffer.allocateDirect(originSDEhostname.length);
            this.originSDEhostnameBB.put(originSDEhostname, 0, originSDEhostname.length);
            this.originSDEhostnameBB.flip();

            this.originStringBB = ByteBuffer.allocateDirect(8*1024);

            // subscriptions
            // Define fields we are interested
            // Main level
            RFC5424ParserSubscription subscription = new RFC5424ParserSubscription();
            subscription.add(ParserEnum.TIMESTAMP);
            subscription.add(ParserEnum.HOSTNAME);
            subscription.add(ParserEnum.MSG);

            // Structured
            RFC5424ParserSDSubscription sdSubscription = new RFC5424ParserSDSubscription();
            sdSubscription.subscribeElement("event_node_source@48577","source");
            sdSubscription.subscribeElement("event_node_relay@48577","source");
            sdSubscription.subscribeElement("event_node_source@48577","source_module");
            sdSubscription.subscribeElement("event_node_relay@48577","source_module");
            sdSubscription.subscribeElement("event_node_source@48577","hostname");
            sdSubscription.subscribeElement("event_node_relay@48577","hostname");

            sdSubscription.subscribeElement("teragrep@48577","streamname");
            sdSubscription.subscribeElement("teragrep@48577","directory");
            sdSubscription.subscribeElement("teragrep@48577","unixtime");

            // Origin
            sdSubscription.subscribeElement("origin@48577","hostname");

            // workaround cfe-06/issue/68 by subscribing the broken field
            sdSubscription.subscribeElement("rfc3164@48577","syslogtag");

            // Set up result
            currentResultSet = new ParserResultset(subscription, sdSubscription);

            // resultset processor
            resultsetAsByteBuffer = new ResultsetAsByteBuffer(null);

            parser = new RFC5424Parser(null);

            initialized = true;
        } else {
            currentResultSet.clear();
        }

        final long offset = inRow.getLong(0);
        final String partition = Integer.toString(inRow.getInt(1));
        final String topic = inRow.getString(2);
        byte[] logEvent = inRow.getAs(4);

        // Set up parser for event        
        parser.setInputStream(new ByteArrayInputStream(logEvent));
        if(LOGGER.isTraceEnabled())
            LOGGER.trace(" ParseRFC5424FromKafka() Parsing:>"+logEvent.toString()+"<");
        // parse event
        try {
            parser.next(currentResultSet);
        } catch (IOException ioe){
            LOGGER.error("RFC5424-parser can't access event topic:"+topic+ " offset:"+offset+" partition:"+partition + " reason:"+ioe.getMessage());
            if (ignoreParserFailures) {
                Instant instant = Instant.ofEpochMilli(0L);
                return RowFactory.create(
                        Timestamp.from(instant), // 0 "_time", DataTypes.TimestampType
                        "",         // 1 "_raw", DataTypes.StringType
                        topic,      // 2 "directory", DataTypes.StringType
                        "",         // 3 "stream", DataTypes.StringType
                        "",         // 4 "host", DataTypes.StringType,
                        "dpl.ignoreParserFailures=true",         // 5 "input", DataTypes.StringType
                        partition,  // 6 "partition", DataTypes.StringType
                        offset      // 7 "offset", DataTypes.LongType
                );
            }
            else {
                throw new RuntimeException(ioe.getMessage());
            }
        } catch (ParseException parseException) {
            LOGGER.error("RFC5424-parser can't parse event topic:"+topic+ " offset:"+offset+" partition:"+partition + " reason:"+parseException.getMessage());
            if (ignoreParserFailures) {
                Instant instant = Instant.ofEpochMilli(0L);
                return RowFactory.create(
                        Timestamp.from(instant), // 0 "_time", DataTypes.TimestampType
                        "",         // 1 "_raw", DataTypes.StringType
                        topic,      // 2 "directory", DataTypes.StringType
                        "",         // 3 "stream", DataTypes.StringType
                        "",         // 4 "host", DataTypes.StringType,
                        "dpl.ignoreParserFailures=true",         // 5 "input", DataTypes.StringType
                        partition,  // 6 "partition", DataTypes.StringType
                        offset      // 7 "offset", DataTypes.LongType
                );
            }
            else {
                throw parseException;
            }
        }

        resultsetAsByteBuffer.setResultset(currentResultSet);

        final Instant instant = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(
                StandardCharsets.ISO_8859_1.decode(
                        resultsetAsByteBuffer.getTIMESTAMP()
                ), Instant::from);


        // input
        final byte[] input = eventToInput();

        // origin
        final byte[] origin = eventToOrigin();

        // message
        final ByteBuffer messageBB = resultsetAsByteBuffer.getMSG();
        final byte[] message = new byte[messageBB.remaining()];
        messageBB.get(message);

        // message
        final ByteBuffer hostnameBB = resultsetAsByteBuffer.getHOSTNAME();
        final byte[] hostname = new byte[hostnameBB.remaining()];
        hostnameBB.get(hostname);

        // index
        ByteBuffer indexResBB = resultsetAsByteBuffer.getSdValue(teragrepBB, indexBB);
        final byte[] index = new byte[indexResBB.remaining()];
        indexResBB.get(index);

        // sourcetype
        ByteBuffer sourcetypeResBB = resultsetAsByteBuffer.getSdValue(teragrepBB, sourcetypeBB);
        final byte[] sourcetype = new byte[sourcetypeResBB.remaining()];
        sourcetypeResBB.get(sourcetype);

        return RowFactory.create(
                Timestamp.from(instant),                    // 0 "_time", DataTypes.TimestampType
                UTF8String.fromBytes(message).toString(),   // 1 "_raw", DataTypes.StringType
                UTF8String.fromBytes(index).toString(),     // 2 "directory", DataTypes.StringType
                UTF8String.fromBytes(sourcetype).toString(),// 3 "stream", DataTypes.StringType
                UTF8String.fromBytes(hostname).toString(),  // 4 "host", DataTypes.StringType,
                UTF8String.fromBytes(input).toString(),     // 5 "input", DataTypes.StringType
                partition,                       // 6 "partition", DataTypes.StringType
                offset,                           // 7 "offset", DataTypes.LongType
                UTF8String.fromBytes(origin).toString()     // 8 "origin", DataTypes.StringType
        );
    }

    private byte[] eventToOrigin() {
        ByteBuffer origin_element = resultsetAsByteBuffer.getSdValue(originSDIDBB, originSDEhostnameBB);
        originStringBB.clear();
        if (origin_element != null) {
            originStringBB.put(origin_element);
        }
        originStringBB.flip();
        byte[] input = new byte[originStringBB.remaining()];
        originStringBB.get(input);
        return input;
    }

    private byte[] eventToInput() {
        //input is produced from SD element event_node_source@48577 by
        // concatenating "source_module:hostname:source". in case
        //if event_node_source@48577 is not available use event_node_relay@48577.
        //If neither are present, use null value.

        ByteBuffer source_module = resultsetAsByteBuffer.getSdValue(eventNodeSourceBB, sourceModuleBB);
        if(source_module == null || !source_module.hasRemaining()){
            source_module = resultsetAsByteBuffer.getSdValue(eventNodeRelayBB, sourceModuleBB);
        }
        ByteBuffer hostname = resultsetAsByteBuffer.getSdValue(eventNodeSourceBB, hostnameBB);
        if(hostname == null || !hostname.hasRemaining()){
            hostname = resultsetAsByteBuffer.getSdValue(eventNodeRelayBB, hostnameBB);
        }
        ByteBuffer source = resultsetAsByteBuffer.getSdValue(eventNodeSourceBB, sourceBB);
        if(source == null || !source.hasRemaining()){
            source = resultsetAsByteBuffer.getSdValue(eventNodeRelayBB, sourceBB);
        }

        // sm:hn:s
        sourceStringBB.clear();
        // source_module:hostname:source"
        if(source_module != null) {
            sourceStringBB.put(source_module);
        }
        sourceStringBB.put((byte) ':');
        if (hostname != null) {
            sourceStringBB.put(hostname);
        }
        sourceStringBB.put((byte)':');
        if (source != null) {
            sourceStringBB.put(source);
        }

        sourceStringBB.flip();
        byte[] input = new byte[sourceStringBB.remaining()];
        sourceStringBB.get(input);

        return input;
    }
}

