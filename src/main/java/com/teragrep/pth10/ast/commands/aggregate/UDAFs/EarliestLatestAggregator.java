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
package com.teragrep.pth10.ast.commands.aggregate.UDAFs;

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.EarliestLatestBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Aggregator for commands earliest() and latest() Aggregator types: IN=Row, BUF=TimestampMapBuffer, OUT=String
 * Serializable
 * 
 * @author eemhu
 */
public abstract class EarliestLatestAggregator<OUT> extends Aggregator<Row, EarliestLatestBuffer, OUT>
        implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String colName;

    /**
     * Constructor used to feed in the column name
     * 
     * @param colName column name for source field
     */
    public EarliestLatestAggregator(String colName) {
        super();
        this.colName = colName;
    }

    /** Encoder for the buffer (class: EarliestLatestBuffer) */
    @Override
    public Encoder<EarliestLatestBuffer> bufferEncoder() {
        // TODO using kryo should speed this up
        return Encoders.javaSerialization(EarliestLatestBuffer.class);
    }

    /** Abstract implementation for output encoder */
    @Override
    public abstract Encoder<OUT> outputEncoder();

    /** Initialization */
    @Override
    public EarliestLatestBuffer zero() {
        return new EarliestLatestBuffer(colName);
    }

    /** Perform at the end of the aggregation */
    @Override
    public abstract OUT finish(EarliestLatestBuffer buffer);

    // Merge two buffers into one
    @Override
    public EarliestLatestBuffer merge(EarliestLatestBuffer buffer, EarliestLatestBuffer buffer2) {
        CurrentTimestamp newEarliest = buffer.earliestTimestamp();
        CurrentTimestamp newLatest = buffer.latestTimestamp();
        Row newEarliestRow = buffer.earliestRow();
        Row newLatestRow = buffer.latestRow();

        if (buffer.earliestTimestamp().isEmpty()) {
            newEarliest = buffer2.earliestTimestamp();
            newEarliestRow = buffer2.earliestRow();
        }

        if (buffer.latestTimestamp().isEmpty()) {
            newLatest = buffer2.latestTimestamp();
            newLatestRow = buffer2.latestRow();
        }

        if (!buffer2.earliestTimestamp().isEmpty() && buffer2.earliestTimestamp().isBefore(newEarliest)) {
            newEarliest = buffer2.earliestTimestamp();
            newEarliestRow = buffer2.earliestRow();
        }

        if (!buffer2.latestTimestamp().isEmpty() && buffer2.latestTimestamp().isAfter(newLatest)) {
            newLatest = buffer2.latestTimestamp();
            newLatestRow = buffer2.latestRow();
        }

        return new EarliestLatestBuffer(colName, newEarliest, newLatest, newEarliestRow, newLatestRow);
    }

    /** Gets the timestamp column as a timestamp, even if it is a string instead of the proper TimestampType */
    private Timestamp getColumnAsTimestamp(Row input) {
        Timestamp rv;
        try {
            rv = input.getAs("_time");
        }
        catch (ClassCastException cce) {
            // This should really never be needed, but it seems like the test reads timestamp in as a stringtype
            // rather than a timestamp
            String temp = input.getAs("_time").toString();

            DateTimeFormatter formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME;
            ZonedDateTime zonedDateTime = LocalDateTime.from(formatter.parse(temp)).atZone(ZoneId.of("UTC"));
            rv = Timestamp.from(Instant.ofEpochSecond(zonedDateTime.toEpochSecond()));
        }

        return rv;
    }

    /** Update TimestampMapBuffer with new value */
    @Override
    public EarliestLatestBuffer reduce(EarliestLatestBuffer buffer, Row input) {
        Timestamp time = getColumnAsTimestamp(input);
        EarliestLatestBuffer newBuffer = new EarliestLatestBuffer(
                colName,
                new CurrentTimestampImpl(time),
                new CurrentTimestampImpl(time),
                input,
                input
        );

        return merge(buffer, newBuffer);
    }
}
