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

package com.teragrep.pth10.ast.commands.aggregate.UDAFs;

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.TimestampMapBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Aggregator for commands earliest() and latest()
 * 
 * Aggregator types: IN=Row, BUF=TimestampMapBuffer, OUT=String
 * Serializable
 * @author eemhu
 *
 */
public abstract class EarliestLatestAggregator<OUT> extends Aggregator<Row, TimestampMapBuffer, OUT> implements Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(EarliestLatestAggregator.class);

	private static final long serialVersionUID = 1L;
	private String colName = null;
	private static final boolean debugEnabled = false;
	
	/** Constructor used to feed in the column name
	 * @param colName column name for source field
	 * */
	public EarliestLatestAggregator(String colName) {
		super();
		this.colName = colName;
	}
	
	/** Encoder for the buffer (class: TimestampMapBuffer) */
	@Override
	public Encoder<TimestampMapBuffer> bufferEncoder() {
		if (debugEnabled) LOGGER.info("Buffer encoder");
		
		// TODO using kryo should speed this up
		return Encoders.javaSerialization(TimestampMapBuffer.class);
	}

	/** Abstract implementation for output encoder */
	@Override
	public abstract Encoder<OUT> outputEncoder();

	/** Initialization */
	@Override
	public TimestampMapBuffer zero() {
		if (debugEnabled) LOGGER.info("zero");
		
		return new TimestampMapBuffer();
	}

	/** Perform at the end of the aggregation */
	@Override
	public abstract OUT finish(TimestampMapBuffer buffer);

	// Merge two buffers into one
	@Override
	public TimestampMapBuffer merge(TimestampMapBuffer buffer, TimestampMapBuffer buffer2) {
		if (debugEnabled) LOGGER.info("merge");
		
		buffer.mergeMap(buffer2.getMap());
		return buffer;
	}

	/** Gets the timestamp column as a timestamp, even if it is a string instead of the proper TimestampType*/
	private Timestamp getColumnAsTimestamp(Row input) {
		Timestamp rv = null;
		try {
			rv = (Timestamp) input.getAs("_time");
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
	public TimestampMapBuffer reduce(TimestampMapBuffer buffer, Row input) {
		if (debugEnabled) LOGGER.info("reduce");
		
		Timestamp time = getColumnAsTimestamp(input);
		String val = input.getAs(colName).toString();
		buffer.add(time, val);
	
		return buffer;
	}
}
