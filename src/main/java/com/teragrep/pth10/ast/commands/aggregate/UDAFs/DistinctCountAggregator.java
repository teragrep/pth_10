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

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.CountBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Aggregator for command dc()
 * 
 * Aggregator types: IN=Row, BUF=CountBuffer, OUT=String
 * Serializable
 * @author eemhu
 *
 */
public class DistinctCountAggregator extends Aggregator<Row, CountBuffer, Integer> implements Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(DistinctCountAggregator.class);

	private static final long serialVersionUID = 1L;
	private String colName = null;
	private static final boolean debugEnabled = false;
	
	/**
	 * Constructor used to feed in the column name
	 * @param colName Column name for source field
	 * */
	public DistinctCountAggregator(String colName) {
		super();
		this.colName = colName;
	}
	
	/** Encoder for the buffer (class: Values)*/
	@Override
	public Encoder<CountBuffer> bufferEncoder() {
		if (debugEnabled) LOGGER.info("Buffer encoder");
		
		// TODO using kryo should speed this up
		return Encoders.javaSerialization(CountBuffer.class);
	}

	/** Encoder for the output (String of all the values in column, lexicographically sorted)*/
	@Override
	public Encoder<Integer> outputEncoder() {
		if (debugEnabled) LOGGER.info("Output encoder");
		
		return Encoders.INT();
	}

	/** Initialization */
	@Override
	public CountBuffer zero() {
		if (debugEnabled) LOGGER.info("zero");
		
		return new CountBuffer();
	}

	/** Perform at the end of the aggregation */
	@Override
	public Integer finish(CountBuffer buffer) {
		if (debugEnabled) LOGGER.info("finish");
		
		return buffer.dc();
	}

	/** Merge two buffers into one */
	@Override
	public CountBuffer merge(CountBuffer buffer, CountBuffer buffer2) {
		if (debugEnabled) LOGGER.info("merge");
		
		buffer.mergeMap(buffer2.getMap());
		return buffer;
	}

	/** Update array with new input value */
	@Override
	public CountBuffer reduce(CountBuffer buffer, Row input) {
		if (debugEnabled) LOGGER.info("reduce");
		
		String inputString = input.getAs(colName).toString();
		buffer.add(inputString);
	
		return buffer;
	}
}
