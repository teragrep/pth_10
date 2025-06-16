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

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.MinMaxBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

/**
 * Aggregator used for commands min(), max() and range()
 */
public class MinMaxAggregator extends Aggregator<Row, MinMaxBuffer, String> implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String colName;
    private final AggregatorMode.MinMaxAggregatorMode mode;

    /**
     * Constructor for MinMaxAggregator, where the column name of the target column and the aggregator mode must be
     * specified.
     * 
     * @param colName target column name
     * @param mode    MinMaxAggregator mode
     */
    public MinMaxAggregator(String colName, AggregatorMode.MinMaxAggregatorMode mode) {
        this.colName = colName;
        this.mode = mode;
    }

    /**
     * Format the buffer (or "zero") in the beginning.
     * 
     * @return ready-to-go buffer
     */
    @Override
    public MinMaxBuffer zero() {
        return new MinMaxBuffer();
    }

    /**
     * Reduce the buffer, aka add more data to it.
     * 
     * @param minMaxBuffer Buffer
     * @param input        Row of input
     * @return Buffer with input added
     */
    @Override
    public MinMaxBuffer reduce(MinMaxBuffer minMaxBuffer, Row input) {
        Object inputValue = input.getAs(colName);

        if (inputValue instanceof Long) {
            minMaxBuffer.addNumber(((Long) inputValue).doubleValue());
        }
        else if (inputValue instanceof Double) {
            minMaxBuffer.addNumber(((Double) inputValue));
            minMaxBuffer.setOutputFormatType(DataTypes.DoubleType.typeName()); // set to double
        }
        else if (inputValue instanceof Float) {
            minMaxBuffer.addNumber(((Float) inputValue).doubleValue());
            minMaxBuffer.setOutputFormatType(DataTypes.DoubleType.typeName()); // set to double
        }
        else if (inputValue instanceof Integer) {
            minMaxBuffer.addNumber(((Integer) inputValue).doubleValue());
        }
        // If input is a string...
        else if (inputValue instanceof String) {
            try { // try parsing in into an integer
                int parsed = Integer.parseInt((String) inputValue);
                minMaxBuffer.addNumber(parsed);
            }
            catch (NumberFormatException nfe) {
                try { // if it fails, try double
                    Double parsed = Double.valueOf((String) inputValue);
                    minMaxBuffer.addNumber(parsed);
                    minMaxBuffer.setOutputFormatType(DataTypes.DoubleType.typeName()); // set to double
                }
                catch (NumberFormatException nfe2) {
                    // if that fails, add to string list
                    minMaxBuffer.addString((String) inputValue);
                }
            }
        }
        else {
            throw new RuntimeException("MinMaxAggregator: Unknown type for input: " + inputValue.toString());
        }

        return minMaxBuffer;
    }

    /**
     * Merge two buffers into one, from different executors.
     * 
     * @param buf1 Buffer #1
     * @param buf2 Buffer #2
     * @return merged buffer
     */
    @Override
    public MinMaxBuffer merge(MinMaxBuffer buf1, MinMaxBuffer buf2) {
        buf1.addNumber(buf2.getMinNumber());
        buf1.addNumber(buf2.getMaxNumber());

        buf1.addString(buf2.getMinString());
        buf1.addString(buf2.getMaxString());

        buf1.setOutputFormatType(buf2.getOutputFormatType()); // merge output format too

        return buf1;
    }

    /**
     * Finish the aggregation
     * 
     * @param minMaxBuffer Final buffer
     * @return Result
     */
    @Override
    public String finish(MinMaxBuffer minMaxBuffer) {
        if (this.mode == AggregatorMode.MinMaxAggregatorMode.MAX) {
            if (minMaxBuffer.getMaxString() != null) {
                return minMaxBuffer.getMaxString();
            }
            else if (minMaxBuffer.getOutputFormatType().equals(DataTypes.DoubleType.typeName())) {
                return minMaxBuffer.getMaxNumber().toString();
            }
            else {
                return String.valueOf(minMaxBuffer.getMaxNumber().intValue());
            }
        }
        else if (this.mode == AggregatorMode.MinMaxAggregatorMode.MIN) {
            if (minMaxBuffer.getMinString() != null) {
                return minMaxBuffer.getMinString();
            }
            else if (minMaxBuffer.getOutputFormatType().equals(DataTypes.DoubleType.typeName())) {
                return minMaxBuffer.getMinNumber().toString();
            }
            else {
                return String.valueOf(minMaxBuffer.getMinNumber().intValue());
            }
        }
        else if (this.mode == AggregatorMode.MinMaxAggregatorMode.RANGE) {
            if (minMaxBuffer.getMinString() != null || minMaxBuffer.getMaxString() != null) {
                throw new RuntimeException(
                        "Aggregate function range() requires only numeric values, but strings were found."
                );
            }
            else if (minMaxBuffer.getOutputFormatType().equals(DataTypes.DoubleType.typeName())) {
                Double range = minMaxBuffer.getMaxNumber() - minMaxBuffer.getMinNumber();
                return range.toString();
            }
            else {
                Double range = minMaxBuffer.getMaxNumber() - minMaxBuffer.getMinNumber();
                return String.valueOf(range.intValue());
            }
        }
        throw new IllegalArgumentException("MinMaxAggregator: Invalid aggregator mode: " + this.mode);
    }

    /**
     * Encoder for buffer
     * 
     * @return Buffer encoder
     */
    @Override
    public Encoder<MinMaxBuffer> bufferEncoder() {
        // TODO kryo should speed this up
        return Encoders.javaSerialization(MinMaxBuffer.class);
    }

    /**
     * Encoder for output
     * 
     * @return Output encoder
     */
    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}
