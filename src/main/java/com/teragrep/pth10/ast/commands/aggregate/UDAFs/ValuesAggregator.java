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

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.ValuesBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.stream.Collectors;

/**
 * Aggregator for commands values() and list() Aggregator types: IN=Row, BUF=Values, OUT=String Serializable
 * 
 * @author eemhu
 */
public class ValuesAggregator extends Aggregator<Row, ValuesBuffer, String> implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValuesAggregator.class);

    private static final long serialVersionUID = 1L;
    private static final boolean debugEnabled = false;

    private static final int maxAmountOfValues = 100; // for list()
    private String colName = null;

    private AggregatorMode.ValuesAggregatorMode mode = AggregatorMode.ValuesAggregatorMode.VALUES; // values() or list()

    /**
     * Constructor used to feed in the column name
     * 
     * @param colName column name
     * @param mode    Aggregator mode
     */
    public ValuesAggregator(String colName, AggregatorMode.ValuesAggregatorMode mode) {
        super();
        this.colName = colName;
        this.mode = mode;
    }

    /**
     * Encoder for the buffer (class: ValuesBuffer)
     * 
     * @return encoder for ValuesBuffer
     */
    @Override
    public Encoder<ValuesBuffer> bufferEncoder() {
        if (debugEnabled)
            LOGGER.info("Buffer encoder");

        // TODO using kryo should speed this up
        return Encoders.javaSerialization(ValuesBuffer.class);
    }

    /**
     * Encoder for the output (String of all the values in column, lexicographically sorted)
     * 
     * @return encoder for string
     */
    @Override
    public Encoder<String> outputEncoder() {
        if (debugEnabled)
            LOGGER.info("Output encoder");

        return Encoders.STRING();
    }

    /**
     * Initialization
     * 
     * @return initialized buffer
     */
    @Override
    public ValuesBuffer zero() {
        if (debugEnabled)
            LOGGER.info("zero");

        return new ValuesBuffer();
    }

    /**
     * Perform at the end of the aggregation
     * 
     * @param buffer buffer
     */
    @Override
    public String finish(ValuesBuffer buffer) {
        if (debugEnabled)
            LOGGER.info("finish");

        if (mode == AggregatorMode.ValuesAggregatorMode.VALUES) {
            // values() needs to be sorted in lexicographical order
            // (default java string-to-string comparison order)
            buffer.sortInternalList();

        }
        else if (mode == AggregatorMode.ValuesAggregatorMode.LIST) {
            // list() is limited to 100 first values in input order
            if (buffer.getSize() > maxAmountOfValues) {
                buffer.setList(buffer.getList().stream().limit(maxAmountOfValues).collect(Collectors.toList()));
            }
        }

        return buffer.toString();
    }

    /**
     * Merge two buffers into one
     * 
     * @param buffer  original
     * @param buffer2 another
     * @return resulting buffer
     */
    @Override
    public ValuesBuffer merge(ValuesBuffer buffer, ValuesBuffer buffer2) {
        if (debugEnabled)
            LOGGER.info("merge");

        buffer.mergeList(buffer2.getList());
        return buffer;
    }

    /**
     * Update array with new input value
     * 
     * @param buffer buffer
     * @param input  input row
     * @return resulting buffer
     */
    @Override
    public ValuesBuffer reduce(ValuesBuffer buffer, Row input) {
        if (debugEnabled)
            LOGGER.info("reduce");

        String inputString = input.getAs(colName).toString();
        buffer.add(inputString);

        return buffer;
    }
}
