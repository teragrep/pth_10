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

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.PercentileBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * Aggregator used for exactperc() and median()
 */
public class ExactPercentileAggregator extends Aggregator<Row, PercentileBuffer, Double> implements Serializable {

    private static final long serialVersionUID = 1L;
    private String colName = null;
    private double percentile = 0.5d;

    /**
     * Calculates the exact percentile using the Nearest Rank algorithm
     * 
     * @param colName    Source column name on dataframe
     * @param percentile 0.0d-1.0d percentile to calculate
     */
    public ExactPercentileAggregator(String colName, double percentile) {
        this.colName = colName;
        this.percentile = percentile;
    }

    /**
     * Buffer encoder
     * 
     * @return Encoder for PercentileBuffer
     */
    @Override
    public Encoder<PercentileBuffer> bufferEncoder() {
        // TODO using kryo should speed this up
        return Encoders.javaSerialization(PercentileBuffer.class);
    }

    /**
     * sort and calculate percentile
     * 
     * @param buffer PercentileBuffer
     * @return percentile as double
     */
    @Override
    public Double finish(PercentileBuffer buffer) {
        buffer.sortInternalList();
        return buffer.calculatePercentile();
    }

    /**
     * Merge two PercentileBuffers
     * 
     * @param buffer  Original buffer
     * @param buffer2 Buffer to merge to original
     * @return resulting buffer
     */
    @Override
    public PercentileBuffer merge(PercentileBuffer buffer, PercentileBuffer buffer2) {
        buffer.mergeList(buffer2.getList());
        return buffer;
    }

    /**
     * Output encoder
     * 
     * @return double encoder
     */
    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }

    /**
     * Add new data to buffer
     * 
     * @param buffer Buffer
     * @param input  input row
     * @return Buffer with input row added
     */
    @Override
    public PercentileBuffer reduce(PercentileBuffer buffer, Row input) {
        Object inputValue = input.getAs(colName);
        Double value = null;

        if (inputValue instanceof Long) {
            value = ((Long) inputValue).doubleValue();
        }
        else if (inputValue instanceof Integer) {
            value = ((Integer) inputValue).doubleValue();
        }
        else if (inputValue instanceof Float) {
            value = ((Float) inputValue).doubleValue();
        }
        else if (inputValue instanceof Double) {
            value = ((Double) inputValue);
        }
        else if (inputValue instanceof String) {
            value = Double.valueOf((String) inputValue);
        }

        buffer.add(value);
        return buffer;
    }

    /**
     * Initialize the buffer
     * 
     * @return initialized buffer
     */
    @Override
    public PercentileBuffer zero() {
        return new PercentileBuffer(this.percentile);
    }

}
