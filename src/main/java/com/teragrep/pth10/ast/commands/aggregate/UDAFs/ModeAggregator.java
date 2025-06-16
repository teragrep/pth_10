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

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.ModeBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * Aggregator used for the command mode()
 */
public class ModeAggregator extends Aggregator<Row, ModeBuffer, String> implements Serializable {

    private static final long serialVersionUID = 1L;
    private String colName = null;

    /**
     * Initialize with the column name
     * 
     * @param colName name of the target column
     */
    public ModeAggregator(String colName) {
        this.colName = colName;
    }

    /**
     * Buffer encoder
     * 
     * @return ModeBuffer encoder
     */
    @Override
    public Encoder<ModeBuffer> bufferEncoder() {
        // TODO kryo should speed this up
        return Encoders.javaSerialization(ModeBuffer.class);
    }

    /**
     * Return the result as string
     * 
     * @param buffer ModeBuffer
     * @return result as string
     */
    @Override
    public String finish(ModeBuffer buffer) {
        return buffer.mode();
    }

    /**
     * Merge two buffers into one
     * 
     * @param buffer  original
     * @param buffer2 another
     * @return merged buffer
     */
    @Override
    public ModeBuffer merge(ModeBuffer buffer, ModeBuffer buffer2) {
        buffer.mergeMap(buffer2.getMap());
        return buffer;
    }

    /**
     * Output encoder
     * 
     * @return String encoder
     */
    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }

    /**
     * Add new data to the buffer
     * 
     * @param buffer target buffer
     * @param input  input row
     * @return resulting buffer
     */
    @Override
    public ModeBuffer reduce(ModeBuffer buffer, Row input) {
        String inputValue = input.getAs(colName).toString();
        buffer.add(inputValue);

        return buffer;
    }

    /**
     * Initialize the ModeBuffer
     * 
     * @return initialized buffer
     */
    @Override
    public ModeBuffer zero() {
        return new ModeBuffer();
    }

}
