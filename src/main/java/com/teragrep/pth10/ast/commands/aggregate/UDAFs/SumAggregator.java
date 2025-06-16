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

import com.teragrep.pth10.ast.NullValue;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.SumBuffer;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.FieldIndex.FieldIndex;
import com.teragrep.pth10.steps.ParsedResult;
import com.teragrep.pth10.steps.TypeParser;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

public class SumAggregator extends Aggregator<Row, SumBuffer, String> {

    private final FieldIndex fieldIndex;
    private final NullValue nullValue;

    public SumAggregator(FieldIndex fieldIndex, NullValue nullValue) {
        this.fieldIndex = fieldIndex;
        this.nullValue = nullValue;
    }

    @Override
    public SumBuffer zero() {
        return new SumBuffer(nullValue);
    }

    @Override
    public SumBuffer reduce(SumBuffer current, Row input) {
        final Object inputObj = input.get(fieldIndex.fieldIndex(input));

        // ignore nulls
        if (inputObj != nullValue.value()) {
            current = getAllNumbersFromInput(current, inputObj);
        }

        return current;
    }

    @Override
    public SumBuffer merge(SumBuffer current, SumBuffer another) {
        return current.merge(another);
    }

    @Override
    public String finish(SumBuffer result) {
        return result.sum();
    }

    @Override
    public Encoder<SumBuffer> bufferEncoder() {
        return Encoders.javaSerialization(SumBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        // use string to allow returning as integers or doubles, if present
        return Encoders.STRING();
    }

    private SumBuffer getAllNumbersFromInput(SumBuffer currentSum, Object inputNumber) {
        // parse object
        TypeParser tp = new TypeParser();
        ParsedResult parsedInputNumber = tp.parse(inputNumber);

        // Skip strings, go through lists
        switch (parsedInputNumber.getType()) {
            case DOUBLE: {
                currentSum.addNumberToSum(parsedInputNumber.getDouble());
                break;
            }
            case LONG: {
                currentSum.addNumberToSum(parsedInputNumber.getLong());
                break;
            }
            case LIST: {
                // go through the list item-by-item
                for (Object item : parsedInputNumber.getList()) {
                    currentSum = getAllNumbersFromInput(currentSum, item);
                }
                break;
            }
        }

        return currentSum;
    }
}
