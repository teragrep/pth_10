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

package com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses;

import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The buffer class that is used for the MinMaxAggregator.
 * Must be Java Bean compliant and serializable.
 */
public class MinMaxBuffer implements Serializable {
    private static final long serialVersionUID = 1L;

    protected List<Double> numericList = null;
    protected List<String> stringList = null;

    // default IntegerType, set to DoubleType if any input is float, can't reset back to IntegerType
    private String outputFormatType = DataTypes.IntegerType.typeName();

    /**
     * Initializes the minMaxBuffer with arraylists
     */
    public MinMaxBuffer() {
        this.numericList = new ArrayList<>();
        this.stringList = new ArrayList<>();
    }

    /**
     * Initializes the minMaxBuffer with pre-existing lists
     * @param numericList list for numeric values
     * @param stringList list for non-numeric values
     */
    public MinMaxBuffer(List<Double> numericList, List<String> stringList) {
        this.numericList = numericList;
        this.stringList = stringList;
    }

    public void setNumericList(List<Double> numericList) {
        this.numericList = numericList;
    }

    public void setStringList(List<String> stringList) {
        this.stringList = stringList;
    }

    public List<Double> getNumericList() {
        return numericList;
    }

    public List<String> getStringList() {
        return stringList;
    }

    /**
     * Gets the outputFormatType of the buffer.
     * Can be either <code>DataTypes.DoubleType.typeName()</code> or
     * <code>DataTypes.IntegerType.typeName()</code>
     * @return output format as a string
     */
    public String getOutputFormatType() {
        return outputFormatType;
    }

    /**
     * Sets the outputFormatType of the buffer.
     * Can be set to <code>DataTypes.DoubleType.typeName()</code> only,
     * the default is <code>DataTypes.IntegerType.typeName()</code>
     * @param outputFormatType type to set to
     */
    public void setOutputFormatType(String outputFormatType) {
        if (!this.outputFormatType.equals(DataTypes.DoubleType.typeName())) {
            this.outputFormatType = outputFormatType;
        }
    }

    /**
     * Returns the minimum value of the numbers, if numbers are present.
     * If only strings are present, use lexicographical sorting.
     * If both are non-existing, return null
     * @return minimum value as String
     */
    public String min() {
        if (this.numericList.size() > 0) {
            if (this.outputFormatType.equals(DataTypes.DoubleType.typeName())) {
                return Collections.min(this.numericList).toString();
            }
            else {
                return String.valueOf(Collections.min(this.numericList).intValue());
            }
        }
        else if (this.stringList.size() > 0) {
            return Collections.min(this.stringList);
        }
        else {
            // both empty
            return null;
        }
    }

    /**
     * Returns the maximum value of the numbers, if numbers are present.
     * If only strings are present, use lexicographical sorting.
     * If both are non-existing, return null
     * @return maximum value as String
     */
    public String max() {
        if (this.numericList.size() > 0) {
            if (this.outputFormatType.equals(DataTypes.DoubleType.typeName())) {
                return Collections.max(this.numericList).toString();
            }
            else {
                return String.valueOf(Collections.max(this.numericList).intValue());
            }
        }
        else if (this.stringList.size() > 0) {
            return Collections.max(this.stringList);
        }
        else {
            // both empty
            return null;
        }

    }

    /**
     * Returns the range of the numbers (max - min).
     * String values are not accepted.
     * @return range of the numeric values
     */
    public String range() {
        if (this.numericList.size() > 0) {
            if (this.outputFormatType.equals(DataTypes.DoubleType.typeName())) {
                return String.valueOf(Double.parseDouble(max()) - Double.parseDouble(min()));
            }
            else {
                return String.valueOf(Integer.parseInt(max()) - Integer.parseInt(min()));
            }
        }
        else {
            return null;
        }
    }
}
