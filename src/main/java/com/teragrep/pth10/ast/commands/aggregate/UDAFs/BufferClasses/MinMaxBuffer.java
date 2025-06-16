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
package com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses;

import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

/**
 * The buffer class that is used for the MinMaxAggregator. Must be Java Bean compliant and serializable.
 */
public class MinMaxBuffer implements Serializable {

    private static final long serialVersionUID = 1L;

    protected Double minNumber = null;
    protected Double maxNumber = null;
    protected String minString = null;
    protected String maxString = null;

    public Double getMinNumber() {
        return minNumber;
    }

    public void setMinNumber(Double minNumber) {
        this.minNumber = minNumber;
    }

    public Double getMaxNumber() {
        return maxNumber;
    }

    public void setMaxNumber(Double maxNumber) {
        this.maxNumber = maxNumber;
    }

    public String getMinString() {
        return minString;
    }

    public void setMinString(String minString) {
        this.minString = minString;
    }

    public String getMaxString() {
        return maxString;
    }

    public void setMaxString(String maxString) {
        this.maxString = maxString;
    }

    // default IntegerType, set to DoubleType if any input is float, can't reset back to IntegerType
    private String outputFormatType = DataTypes.IntegerType.typeName();

    /**
     * Initializes the minMaxBuffer
     */
    public MinMaxBuffer() {
    }

    /**
     * Gets the outputFormatType of the buffer. Can be either <code>DataTypes.DoubleType.typeName()</code> or
     * <code>DataTypes.IntegerType.typeName()</code>
     * 
     * @return output format as a string
     */
    public String getOutputFormatType() {
        return outputFormatType;
    }

    /**
     * Sets the outputFormatType of the buffer. Can be set to <code>DataTypes.DoubleType.typeName()</code> only, the
     * default is <code>DataTypes.IntegerType.typeName()</code>
     * 
     * @param outputFormatType type to set to
     */
    public void setOutputFormatType(String outputFormatType) {
        if (!this.outputFormatType.equals(DataTypes.DoubleType.typeName())) {
            this.outputFormatType = outputFormatType;
        }
    }

    /**
     * Checks given number against current max and min values, and sets it as them if it is the new minimum and/or
     * maximum
     * 
     * @param value to add
     */
    public void addNumber(double value) {
        if (this.maxNumber == null || this.maxNumber <= value) {
            this.maxNumber = value;
        }

        if (this.minNumber == null || this.minNumber > value) {
            this.minNumber = value;
        }

    }

    /**
     * Checks given string against current max and min values, and sets it as them if it is the new minimum and/or
     * maximum
     * 
     * @param value to add
     */
    public void addString(String value) {
        if (this.maxString == null || value.compareTo(this.maxString) >= 0) {
            this.maxString = value;
        }

        if (this.minString == null || value.compareTo(this.maxString) < 0) {
            this.minString = value;
        }
    }
}
