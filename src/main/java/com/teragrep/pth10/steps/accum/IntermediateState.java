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
package com.teragrep.pth10.steps.accum;

import java.io.Serializable;

public class IntermediateState implements Serializable {

    private static final long serialVersionUID = 1L;
    private Double aggValueAsDouble;
    private Long aggValueAsLong;

    private boolean isLongType;

    public IntermediateState() {
        this.aggValueAsDouble = 0d;
        this.aggValueAsLong = 0L;
        this.isLongType = true;
    }

    public void accumulate(long val) {
        // If long type, accumulate on both as long has less precision compared to double
        this.aggValueAsLong += val;
        this.aggValueAsDouble += val;
    }

    public void accumulate(double val) {
        // If double type, accumulate on only double field and set isLongType to false.
        this.isLongType = false;
        this.aggValueAsDouble += val;
    }

    public boolean isLongType() {
        return isLongType;
    }

    public Long asLong() {
        // allow return only if longs were present with no doubles
        if (this.isLongType) {
            return this.aggValueAsLong;
        }
        else {
            throw new IllegalStateException("Can't return long value as double type was used!");
        }
    }

    public Double asDouble() {
        // allow return only if one or more doubles were present
        if (!this.isLongType) {
            return this.aggValueAsDouble;
        }
        else {
            throw new IllegalStateException("Can't return double value as long type was used!");
        }
    }

    // getters and setters for java bean compliance
    public Double getAggValueAsDouble() {
        return aggValueAsDouble;
    }

    public Long getAggValueAsLong() {
        return aggValueAsLong;
    }

    public void setAggValueAsDouble(Double aggValueAsDouble) {
        this.aggValueAsDouble = aggValueAsDouble;
    }

    public void setAggValueAsLong(Long aggValueAsLong) {
        this.aggValueAsLong = aggValueAsLong;
    }

    public void setLongType(boolean longType) {
        isLongType = longType;
    }
}
