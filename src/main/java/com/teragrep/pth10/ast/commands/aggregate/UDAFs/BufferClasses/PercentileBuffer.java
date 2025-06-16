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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Buffer used for {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.ExactPercentileAggregator
 * ExactPercentileAggregator}
 */
public class PercentileBuffer extends ListBuffer<Double> implements Serializable {

    private static final long serialVersionUID = 1L;
    private double percentile = 0.5d;

    /**
     * Initialize the buffer with default <code>percentile = 0.5d</code>
     */
    public PercentileBuffer() {
        super();
    }

    /**
     * Initialize the buffer with a custom <code>percentile</code>
     * 
     * @param percentile values 0.0 - 1.0 (0 - 100%)
     */
    public PercentileBuffer(double percentile) {
        super();
        this.percentile = percentile;
    }

    public double getPercentile() {
        return this.percentile;
    }

    public void setPercentile(double percentile) {
        this.percentile = percentile;
    }

    // Helper methods

    /**
     * Merge list with another
     * 
     * @param another list to merge with
     */
    public void mergeList(List<Double> another) {
        this.list.addAll(another);
    }

    /**
     * Add value to buffer
     * 
     * @param value to add to the buffer
     */
    public void add(Double value) {
        this.list.add(value);
    }

    /**
     * Sort the internal list used by the buffer
     */
    public void sortInternalList() {
        Collections.sort(this.list);
    }

    /**
     * Calculates the percentile<br>
     * If the amount of values is even, get the mean of the two middle values, otherwise get the middle value.
     * 
     * @return percentile
     */
    public double calculatePercentile() {
        double nonRoundedIndex = this.percentile * (this.list.size() - 1);
        int size = this.list.size();

        int index = -1;
        if (
            Math.abs(nonRoundedIndex - Math.ceil(nonRoundedIndex)) < Math
                    .abs(nonRoundedIndex - Math.floor(nonRoundedIndex))
        ) {
            // difference from original to rounded up < down

            index = (int) Math.ceil(nonRoundedIndex);
        }
        else {
            index = (int) Math.floor(nonRoundedIndex);
        }

        if (size % 2 == 0 && index + 1 <= this.list.size() - 1) {
            return (this.list.get(index) + this.list.get(index + 1)) / 2d;
        }
        else {
            return (double) this.list.get(index);
        }
    }

}
