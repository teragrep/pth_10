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
package com.teragrep.pth10.steps.addtotals;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

public class AddtotalsIntermediateState implements Serializable {

    private static final long serialVersionUID = 1L;
    private Map<Long, MultiPrecisionValuePair> aggValuePerColumn;

    /**
     * Initialize a new empty AddtotalsIntermediateState to use with flatMapGroupsWithState
     */
    public AddtotalsIntermediateState() {
        this.aggValuePerColumn = new HashMap<>();
    }

    /**
     * Accumulate a long value to index i
     * 
     * @param i   index, should match row index
     * @param val long value to add into accumulated sum
     */
    public void accumulate(long i, long val) {
        final MultiPrecisionValuePair aggValuePair = aggValuePerColumn.getOrDefault(i, new MultiPrecisionValuePair());
        aggValuePair.add(val);
        aggValuePerColumn.put(i, aggValuePair);
    }

    /**
     * Accumulate a double value to index i
     * 
     * @param i   index, should match row index
     * @param val double value to add into accumulated sum
     */
    public void accumulate(long i, double val) {
        final MultiPrecisionValuePair aggValuePair = aggValuePerColumn.getOrDefault(i, new MultiPrecisionValuePair());
        aggValuePair.add(val);
        aggValuePerColumn.put(i, aggValuePair);
    }

    /**
     * Return the accumulated sum as a long, if no doubles were present.
     * 
     * @param i column index
     * @return accumulated sum as long
     * @throws IllegalStateException if double type was used
     */
    public Long asLong(long i) {
        if (aggValuePerColumn.get(i).isLongType()) {
            return aggValuePerColumn.get(i).asLong();
        }
        else {
            throw new IllegalStateException("Can't return long value as double type was used!");
        }
    }

    /**
     * Return the accumulated sum as a double, if there were any of them present
     * 
     * @param i column index
     * @return accumulated sum as double
     * @throws IllegalStateException if no double type was used
     */
    public Double asDouble(long i) {
        if (!aggValuePerColumn.get(i).isLongType()) {
            return this.aggValuePerColumn.get(i).asDouble();
        }
        else {
            throw new IllegalStateException("Can't return double value as long type was used!");
        }
    }

    /**
     * Return the accumulated sum as a string
     * 
     * @param i column index
     * @return accumulated sum as string, which can be formatted as an integer or decimal number
     */
    public String asString(long i) {
        if (aggValuePerColumn.get(i).isLongType()) {
            return asLong(i).toString();
        }
        else {
            return asDouble(i).toString();
        }
    }

    /**
     * Checks if the given index i exists in the internal mapping
     * 
     * @param i column index
     * @return does the given index i exist in the mapping
     */
    public boolean exists(long i) {
        return aggValuePerColumn.containsKey(i);
    }

    /**
     * For Spark internal usage. Required for Java Bean compliance.
     * 
     * @return internal mapping of accumulated sums per column
     */
    public Map<Long, MultiPrecisionValuePair> getAggValuePerColumn() {
        return aggValuePerColumn;
    }

    /**
     * For Spark internal usage. Required for Java Bean compliance.
     * <p>
     * Spark uses this internally, but the param will be of the type AbstractMap. Need to copy to HashMap to allow for
     * put() operation. Otherwise accumulate() method will fail as AbstractMap does not support map manipulation via the
     * put() method. Should not be a big deal as the map will contain at best the same amount of entries as the dataset
     * has columns.
     * </p>
     * 
     * @param aggValuePerColumn map to set as the internal mapping of accumulated sums per column.
     */
    public void setAggValuePerColumn(Map<Long, MultiPrecisionValuePair> aggValuePerColumn) {
        this.aggValuePerColumn = new HashMap<>(aggValuePerColumn);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("AddtotalsIntermediateState{");
        Iterator<Map.Entry<Long, MultiPrecisionValuePair>> it = aggValuePerColumn.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, MultiPrecisionValuePair> pair = it.next();
            builder.append(pair.getKey());
            builder.append(": ");
            builder.append(pair.getValue().toString());
            if (it.hasNext()) {
                // Append comma only if there's more entries incoming
                builder.append(", ");
            }
        }
        builder.append("}");
        return builder.toString();
    }
}
