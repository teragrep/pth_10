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
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract class to base Map-based Aggregation buffers on
 * 
 * @author eemhu
 * @param <K> type of key in map
 * @param <V> type of value in map
 */
public abstract class MapBuffer<K, V> implements Serializable {

    private static final long serialVersionUID = 1L;
    protected Map<K, V> map;

    // Required constructors & methods for Java Bean compliance

    /**
     * Initialize a map buffer
     */
    public MapBuffer() {
        this.map = new HashMap<K, V>();
    }

    /**
     * Gets the internal map of the buffer
     * 
     * @return internal map
     */
    public Map<K, V> getMap() {
        return this.map;
    }

    /**
     * Sets the internal map of the buffer
     * 
     * @param map new internal map
     */
    public void setMap(Map<K, V> map) {
        this.map = map;
    }

    /**
     * Abstract method to merge internal map with another
     * 
     * @param another map to merge with
     */
    public abstract void mergeMap(Map<K, V> another);
}
