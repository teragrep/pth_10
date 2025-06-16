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
import java.util.ArrayList;
import java.util.List;

/**
 * An abstract class to be List-based aggregation buffers on
 * 
 * @author eemhu
 * @param <T> Type of data in list
 */
public abstract class ListBuffer<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    protected List<T> list;

    // Required constructors & methods for Java Bean compliance

    /**
     * Initialize a ListBuffer with an arraylist of type T
     */
    public ListBuffer() {
        this.list = new ArrayList<T>();
    }

    /**
     * Initialize a ListBuffer with an existing list of type T
     * 
     * @param list existing list to initialize the buffer with
     */
    public ListBuffer(List<T> list) {
        this.list = list;
    }

    /**
     * Gets the internal list from the buffer
     * 
     * @return list of type T
     */
    public List<T> getList() {
        return this.list;
    }

    /**
     * Sets the internal list of the buffer
     * 
     * @param list to set it to
     */
    public void setList(List<T> list) {
        this.list = list;
    }

    /**
     * Gets the size of the internal list
     * 
     * @return size of the list as an integer
     */
    public int getSize() {
        return this.list.size();
    }

    /**
     * Abstract method for sorting the internal list
     */
    public abstract void sortInternalList();

    /**
     * Abstract method for merging the internal list with another
     * 
     * @param another list to merge with
     */
    public abstract void mergeList(List<T> another);

    /**
     * Abstract method for adding data to the buffer
     * 
     * @param data to add to the buffer
     */
    public abstract void add(T data);
}
