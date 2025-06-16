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
package com.teragrep.pth10.ast.commands.evalstatement.UDFs;

import org.apache.spark.sql.api.java.UDF4;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;

/**
 * UDF for command mvindex(mvfield, startindex, endindex)<br>
 * Returns the values between startindex and endindex (inclusive)<br>
 * Indices can be negative, first value is 0 and last element can be accessed with -1.<br>
 * If only startindex is provided, only the element that matches that index will be returned.<br>
 * Examples:<br>
 * (1) mvindex(field, 0, 5) - return elements [0..5]<br>
 * (2) mvindex(field, 0, -1) - return all elements<br>
 * (3) mvindex(field, 0) - return only the first element<br>
 * (4) mvindex(field, -1) - return only the last element<br>
 * 
 * @author eemhu
 */
public class Mvindex
        implements UDF4<WrappedArray<String>, Integer, Integer, Boolean, WrappedArray<String>>, Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    @Override
    public WrappedArray<String> call(
            WrappedArray<String> mvField,
            Integer startIndex,
            Integer endIndex,
            Boolean endIndexProvided
    ) throws Exception {
        // If endIndex was not given, get one element specified by startIndex
        if (!endIndexProvided) {
            if (startIndex != -1) {
                // if start=0,1,2,... with no endIndex, get that element only
                return ((WrappedArray<String>) mvField.slice(startIndex, startIndex + 1));
            }
            else {
                // if start=-1, get last element
                return ((WrappedArray<String>) mvField.takeRight(1));
            }
        }
        else if (endIndex == -1) {
            // if endIndex=-1, set it to last element
            endIndex = mvField.size() - 1;
        }

        // Drop elements from left and right based on given indices
        // However, if nothing is to be dropped, don't even call the drop()/dropRight() function
        int nDropFromRight = (mvField.size() - (endIndex + 1));
        if (nDropFromRight > 0)
            mvField = (WrappedArray<String>) mvField.dropRight(nDropFromRight);

        int nDropFromLeft = startIndex;
        if (nDropFromLeft > 0)
            mvField = (WrappedArray<String>) mvField.drop(nDropFromLeft);

        return mvField;
    }

}
