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

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * UDF to used to calculate eval min/max functions. The isMin boolean is used to choose the function.
 */
public class MinMax implements UDF2<WrappedArray<Object>, Boolean, String> {

    @Override
    public String call(WrappedArray<Object> items, Boolean isMin) throws Exception {
        Iterator<Object> it = items.iterator();

        // check for different types
        // if string is present use lexicographical sorting, otherwise use normal number ordering
        DataType outputType = DataTypes.LongType;
        while (it.hasNext()) {
            Object current = it.next();

            if (current instanceof String) {
                try {
                    Long.valueOf((String) current);
                }
                catch (NumberFormatException nfe) {
                    try {
                        Double.valueOf((String) current);
                        outputType = DataTypes.DoubleType;
                    }
                    catch (NumberFormatException nfe2) {
                        outputType = DataTypes.StringType;
                        break;
                    }
                }
            }
            else if (current instanceof Double || current instanceof Float) {
                outputType = DataTypes.DoubleType;
            }
        }

        // perform min/max operation based on data type and selected function
        List<Object> javaList = JavaConversions.mutableSeqAsJavaList(items);
        Optional<Object> result;
        if (isMin) {
            if (outputType.equals(DataTypes.StringType)) {
                result = javaList.stream().min(Comparator.comparing(Object::toString));
            }
            else if (outputType.equals(DataTypes.DoubleType)) {
                result = javaList.stream().min(Comparator.comparing(a -> Double.valueOf(a.toString())));
            }
            else {
                result = javaList.stream().min(Comparator.comparing(a -> Long.valueOf(a.toString())));
            }
        }
        else {
            if (outputType.equals(DataTypes.StringType)) {
                result = javaList.stream().max(Comparator.comparing(Object::toString));
            }
            else if (outputType.equals(DataTypes.DoubleType)) {
                result = javaList.stream().max(Comparator.comparing(a -> Double.valueOf(a.toString())));
            }
            else {
                result = javaList.stream().max(Comparator.comparing(a -> Long.valueOf(a.toString())));
            }
        }

        // return empty if min/max is not found
        if (result.isPresent()) {
            return result.get().toString();
        }
        else {
            return "";
        }
    }
}
