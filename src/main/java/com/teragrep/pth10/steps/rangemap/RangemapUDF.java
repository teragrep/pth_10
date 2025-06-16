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
package com.teragrep.pth10.steps.rangemap;

import com.teragrep.pth10.steps.ParsedResult;
import com.teragrep.pth10.steps.TypeParser;
import org.apache.spark.sql.api.java.UDF3;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.util.*;

public class RangemapUDF
        implements UDF3<Object, String, scala.collection.immutable.Map<String, WrappedArray<String>>, List<String>> {

    @Override
    public List<String> call(
            Object inputNumber,
            String defaultValue,
            scala.collection.immutable.Map<String, WrappedArray<String>> attributeRangeMap
    ) throws Exception {
        // parse numbers
        List<Double> inputs = getAllNumbersFromInput(new ArrayList<>(), inputNumber);
        // when all strings, then use defaultValue. Otherwise, skip strings.
        if (inputs.isEmpty()) {
            return Collections.singletonList(defaultValue);
        }

        // get map contents
        Set<String> rv = new HashSet<>();
        Iterator<Tuple2<String, WrappedArray<String>>> iterator = attributeRangeMap.toIterator();
        while (iterator.hasNext()) {
            Tuple2<String, WrappedArray<String>> tuple = iterator.next();

            // get key-value pair
            final String key = tuple._1();
            final WrappedArray<String> value = tuple._2();
            // range min and max values
            final double min = Double.parseDouble(value.head());
            final double max = Double.parseDouble(value.last());

            for (Double input : inputs) {
                // range is start and end inclusive
                if (input >= min && input <= max) {
                    rv.add(key);
                }
            }
        }

        // if rv is empty it means there were no matches for range, return default
        return rv.isEmpty() ? Collections.singletonList(defaultValue) : new ArrayList<>(rv);
    }

    private List<Double> getAllNumbersFromInput(List<Double> targetList, Object inputNumber) {
        // parse object
        TypeParser tp = new TypeParser();
        ParsedResult parsedInputNumber = tp.parse(inputNumber);
        double parsedNumber;
        // get numbers, skip strings, go through lists
        if (parsedInputNumber.getType() != ParsedResult.Type.STRING) {
            if (parsedInputNumber.getType() == ParsedResult.Type.LONG) {
                parsedNumber = parsedInputNumber.getLong();
                targetList.add(parsedNumber);
            }
            else if (parsedInputNumber.getType() == ParsedResult.Type.DOUBLE) {
                parsedNumber = parsedInputNumber.getDouble();
                targetList.add(parsedNumber);
            }
            else if (parsedInputNumber.getType() == ParsedResult.Type.LIST) {
                for (Object item : parsedInputNumber.getList()) {
                    targetList.addAll(getAllNumbersFromInput(targetList, item));
                }
            }
        }
        return targetList;
    }
}
