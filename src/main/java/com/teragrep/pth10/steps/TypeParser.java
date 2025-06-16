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
package com.teragrep.pth10.steps;

import java.io.Serializable;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.util.List;

public class TypeParser implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Checks if the Object contains a String, Double or a Long value and returns it as a ParsedResult object.
     * 
     * @param input Object to parse
     * @return String, Double or Long wrapped in a ParsedResult
     */
    public ParsedResult parse(Object input) {
        // Default to String
        ParsedResult parsedResult = new ParsedResult(input.toString());

        // Parse the string in case a command was used that returns all data as String
        if (input instanceof String) {
            try {
                // try to parse into long
                parsedResult = new ParsedResult(Long.valueOf((String) input));
            }
            catch (NumberFormatException nfe) {
                try {
                    // try to parse into double
                    parsedResult = new ParsedResult(Double.valueOf((String) input));
                }
                catch (NumberFormatException ignored) {
                    // returns as String
                }
            }
        }
        else if (input instanceof Long) {
            parsedResult = new ParsedResult((Long) input);
        }
        else if (input instanceof WrappedArray) {
            WrappedArray<String> wr = (WrappedArray<String>) input;
            if (wr.length() == 1) {
                // reparse arrays with one item
                parsedResult = parse(wr.head());
            }
            else if (wr.isEmpty()) {
                // empty arrays will be empty string
                parsedResult = new ParsedResult("");
            }
            else {
                // arrays with more than one item will be LIST
                parsedResult = new ParsedResult(JavaConversions.bufferAsJavaList(wr.toBuffer()));
            }

        }
        else if (input instanceof List) {
            List<String> l = (List<String>) input;
            if (l.size() == 1) {
                parsedResult = new ParsedResult(String.valueOf(l.get(0)));
            }
            else if (l.isEmpty()) {
                parsedResult = new ParsedResult("");
            }
            else {
                parsedResult = new ParsedResult(l);
            }
        }
        else {
            try {
                // Turns integers into double as well
                parsedResult = new ParsedResult(Double.valueOf(input.toString()));
            }
            catch (NumberFormatException ignored) {
                // returns as String
            }
        }

        return parsedResult;
    }
}
