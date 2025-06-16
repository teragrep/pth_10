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

import com.teragrep.pth10.steps.ParsedResult;
import com.teragrep.pth10.steps.TypeParser;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class NumericColumnSum implements Serializable {

    private static final long serialVersionUID = 1L;
    private final AddtotalsIntermediateState currentState;

    public NumericColumnSum() {
        this.currentState = new AddtotalsIntermediateState();
    }

    public List<Row> process(Iterator<Row> events) {
        // Perform the cumulative sum aggregation
        List<Row> rv = new ArrayList<>();
        int rowLength = -1;

        while (events.hasNext()) {
            final Row r = events.next();
            if (rowLength == -1) {
                rowLength = r.length();
            }

            for (int i = 0; i < r.length(); i++) {
                // Get sourceField content as string
                Object rowItem = r.get(i);
                final String valueAsString;
                if (rowItem == null) {
                    valueAsString = "";
                }
                else {
                    valueAsString = rowItem.toString();
                }
                // Parse to LONG or DOUBLE. Others will be STRING and skipped.
                TypeParser typeParser = new TypeParser();
                final ParsedResult parsedResult = typeParser.parse(valueAsString);
                if (parsedResult.getType().equals(ParsedResult.Type.LONG)) {
                    // got long, accumulate
                    currentState.accumulate(i, parsedResult.getLong());
                }
                else if (parsedResult.getType().equals(ParsedResult.Type.DOUBLE)) {
                    // got double, accumulate
                    currentState.accumulate(i, parsedResult.getDouble());
                }
            }
        }

        // add final row/event with accumulated values
        Object[] finalRowItems = new Object[rowLength];
        Arrays.fill(finalRowItems, "");

        for (int i = 0; i < rowLength; i++) {
            if (currentState.exists(i)) {
                finalRowItems[i] = currentState.asString(i);
            }
        }
        rv.add(RowFactory.create(finalRowItems));

        // Return row with accumulated values
        return rv;
    }
}
