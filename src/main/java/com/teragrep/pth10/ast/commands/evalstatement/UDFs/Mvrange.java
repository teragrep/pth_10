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

import com.teragrep.pth10.ast.time.RelativeTimestamp;
import org.apache.spark.sql.api.java.UDF3;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * UDF used for command mvrange(start, end, step)<br>
 * end is excluded from the resulting field, step can be an integer or a timespan (string)<br>
 * While using a timespan, if the increment causes the time to increment past the end time, it will not be included in
 * the resulting field.<br>
 * 
 * @author eemhu
 */
public class Mvrange implements UDF3<Integer, Integer, Object, List<String>>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public List<String> call(Integer start, Integer end, Object stepObj) throws Exception {
        Integer step = null;
        String stepStr = null;

        // Take step in as an Object, and check if it is String or Integer
        // This allows the use of a single class for different input argument types,
        // instead of making multiples of Mvrange for Integer / String.
        if (stepObj instanceof Long) {
            step = ((Long) stepObj).intValue();
        }
        else if (stepObj instanceof Integer) {
            step = ((Integer) stepObj);
        }
        else if (stepObj instanceof String) {
            stepStr = ((String) stepObj);
        }
        else {
            throw new RuntimeException(
                    "Mvrange: Step increment argument could not be interpreted into a valid argument. Make sure the argument is an integer or a timespan."
            );
        }

        List<String> rv = new ArrayList<>();
        // Numeric increment step
        if (step != null) {
            for (int i = start; i < end; i = i + step) {
                rv.add(String.valueOf(i));
            }
        }
        // Unix time increment
        else {
            // Add start to mv field
            long time = start;
            rv.add(String.valueOf(time));

            // Go until incremented past end
            while (time < end) {
                final ZonedDateTime startTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneId.of("UTC"));
                final RelativeTimestamp relativeTimestamp = new RelativeTimestamp("+" + stepStr, startTime);
                time = relativeTimestamp.zonedDateTime().toEpochSecond();

                // If time went past end, stop incrementing and don't add to mv field
                if (time > end) {
                    break;
                }

                rv.add(String.valueOf(time));
            }
        }

        return rv;
    }

}
