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
package com.teragrep.pth10.ast.commands.transformstatement.convert;

import org.apache.spark.sql.api.java.UDF1;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Positive int or float to kilobytes.<br>
 * Default is kb to kb, can be k, m or g
 * 
 * @author eemhu
 */
public class Memk implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(String input) throws Exception {
        // (\d+(.\d+)?(k|m|g)?)
        Matcher m = Pattern.compile("\\d+(.\\d+)?").matcher(input);

        if (!m.find()) {
            throw new RuntimeException(
                    "Invalid value given for function memk(). Expected: Positive integer or float, with an optional unit k, m or g. Instead got: "
                            + input
            );
        }

        String numberPart = m.group();
        float number = Float.parseFloat(numberPart);
        String unit = input.substring(numberPart.length());

        if (unit.equalsIgnoreCase("k")) {
            return String.valueOf(number);
        }
        else if (unit.equalsIgnoreCase("m")) {
            return String.valueOf(number * 1024f);
        }
        else if (unit.equalsIgnoreCase("g")) {
            return String.valueOf(number * 1024f * 1024f);
        }
        else {
            // invalid unit, default to "k"
            return String.valueOf(number);
        }
    }

}
