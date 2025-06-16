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

/**
 * UDF for convert command 'mstime'<br>
 * Human readable time ([MM:]SS.SSS) to epoch using given timeformat<br>
 * 
 * @author eemhu
 */
public class Mstime implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(String duration) throws Exception {
        // duration is in format [MM:]SS.SSS
        // split based on colon (':') and period ('.')

        String[] parts = duration.split("\\."); // MM:SS and SSS parts
        String[] minutesAndSeconds = parts[0].split(":"); // separate MM and SS

        long min = 0;
        long sec = 0;
        if (minutesAndSeconds.length > 1) { // if minutes present
            min = Long.valueOf(minutesAndSeconds[0]);
            sec = Long.valueOf(minutesAndSeconds[1]);
        }
        else { // no minutes, just sec and millisec
            sec = Long.valueOf(minutesAndSeconds[0]);
        }

        long ms = Long.valueOf(parts[1]);

        // add everything up to milliseconds
        ms += min * 60L * 1000L;
        ms += sec * 1000L;

        return String.valueOf(ms);
    }

}
