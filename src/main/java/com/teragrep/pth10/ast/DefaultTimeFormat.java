/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
package com.teragrep.pth10.ast;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Parser for the three default timeformats that can be used: 1. MM/dd/yyyy:HH:mm:ss 2. ISO 8601 with timezone offset,
 * e.g. 2011-12-03T10:15:30+01:00 3. ISO 8601 without offset, e.g. 2011-12-03T10:15:30 When timezone is not specified,
 * uses the system default
 */
public class DefaultTimeFormat {

    /**
     * Calculate the epoch from given string.
     * 
     * @param time The human-readable time
     * @return epoch as long
     */
    public long getEpoch(String time) {
        return this.parse(time).getTime() / 1000L;
    }

    /**
     * Parses the given human-readable time to a Date object.
     * 
     * @param time The human-readable time
     * @return Date parsed from the given string
     */
    public Date parse(String time) {
        // Try parsing all the three default timeformats
        Date date;

        int attempt = 0;
        while (true) {
            try {
                if (attempt == 0) {
                    // Use default format (MM/dd/yyyy:HH:mm:ss)
                    // Use system default timezone
                    date = this.parseDate(time, "MM/dd/yyyy:HH:mm:ss");
                }
                else if (attempt == 1) {
                    // On first fail, try ISO 8601 with timezone offset, e.g. '2011-12-03T10:15:30+01:00'
                    date = this.parseDate(time, "yyyy-MM-dd'T'HH:mm:ssXXX");
                }
                else {
                    // On second fail, try ISO 8601 without offset, e.g. '2011-12-03T10:15:30'
                    // Use system default timezone
                    date = this.parseDate(time, "yyyy-MM-dd'T'HH:mm:ss");
                }
                break;

            }
            catch (ParseException e) {
                if (attempt > 1) {
                    throw new RuntimeException("TimeQualifier conversion error: <" + time + "> can't be parsed.");
                }
            }
            finally {
                attempt++;
            }
        }

        return date;
    }

    private Date parseDate(String time, String timeFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        sdf.setLenient(false);
        return sdf.parse(time);
    }
}
