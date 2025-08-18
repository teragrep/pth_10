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
package com.teragrep.pth_10.ast;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Parser for the three default timeformats that can be used: 1. MM/dd/yyyy:HH:mm:ss 2. ISO 8601 with timezone offset,
 * e.g. 2011-12-03T10:15:30+01:00 3. ISO 8601 without offset, e.g. 2011-12-03T10:15:30 When timezone is not specified,
 * uses the system default
 */
public class DefaultTimeFormat {

    private final String[] formats;

    public DefaultTimeFormat() {
        this(new String[] {
                "MM/dd/yyyy:HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd'T'HH:mm:ssXXX",
                "yyyy-MM-dd'T'HH:mm:ss"
        });
    }

    public DefaultTimeFormat(String[] formats) {
        this.formats = formats;
    }

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
        // Try parsing all provided time formats in order
        for (final String format : formats) {
            try {
                return parseDate(time, format);
            }
            catch (ParseException ignored) {
            }
        }
        throw new RuntimeException("TimeQualifier conversion error: <" + time + "> can't be parsed.");
    }

    private Date parseDate(String time, String timeFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        sdf.setLenient(false);
        return sdf.parse(time);
    }
}
