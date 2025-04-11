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
import java.time.Instant;

/**
 * For using the DPL custom timeformat like Java's SimpleDateFormat. Get a Date object with parse -function for example.
 */
public final class DPLTimeFormat {

    private final String format;

    public DPLTimeFormat(String format) {
        this.format = format;
    }

    /**
     * Create a SimpleDateFormat object from the given DPL specific timeformat. Allows all sorts of parsing and
     * tampering with the time.
     * 
     * @return SimpleDateFormat created from the DPLTimeFormat
     */
    public SimpleDateFormat createSimpleDateFormat() {
        return new SimpleDateFormat(convertDplTimeFormatToJava(this.format));
    }

    /**
     * Parses the time string and converts it to an unic epoch long. Uses the system timezone as default if timezone is
     * not specified in the pattern (given in the constructor). For setting a timezone later on (or any other operation)
     * you will have to use createSimpleDateFormat function.
     * 
     * @param dplTime Time represented with the pattern
     * @return Unix Epoch
     * @throws ParseException when dplTime doesn't have the correct format
     */
    public Instant instantOf(String dplTime) throws ParseException {
        return createSimpleDateFormat().parse(dplTime).toInstant();
    }

    // replace dpl time units with java-compatible time units
    private String convertDplTimeFormatToJava(String dplTf) {
        dplTf = new UnquotedText(new TextString(dplTf)).read();
        return dplTf
                // Date
                .replaceAll("%F", "y-MM-dd") // ISO 8601 %Y-%m-%d
                .replaceAll("%y", "yy") // year without century (00-99)
                .replaceAll("%Y", "y") // full year
                .replaceAll("%m", "MM") // month 1-12
                .replaceAll("%d", "dd") // day 1-31
                .replaceAll("%b", "MMM") // abbrv. month name
                .replaceAll("%B", "MMMM") // full month name
                .replaceAll("%A", "EE") // full weekday name, e.g. "sunday"
                .replaceAll("%a", "E") // abbrv. weekday name, e.g. "Sun"
                .replaceAll("%j", "D") // day of year, 001-366
                .replaceAll("%w", "F") // weekday as decimal 0=sun 6=sat
                // Time
                .replaceAll("%H", "HH") // hour 0-23
                .replaceAll("%k", "H") // hour without leading zeroes
                .replaceAll("%M", "mm") // minute 0-59
                .replaceAll("%S", "ss") // second 0-59
                .replaceAll("%I", "hh") // hour 1-12
                .replaceAll("%p", "a") // am/pm
                .replaceAll("%T", "HH:mm:ss") // hour:min:sec
                .replaceAll("%f", "SSS") // microsecs
                // Time zone
                .replaceAll("%Z", "zz") // timezone abbreviation
                .replaceAll("%z", "X") // timezone offset +00:00
                // Other
                .replaceAll("%%", "%"); // percent sign
    }
}
