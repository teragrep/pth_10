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
package com.teragrep.pth10.ast.commands.transformstatement.timechart.span;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A time range for | timechart command's 'span=' parameter. Gets the span= parameter from the query and parses it.
 */
public final class TimeRange {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeRange.class);

    private final String duration;

    public TimeRange(String duration) {
        this.duration = duration;
    }

    public long asSeconds() {
        // incoming span-length consist of <int>[<timescale>]
        // default timescale is sec
        String timescale = "sec";
        final int numericalValue;
        final Pattern p = Pattern.compile("\\d+");
        final Matcher m = p.matcher(duration);
        if (m.lookingAt()) {
            numericalValue = Integer.parseInt(m.group());
            final String[] parts = duration.split(m.group());
            if (parts.length > 1) {
                timescale = parts[1].trim();
            }
        }
        else {
            LOGGER.error("Span length error: missing numerical value:<{}>", duration);
            throw new RuntimeException("| timechart 'span' parameter is missing a numerical value:" + duration);
        }

        long sec;
        switch (timescale) {
            case "s":
            case "sec":
            case "secs":
            case "second":
            case "seconds":
            case "S": {
                sec = numericalValue;
                break;
            }
            case "m":
            case "min":
            case "mins":
            case "minute":
            case "minutes":
            case "M": {
                sec = numericalValue * 60L;
                break;
            }
            case "h":
            case "hr":
            case "hrs":
            case "hour":
            case "hours":
            case "H": {
                sec = numericalValue * 3600L;
                break;
            }
            case "d":
            case "day":
            case "days":
            case "D": {
                sec = numericalValue * 3600L * 24;
                break;
            }
            case "w":
            case "week":
            case "weeks":
            case "W": {
                sec = numericalValue * 3600L * 24 * 7;
                break;
            }
            case "mon":
            case "month":
            case "months":
            case "MON": {
                // month is not  supported as such, it needs to be changed to seconds
                // use 30 as default month length
                sec = (long) numericalValue * 30 * 24 * 3600;
                break;
            }
            default: {
                throw new RuntimeException(
                        "| timechart 'span' parameter only accepts seconds, minutes, hours, days, weeks and months. Got '"
                                + timescale + "' instead."
                );
            }
        }

        return sec;
    }
}
