/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import static com.teragrep.pth10.ast.Util.stripQuotes;

/**
 * Utilities to convert string to unix epoch and dpl to java time format
 */
public class TimestampToEpochConversion {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampToEpochConversion.class);

    public static long unixEpochFromString(String timevalue, String tf) {
        long rv = 0;
        // Remove '"' around string if needed
        timevalue = stripQuotes(timevalue);
        try {
            if (tf != null && !tf.isEmpty()) {
                if ("%s".equals(tf)) {
                    // unix-epoch string so
                    rv = Long.parseLong(timevalue);
                } else {
                    String convertedFormat = convertDplTimeFormatToJava(tf);

                    LOGGER.info("converted = " + convertedFormat);
                    LocalDateTime time = LocalDateTime.parse(timevalue, DateTimeFormatter.ofPattern(convertedFormat));
                    final ZonedDateTime zonedtime = ZonedDateTime.of(time, ZoneId.systemDefault()); // .atZone(fromZone);
                    final ZonedDateTime converted = zonedtime.withZoneSameInstant(ZoneOffset.UTC);
                    rv = converted.toEpochSecond();

                    //LOGGER.info("LocalDatetime="+time+ " TZ="+ZoneId.systemDefault());
                    //LOGGER.info("Datetime(UTC)="+converted+" timestamp="+rv);
                }
            } else {
                int attempt = 0;
                while (attempt < 2) {
                    try {
                        if (attempt == 0){
                            // Use default format (MM/dd/yyyy:HH:mm:ss)
                            rv = timeStringToEpoch(timevalue, DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss"));
                        }
                        else {
                            // On fail, try ISO 8601. e.g. '2011-12-03T10:15:30+01:00'
                            rv = timeStringToEpoch(timevalue, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                        }
                        break;

                    } catch (DateTimeParseException dtpe){
                        //dtpe.printStackTrace();
                        if (attempt != 0) {
                            throw new RuntimeException("TimeQualifier conversion error: <" + timevalue + "> can't be parsed.");
                        }
                    }
                    finally {
                        attempt++;
                    }
                }
            }
        } catch (NumberFormatException exn) {
            exn.printStackTrace();
            throw new RuntimeException("TimeQualifier conversion error: <" + timevalue + "> can't be parsed.");
        }
        return rv;
    }

    private static long timeStringToEpoch(final String timeStr, final DateTimeFormatter dtFormatter) throws DateTimeParseException {
        long epoch;

        try {
            // try parsing as ZonedDateTime
            epoch = ZonedDateTime.parse(timeStr, dtFormatter).toEpochSecond();
        }
        catch (DateTimeParseException zonedDtpe) {
            // zonedDateTime parse failed, try LocalDateTime
            epoch = LocalDateTime.parse(timeStr, dtFormatter).atZone(ZoneId.systemDefault()).toEpochSecond();
            // if this throws, catch it in timeStringToEpoch() call
        }

        return epoch;
    }

    // replace dpl time units with java-compatible time units
    public static String convertDplTimeFormatToJava(String dplTf) {
        return stripQuotes(dplTf)
                // Date
                .replaceAll("%F", "y-MM-dd")    // ISO 8601 %Y-%m-%d
                .replaceAll("%y", "yy")            // year without century (00-99)
                .replaceAll("%Y", "y")             // full year
                .replaceAll("%m", "MM")         // month 1-12
                .replaceAll("%d", "dd")         // day 1-31
                .replaceAll("%b", "MMM")         // abbrv. month name
                .replaceAll("%B", "MMMM")         // full month name
                .replaceAll("%A", "EE")         // full weekday name, e.g. "sunday"
                .replaceAll("%a", "E")             // abbrv. weekday name, e.g. "Sun"
                .replaceAll("%j", "D")             // day of year, 001-366
                .replaceAll("%w", "F")             // weekday as decimal 0=sun 6=sat
                // Time
                .replaceAll("%H", "HH")         // hour 0-23
                .replaceAll("%k", "H")             // hour without leading zeroes
                .replaceAll("%M", "mm")         // minute 0-59
                .replaceAll("%S", "ss")         // second 0-59
                .replaceAll("%I", "hh")         // hour 1-12
                .replaceAll("%p", "a")             // am/pm
                .replaceAll("%T", "HH:mm:ss")     // hour:min:sec
                .replaceAll("%f", "SSS")         // microsecs
                // Time zone
                .replaceAll("%Z", "zz")         // timezone abbreviation
                .replaceAll("%z", "XXX")         // timezone offset +00:00
                // Other
                .replaceAll("%%", "%");         // percent sign
    }

}
