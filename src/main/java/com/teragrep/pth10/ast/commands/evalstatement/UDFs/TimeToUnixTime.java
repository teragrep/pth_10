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

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Converts Timestamp/String object to a unix epoch, otherwise Long/Integer will be returned as-is
 */
public class TimeToUnixTime implements UDF1<Object, Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeToUnixTime.class);

    /**
     * @param time Object of type timestamp or unix time
     * @return as unix time
     * @throws Exception parsing exception
     */
    @Override
    public Long call(Object time) throws Exception {
        long unixtime = 0L;

        if (time instanceof Timestamp) {
            Timestamp ts = (Timestamp) time;
            LOGGER.debug("Time was detected as TIMESTAMP, epoch: <{}>", ts.getTime() / 1000L);
            unixtime = ts.getTime() / 1000L;
        }
        else if (time instanceof Long) {
            LOGGER.debug("Time was directly a LONG/EPOCH: <{}>", time);
            unixtime = (Long) time;
        }
        else if (time instanceof Integer) {
            LOGGER.debug("Time was an INTEGER: <{}>", time);
            unixtime = ((Integer) time).longValue();
        }
        else if (time instanceof String) {
            LOGGER.debug("Time was a STRING: <{}>", time);
            try {
                LOGGER.debug("Attempting to use as-is (epoch)");
                unixtime = Long.parseLong(((String) time));
            }
            catch (NumberFormatException nfe) {
                LOGGER.debug("Failed, attempting to parse as a ISO_ZONED_DATE_TIME");
                String timeStr = (String) time;
                LocalDateTime ldt = LocalDateTime.parse(timeStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
                ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneId.systemDefault());
                ZonedDateTime asUtc = zdt.withZoneSameInstant(ZoneOffset.UTC);
                unixtime = asUtc.toEpochSecond();
            }

        }
        else {
            throw new RuntimeException("Invalid type, cannot convert to unix time");
        }

        LOGGER.debug("Returning unix time = <{}>", unixtime);
        return unixtime;
    }
}
