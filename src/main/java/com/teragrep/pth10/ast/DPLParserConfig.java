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

import com.teragrep.pth10.ast.time.DPLTimestampImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.Math.abs;

/**
 * Configuration for parser/UI data transfer. For instance latest/earliest values used for default span/time window
 * calculation and map which can contain additional named values.
 */
public class DPLParserConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLParserConfig.class);

    private Map<String, Object> config = new LinkedHashMap<>();

    TimeRange timeRange = TimeRange.ONE_MONTH;

    /**
     * Get named value from config map
     * 
     * @param key Name string
     * @return value
     */
    public Object get(String key) {
        return config.get(key);
    }

    /**
     * Put named value object into the map
     * 
     * @param key   name string
     * @param value as object. String/int/...
     */
    public void put(String key, Object value) {
        config.put(key, value);
    }

    /**
     * Get earliest flag which is used when calculating window ranges for different spans.
     * 
     * @return earliest as string value
     */
    public String getEarliest() {
        return (String) config.get("earliest");
    }

    /**
     * Set earliest flag with given string, If flag is relative, calculate it relative to current now-instance. Store
     * also that calculated value as epoch
     * 
     * @param earliest string value like -1h or actual timestamp
     */
    public void setEarliest(String earliest) {
        final long earliestEpoch = new DPLTimestampImpl(earliest).zonedDateTime().toEpochSecond();
        config.put("earliest", earliest);
        config.put("earliestEpoch", earliestEpoch);
    }

    /**
     * Get latest flag which is used when calculating window ranges for different spans.
     * 
     * @return latest as string value
     */
    public String getLatest() {
        return (String) config.get("latest");
    }

    /**
     * Set latest flag with given string, If flag is relative, calculate it relative to current now-instance. Store also
     * that calculated value as epoch
     * 
     * @param latest string value like -1h or actual timestamp
     */
    public void setLatest(String latest) {
        final long latestEpoch = new DPLTimestampImpl(latest).zonedDateTime().toEpochSecond();
        config.put("latest", latest);
        config.put("latestEpoch", latestEpoch);
    }

    /**
     * Use config map and calculate default time range according to it.
     * 
     * @return enum range values 10s,...,1M
     */
    public TimeRange getTimeRange() {
        TimeRange rv = TimeRange.ONE_DAY;
        long r = 0;
        // Earliest set, latest not
        if (config.get("earliest") != null && config.get("latest") == null) {
            r = System.currentTimeMillis() - (long) config.get("earliestEpoch");
        }
        else if (config.get("latest") != null && config.get("earliest") == null) {
            r = (long) config.get("latestEpoch");
        }
        else if (config.get("earliest") != null && config.get("latest") != null) {
            // Both set
            // Calculate time range according to latest-earliest
            LOGGER.info("config=<[{}]>", config);
            r = (long) config.get("latestEpoch") - (long) config.get("earliestEpoch");
        }
        if (r < 0)
            r = abs(r);
        LOGGER.info("Calculated range=<{}>", r);
        if (r <= 15 * 60) {
            rv = TimeRange.TEN_SECONDS;
        }
        else if (r <= 60 * 60) {
            rv = TimeRange.ONE_MINUTE;
        }
        else if (r <= 4 * 60 * 60) {
            rv = TimeRange.FIVE_MINUTES;
        }
        else if (r <= 24 * 60 * 60) {
            rv = TimeRange.THIRTY_MINUTES;
        }
        else if (r > 30 * 24 * 60 * 60) {
            // Default max value is 1 day
            rv = TimeRange.ONE_DAY;
        }
        return rv;
    }
}
