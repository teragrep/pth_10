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
package com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses;

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.CurrentTimestamp;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.CurrentTimestampImpl;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.CurrentTimestampStub;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Java Bean compliant class with helper methods used in EarliestLatestAggregator.java
 * 
 * @author eemhu
 */
public class EarliestLatestBuffer implements Serializable {

    private static final long serialVersionUID = 1L;
    private CurrentTimestamp earliest = new CurrentTimestampStub();
    private CurrentTimestamp latest = new CurrentTimestampStub();
    private Row earliestRow = Row.empty();
    private Row latestRow = Row.empty();
    private final String colName;

    public EarliestLatestBuffer(final String colName) {
        this.colName = colName;
    }

    /**
     * Merge two buffers.
     * 
     * @param other buffer
     */
    public void merge(EarliestLatestBuffer other) {
        if (this.earliest.isEmpty()) {
            this.earliest = other.earliest;
            this.earliestRow = other.earliestRow;
        }

        if (this.latest.isEmpty()) {
            this.latest = other.latest;
            this.latestRow = other.latestRow;
        }

        if (!other.earliest.isEmpty() && other.earliest.isBefore(this.earliest)) {
            this.earliest = other.earliest;
            this.earliestRow = other.earliestRow;
        }

        if (!other.latest.isEmpty() && other.latest.isAfter(this.latest)) {
            this.latest = other.latest;
            this.latestRow = other.latestRow;
        }
    }

    /**
     * Add Time, Data pair
     * 
     * @param time key
     * @param data value
     */
    public void add(Timestamp time, Row data) {
        CurrentTimestamp currentTimestamp = new CurrentTimestampImpl(time);
        if (this.earliest.isEmpty() || currentTimestamp.isBefore(this.earliest)) {
            this.earliest = currentTimestamp;
            this.earliestRow = data;
        }

        if (this.latest.isEmpty() || currentTimestamp.isAfter(this.latest)) {
            this.latest = currentTimestamp;
            this.latestRow = data;
        }
    }

    /**
     * Gets the earliest field value
     * 
     * @return field value as string
     */
    public String earliest() {
        if (earliestRow.size() == 0) {
            return "";
        }
        return earliestRow.get(earliestRow.fieldIndex(colName)).toString();
    }

    /**
     * Gets the latest field value
     * 
     * @return field value as string
     */
    public String latest() {
        if (latestRow.size() == 0) {
            return "";
        }
        return latestRow.get(latestRow.fieldIndex(colName)).toString();
    }

    /**
     * Gets the earliest unix time
     * 
     * @return field time as unix epoch
     */
    public String earliest_time() {
        if (earliest.isEmpty()) {
            return "";
        }
        return String.valueOf(earliest.timestamp().getTime() / 1000L);
    }

    /**
     * Gets the latest unix time
     * 
     * @return field time as unix epoch
     */
    public String latest_time() {
        if (latest.isEmpty()) {
            return "";
        }
        return String.valueOf(latest.timestamp().getTime() / 1000L);
    }

    /**
     * Calculates the rate<br>
     * <pre>rate = latest - earliest / latest_time - earliest_time</pre> latest and earliest must be numerical<br>
     * latest_time != earliest_time<br>
     * 
     * @return rate as double
     */
    public Double rate() {
        String earliestEntry = earliest();
        String latestEntry = latest();
        if (earliestEntry.isEmpty() || latestEntry.isEmpty()) {
            throw new IllegalStateException("Could not get earliest / latest entry from data!");
        }

        // get earliest and latest values - must be numerical!
        long earliest = Long.parseLong(earliestEntry);
        long latest = Long.parseLong(latestEntry);

        // get earliest and latest time
        long earliest_time = Long.parseLong(earliest_time());
        long latest_time = Long.parseLong(latest_time());

        if (earliest_time == latest_time) {
            throw new IllegalStateException("Earliest time was the same as the latest time! Can't calculate rate.");
        }

        // rate = latest - earliest / latest_time - earliest_time
        double dividend = (double) (latest - earliest);
        double divisor = (double) (latest_time - earliest_time);
        double rate = dividend / divisor;

        return rate;
    }
}
