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
package com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses;

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.CurrentTimestamp;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.CurrentTimestampStub;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Java Bean compliant class with helper methods used in EarliestLatestAggregator.java
 * 
 * @author eemhu
 */
public final class EarliestLatestBuffer implements Serializable {

    private static final long serialVersionUID = 1L;
    private final CurrentTimestamp earliestTimestamp;
    private final CurrentTimestamp latestTimestamp;
    private final Row earliestRow;
    private final Row latestRow;
    private final String colName;

    public EarliestLatestBuffer(final String colName) {
        this.colName = colName;
        this.earliestTimestamp = new CurrentTimestampStub();
        this.latestTimestamp = new CurrentTimestampStub();
        this.earliestRow = Row.empty();
        this.latestRow = Row.empty();
    }

    public EarliestLatestBuffer(
            final String colName,
            final CurrentTimestamp earliestTimestamp,
            final CurrentTimestamp latestTimestamp,
            final Row earliestRow,
            final Row latestRow
    ) {
        this.colName = colName;
        this.earliestTimestamp = earliestTimestamp;
        this.latestTimestamp = latestTimestamp;
        this.earliestRow = earliestRow;
        this.latestRow = latestRow;
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
        if (earliestTimestamp.isEmpty()) {
            return "";
        }
        return String.valueOf(earliestTimestamp.timestamp().getTime() / 1000L);
    }

    /**
     * Gets the latest unix time
     * 
     * @return field time as unix epoch
     */
    public String latest_time() {
        if (latestTimestamp.isEmpty()) {
            return "";
        }
        return String.valueOf(latestTimestamp.timestamp().getTime() / 1000L);
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

    public Row earliestRow() {
        return earliestRow;
    }

    public Row latestRow() {
        return latestRow;
    }

    public CurrentTimestamp earliestTimestamp() {
        return earliestTimestamp;
    }

    public CurrentTimestamp latestTimestamp() {
        return latestTimestamp;
    }

    public String colName() {
        return colName;
    }
}
