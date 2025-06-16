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
package com.teragrep.pth10.ast.commands.aggregate.UDAFs;

import com.teragrep.pth10.ast.commands.aggregate.UDAFs.BufferClasses.EarliestLatestBuffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;

/**
 * Used for rate() function
 */
public class EarliestLatestAggregator_Double extends EarliestLatestAggregator<Double> implements Serializable {

    private final AggregatorMode.EarliestLatestAggregatorMode mode; // 0=earliest, 1=latest, 2=earliest_time, 3=latest_time, 4=rate
    private static final long serialVersionUID = 1L;

    /**
     * Initialize with column and mode
     * 
     * @param colName column name
     * @param mode    aggregator mode
     */
    public EarliestLatestAggregator_Double(java.lang.String colName, AggregatorMode.EarliestLatestAggregatorMode mode) {
        super(colName);
        this.mode = mode;
    }

    /**
     * Output encoder
     * 
     * @return double encoder
     */
    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }

    /**
     * Return the rate
     * 
     * @param buffer buffer
     * @return rate as double
     */
    @Override
    public Double finish(EarliestLatestBuffer buffer) {
        switch (this.mode) {
            case RATE: // rate
                return buffer.rate();
            default: // shouldn't happen, throw Exception
                throw new UnsupportedOperationException("EarliestLatestAggregator was called with unsupported mode");
        }
    }

}
