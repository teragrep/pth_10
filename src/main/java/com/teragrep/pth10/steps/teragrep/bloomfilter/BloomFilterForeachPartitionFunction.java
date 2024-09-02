/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022, 2023  Suomen Kanuuna Oy
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

package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.util.Iterator;

public class BloomFilterForeachPartitionFunction implements ForeachPartitionFunction<Row> {

    private final FilterSizes filterSizes;
    private final LazyConnection lazyConnection;
    private final boolean overwrite;

    public BloomFilterForeachPartitionFunction(Config config, boolean overwrite) {
        this.filterSizes = new FilterSizes(config);
        this.lazyConnection = new LazyConnection(config);
        this.overwrite = overwrite;
    }
    public BloomFilterForeachPartitionFunction(Config config) {
        this.filterSizes = new FilterSizes(config);
        this.lazyConnection = new LazyConnection(config);
        this.overwrite = false;
    }

    @Override
    public void call(Iterator<Row> iter) throws Exception {

        Connection conn = lazyConnection.get();

        while (iter.hasNext()) {
            Row row = iter.next(); // Row[partitionID, filterBytes]

            String partition = row.getString(0);
            byte[] filterBytes = (byte[]) row.get(1);

            TeragrepBloomFilter filter =
                    new TeragrepBloomFilter(partition, filterBytes, conn, filterSizes);

            filter.saveFilter(overwrite);
            conn.commit();
        }
    }
}
