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
package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Objects;

public final class BloomFilterForeachPartitionFunction implements ForeachPartitionFunction<Row> {

    private final FilterTypes filterTypes;
    private final LazyConnection lazyConnection;
    private final boolean overwrite;
    private final String tableName;
    private final String regex;

    public BloomFilterForeachPartitionFunction(Config config, String tableName, String regex) {
        this(new FilterTypes(config), new LazyConnection(config), tableName, regex, false);
    }

    public BloomFilterForeachPartitionFunction(Config config, String tableName, String regex, boolean overwrite) {
        this(new FilterTypes(config), new LazyConnection(config), tableName, regex, overwrite);
    }

    public BloomFilterForeachPartitionFunction(
            FilterTypes filterTypes,
            LazyConnection lazyConnection,
            String tableName,
            String regex,
            boolean overwrite
    ) {
        this.filterTypes = filterTypes;
        this.lazyConnection = lazyConnection;
        this.tableName = tableName;
        this.regex = regex;
        this.overwrite = overwrite;
    }

    @Override
    public void call(final Iterator<Row> iter) throws Exception {
        final Connection conn = lazyConnection.get();
        while (iter.hasNext()) {
            final Row row = iter.next(); // Row[partitionID, filterBytes]
            final String partition = row.getString(0);
            final byte[] filterBytes = (byte[]) row.get(1);
            final TeragrepBloomFilter tgFilter = new TeragrepBloomFilter(
                    partition,
                    filterBytes,
                    conn,
                    filterTypes,
                    tableName,
                    regex
            );
            tgFilter.saveFilter(overwrite);
            conn.commit();
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        final BloomFilterForeachPartitionFunction cast = (BloomFilterForeachPartitionFunction) o;
        return filterTypes.equals(cast.filterTypes) && lazyConnection.equals(cast.lazyConnection)
                && overwrite == cast.overwrite && tableName.equals(cast.tableName) && regex.equals(cast.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterTypes, lazyConnection, overwrite, tableName, regex);
    }
}
