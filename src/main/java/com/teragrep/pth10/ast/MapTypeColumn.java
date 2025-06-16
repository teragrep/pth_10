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
package com.teragrep.pth10.ast;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Represents a Column that is of MapType
 */
public class MapTypeColumn {

    private Dataset<Row> dataset;
    private String columnName; // name of the column that is of MapType
    private DPLParserCatalystContext catCtx;

    public MapTypeColumn(Dataset<Row> dataset, String columnName, DPLParserCatalystContext catCtx) {
        this.dataset = dataset;
        this.columnName = columnName;
        this.catCtx = catCtx;
    }

    /**
     * Extracts the keys from the Map of the column.
     * 
     * @return Keys as a Set of Strings
     */
    public Set<String> getKeys() throws StreamingQueryException {
        Set<String> keys;
        if (dataset.isStreaming()) {
            keys = this.getKeysParallel();
        }
        else {
            keys = this.getKeysSequential();
        }
        return keys;
    }

    // Extract the keys from a non-streaming dataset
    private Set<String> getKeysSequential() {
        final Set<String> keys = new HashSet<>();

        dataset
                .select(functions.explode(functions.map_keys(functions.col(this.columnName))))
                .collectAsList()
                .forEach(r -> keys.add(r.getString(0)));

        return keys;
    }

    // Extract the keys from a streaming dataset
    private Set<String> getKeysParallel() throws StreamingQueryException {
        // set of keys in map
        final Set<String> keys = new HashSet<>();

        if (dataset.isStreaming()) { // parallel mode
            final String id = UUID.randomUUID().toString();
            final String name = "keys_" + id;
            DataStreamWriter<Row> writer = dataset
                    .select(functions.explode(functions.map_keys(functions.col(this.columnName))))
                    .writeStream()
                    .foreachBatch((batchDs, batchId) -> {
                        // Get all the map's keys
                        // e.g. key->value; key2->value2 ==> key, key2
                        batchDs.collectAsList().forEach(r -> keys.add(r.getString(0)));
                    });

            StreamingQuery sq = this.catCtx.getInternalStreamingQueryListener().registerQuery(name, writer);
            sq.awaitTermination();

        }

        return keys;
    }
}
