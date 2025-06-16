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
package com.teragrep.pth10.ast.commands.transformstatement.accum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class BatchCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollector.class);

    private final ConcurrentSkipListMap<Timestamp, List<Row>> mapOfCollected;
    private int rowsCollected;
    private int numberOfRowsToCollect;

    private StructType batchSchema = null;
    private boolean ordered = true;

    public BatchCollector() {
        this.mapOfCollected = new ConcurrentSkipListMap<>();
        this.rowsCollected = 0;
        //this.numberOfRowsToCollect = numberOfRowsToCollect;
        this.ordered = true;
    }

    public BatchCollector(boolean ordered) {
        this.mapOfCollected = new ConcurrentSkipListMap<>();
        this.rowsCollected = 0;
        //this.numberOfRowsToCollect = numberOfRowsToCollect;
        this.ordered = ordered;
    }

    public void collect(Dataset<Row> batchDF, Long batchId) {
        List<Row> rowsFromBatch = null;
        this.batchSchema = batchDF.schema();

        if (ordered) {
            rowsFromBatch = batchDF.orderBy(functions.col("_time").desc()).repartition(1).collectAsList();
        }
        else {
            rowsFromBatch = batchDF.repartition(1).collectAsList();
        }

        rowsFromBatch.forEach(row -> {
            insert(row);
        });
    }

    public StructType getBatchSchema() {
        return this.batchSchema;
    }

    public void insert(Row rowToInsert) {
        Timestamp timestampOfRow = rowToInsert.getTimestamp(0);
        rowsCollected++;

        // Key exists in map, add the rowToInsert to the List of rows for the timestamp
        if (mapOfCollected.get(timestampOfRow) != null) {
            mapOfCollected.get(timestampOfRow).add(rowToInsert);
        }
        // Key does not exist in map, add new Key (Timestamp), Value (List<Row>) pair to map
        else {
            LinkedList<Row> listOfRows = new LinkedList<Row>();
            listOfRows.add(rowToInsert);
            mapOfCollected.put(timestampOfRow, listOfRows);
        }
    }

    public void printCollected() {
        mapOfCollected.values().forEach(value -> {
            LOGGER.info(value.toString());
        });
    }

    public ArrayList<Row> toList() {
        ArrayList<Row> rv = new ArrayList<>();

        mapOfCollected.values().forEach(rowList -> {
            rv.addAll(rowList);
        });

        return rv;
    }
}
