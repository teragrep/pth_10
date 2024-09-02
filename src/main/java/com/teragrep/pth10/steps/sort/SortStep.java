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

package com.teragrep.pth10.steps.sort;

import com.teragrep.functions.dpf_02.BatchCollect;
import com.teragrep.functions.dpf_02.SortByClause;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class SortStep extends AbstractSortStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortStep.class);
    public SortStep(DPLParserCatalystContext catCtx, List<SortByClause> listOfSortByClauses, int limit, boolean desc) {
        super();
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
        this.properties.add(CommandProperty.USES_INTERNAL_BATCHCOLLECT);

        this.catCtx = catCtx;
        this.listOfSortByClauses = listOfSortByClauses;
        this.limit = limit;
        this.desc = desc;
        this.sortingBatchCollect = new BatchCollect(null, limit, listOfSortByClauses);
    }


    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        if (this.aggregatesUsedBefore) {
            LOGGER.info("Aggregates used: using regular sorting");
            return aggregatedSort(dataset);
        } else {
            LOGGER.info("Aggregates not used: using BatchCollect to sort");
            return sort(dataset);
        }
    }

    /**
     * Performs a sort on a unsorted dataframe, using a RowComparator.<br>
     * Collects all rows of the current batch to the driver.
     * @param unsortedDs dataframe to sort
     * @return sorted dataframe
     */
    private Dataset<Row> sort(Dataset<Row> unsortedDs) {
        // sort command sorting for streaming dataset
        if (this.listOfSortByClauses != null && !this.listOfSortByClauses.isEmpty()) {
            return this.sortingBatchCollect.call(unsortedDs, 0L, false);
        }
        throw new RuntimeException("SortBy clauses were empty! Cannot perform sorting");
    }

    private Dataset<Row> aggregatedSort(Dataset<Row> unsortedDs) {
        AggregatedSort aggSort = new AggregatedSort(this.limit, this.listOfSortByClauses);
        return aggSort.sort(unsortedDs);
    }
}
