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
package com.teragrep.pth10.steps.timechart;

import org.apache.spark.sql.*;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class TimechartStep extends AbstractTimechartStep {

    public TimechartStep() {
        super();
        this.properties.add(CommandProperty.AGGREGATE);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        if (this.getAggCols() == null || this.getAggCols().isEmpty()) {
            throw new RuntimeException("Aggregate columns not present in TimechartStep, cannot proceed");
        }

        // .agg has funky arguments; just giving a Seq of columns is no good, first arg needs to be a column
        Column firstAggCol = this.aggCols.get(0);
        Seq<Column> seqOfAggColsExceptFirst = JavaConversions
                .asScalaBuffer(this.aggCols.subList(1, this.aggCols.size()));

        List<Column> allGroupBys = new ArrayList<>();
        allGroupBys.add(this.span);
        allGroupBys.addAll(this.divByInsts.stream().map(functions::col).collect(Collectors.toList()));

        Seq<Column> seqOfAllGroupBys = JavaConversions.asScalaBuffer(allGroupBys);

        return dataset
                .groupBy(seqOfAllGroupBys)
                .agg(firstAggCol, seqOfAggColsExceptFirst)
                .drop("_time")
                .select("window.start", "*")
                .withColumnRenamed("start", "_time")
                .drop("window")
                .orderBy("_time");
    }
}
