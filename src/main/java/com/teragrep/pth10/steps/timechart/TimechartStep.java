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
package com.teragrep.pth10.steps.timechart;

import com.teragrep.pth10.steps.AbstractStep;
import org.apache.spark.sql.*;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TimechartStep extends AbstractStep {

    private final List<Column> aggCols;
    private final List<String> divByInsts;
    private final Column span;

    public TimechartStep(final List<Column> aggCols, final List<String> divByInsts, final Column span) {
        super();
        this.aggCols = aggCols;
        this.divByInsts = divByInsts;
        this.span = span;
        this.properties.add(CommandProperty.AGGREGATE);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        if (aggCols.isEmpty()) {
            throw new RuntimeException("Aggregate columns not present in TimechartStep, cannot proceed");
        }

        // .agg has funky arguments; just giving a Seq of columns is no good, first arg needs to be a column
        Column firstAggCol = aggCols.get(0);
        Seq<Column> seqOfAggColsExceptFirst = JavaConversions.asScalaBuffer(aggCols.subList(1, aggCols.size()));

        List<Column> allGroupBys = new ArrayList<>();
        allGroupBys.add(span);
        allGroupBys.addAll(divByInsts.stream().map(functions::col).collect(Collectors.toList()));

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

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TimechartStep that = (TimechartStep) o;
        // Column.equals() doesn't work, using String representation instead to see that the same operations are done on both
        return Objects.equals(aggCols.toString(), that.aggCols.toString())
                && Objects.equals(divByInsts, that.divByInsts) && Objects.equals(span.toString(), that.span.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggCols, divByInsts, span);
    }
}
