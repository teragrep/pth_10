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

package com.teragrep.pth10.steps.stats;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class StatsStep extends AbstractStatsStep{
    public StatsStep(Dataset<Row> dataset) {
        super(dataset);
    }

    @Override
    public Dataset<Row> get() {
        if (this.dataset == null) {
            return null;
        }

        // Check that at least one aggregation expression is present
        if (this.listOfAggregationExpressions != null && !this.listOfAggregationExpressions.isEmpty()) {
            Column mainExpr = this.listOfAggregationExpressions.get(0);
            Seq<Column> seqOfAggs = JavaConversions.asScalaBuffer(
                    this.listOfAggregationExpressions.subList(1, this.listOfAggregationExpressions.size()));

            // Check for any group by expressions
            if (this.listOfGroupBys != null && !this.listOfGroupBys.isEmpty()) {
                Seq<Column> seqOfGroupBys = JavaConversions.asScalaBuffer(this.listOfGroupBys);
                return this.dataset.groupBy(seqOfGroupBys).agg(mainExpr, seqOfAggs);
            }
            else {
                // No group by, just perform a direct aggregation
                return this.dataset.agg(mainExpr, seqOfAggs);
            }
        }

        // throw exception if everything was null
        throw new RuntimeException("StatsStep did not receive the necessary aggregation columns!");
    }
}
