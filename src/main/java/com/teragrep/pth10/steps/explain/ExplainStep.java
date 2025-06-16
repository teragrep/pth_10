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
package com.teragrep.pth10.steps.explain;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.RowFactory;

import java.util.Collections;
import java.util.List;

public final class ExplainStep extends AbstractExplainStep {

    public ExplainStep() {
        super();
        // Has to be in Sequential_Only for queryExecution() (doesn't work with a streaming dataset)
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        // Build static datasets with queryExecution().simpleString() and .stringWithStats()
        Dataset<Row> rv = null;
        if (this.mode == ExplainMode.BRIEF) {
            List<Row> rowList = Collections.singletonList(RowFactory.create(dataset.queryExecution().simpleString()));
            StructType schema = new StructType(new StructField[] {
                    StructField.apply("result", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            rv = SparkSession.builder().getOrCreate().createDataFrame(rowList, schema);
        }
        else if (this.mode == ExplainMode.EXTENDED) {
            List<Row> rowList = Collections
                    .singletonList(RowFactory.create(dataset.queryExecution().stringWithStats()));
            StructType schema = new StructType(new StructField[] {
                    StructField.apply("result", DataTypes.StringType, false, new MetadataBuilder().build())
            });
            rv = SparkSession.builder().getOrCreate().createDataFrame(rowList, schema);
        }
        else {
            throw new UnsupportedOperationException("Invalid explain mode provided: " + this.mode);
        }

        return rv;
    }
}
