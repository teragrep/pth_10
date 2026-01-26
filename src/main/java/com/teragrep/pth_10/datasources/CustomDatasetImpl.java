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
package com.teragrep.pth_10.datasources;

import com.teragrep.pth_10.ast.DPLParserCatalystContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.types.StructType;
import scala.Option;

import java.util.List;
import java.util.Objects;

public final class CustomDatasetImpl implements CustomDataset {

    private final StructType schema;
    private final List<Object[]> values;
    private final DPLParserCatalystContext catCtx;

    public CustomDatasetImpl(
            final StructType schema,
            final List<Object[]> values,
            final DPLParserCatalystContext catCtx
    ) {
        this.schema = schema;
        this.values = values;
        this.catCtx = catCtx;
    }

    @Override
    public Dataset<Row> asStaticDataset() {
        return catCtx.getSparkSession().createDataFrame(new Rows(values).asList(), schema);
    }

    @Override
    public Dataset<Row> asStreamingDataset() {
        final SQLContext sqlContext = catCtx.getSparkSession().sqlContext();
        final Encoder<Row> encoder = ExpressionEncoder.apply(schema);
        final MemoryStream<Row> rowMemoryStream = new MemoryStream<>(1, sqlContext, Option.apply(1), encoder);

        rowMemoryStream.addData(new Rows(values).asSeq());

        return rowMemoryStream.toDF();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CustomDatasetImpl that = (CustomDatasetImpl) o;
        return Objects.equals(schema, that.schema) && Objects.equals(values, that.values)
                && Objects.equals(catCtx, that.catCtx);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, values, catCtx);
    }
}
