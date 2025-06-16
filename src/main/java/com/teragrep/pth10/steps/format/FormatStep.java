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
package com.teragrep.pth10.steps.format;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public final class FormatStep extends AbstractFormatStep implements Serializable {

    // TODO: Implement emptyStr (seems to be missing from parser)

    public FormatStep() {
        super();
        this.properties.add(CommandProperty.AGGREGATE);
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        // make variables that are in mapFunction final. Otherwise, they will be the initial default values.
        final String colPrefix = this.colPrefix;
        final String colSep = this.colSep;
        final String colSuffix = this.colSuffix;
        final String mvSep = this.mvSep;

        Dataset<Row> mappedDs = dataset;

        // maxResults=0 does not limit and maxResults<0 is invalid
        if (maxResults > 0) {
            mappedDs = mappedDs.limit(this.maxResults);
        }
        else if (maxResults < 0) {
            throw new IllegalArgumentException("Expected a non-negative integer value for 'maxresults' parameter.");
        }

        mappedDs = mappedDs.map((MapFunction<Row, Row>) r -> {
            final StringBuilder strBuilder = new StringBuilder();
            // '( '
            strBuilder.append(colPrefix);
            strBuilder.append(' ');
            for (int j = 0; j < r.schema().length(); j++) {
                if (
                    r.schema().fields()[j]
                            .dataType()
                            .typeName()
                            .equals(DataTypes.createArrayType(DataTypes.StringType).typeName())
                ) {
                    // MV field
                    // ( col="value1" OR col="value2" OR col="value3" )
                    List<Object> mvField = r.getList(j);
                    // '( '
                    strBuilder.append("( ");
                    for (int k = 0; k < mvField.size(); k++) {
                        // 'col="value1" OR '
                        strBuilder.append(r.schema().fields()[j].name());
                        strBuilder.append("=\"");
                        strBuilder.append(mvField.get(k));
                        strBuilder.append("\"");
                        if (k != mvField.size() - 1) {
                            // Do not append ' OR ' on last mvField cell
                            strBuilder.append(" ".concat(mvSep).concat(" "));
                        }
                    }
                    // ' ) '
                    strBuilder.append(" ) ");
                }
                else {
                    // 'col="value"'
                    strBuilder.append(r.schema().fields()[j].name());
                    strBuilder.append("=\"");
                    strBuilder.append(r.getAs(r.fieldIndex(r.schema().fields()[j].name())).toString());
                    strBuilder.append("\"");
                    if (j != r.schema().fields().length - 1) {
                        // ' AND '
                        strBuilder.append(' ');
                        strBuilder.append(colSep);
                    }
                    strBuilder.append(' ');
                }

            }

            // ') '
            strBuilder.append(colSuffix);
            strBuilder.append(' ');

            return RowFactory.create(strBuilder.toString());
        }, RowEncoder.apply(new StructType(new StructField[] {
                StructField.apply("search", DataTypes.StringType, false, new MetadataBuilder().build())
        })));

        Seq<Column> concatRows = JavaConversions
                .asScalaBuffer(Arrays.asList(functions.lit(this.rowPrefix.concat(" ")), // '( '
                        functions.concat_ws(this.rowSep.concat(" "), // 'OR '
                                functions.collect_list("search")
                        ), // cols
                        functions.lit(this.rowSuffix)
                )); // ')'

        return mappedDs.agg(functions.concat(concatRows).as("search"));
    }
}
