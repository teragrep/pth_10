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
package com.teragrep.pth10.steps.strcat;

import com.teragrep.pth10.ast.NullValue;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class StrcatStep extends AbstractStrcatStep {

    private final NullValue nullValue;

    public StrcatStep(NullValue nullValue) {
        super();
        this.nullValue = nullValue;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        // remove non-existing columns from list of fields
        List<String> listOfFieldsWithNonExistingRemoved = removeNonExistingColumns(this.listOfFields, dataset);

        // Check for literals and fields
        Seq<Column> seqOfCols = JavaConversions.asScalaBuffer(listOfFieldsWithNonExistingRemoved.stream().map(s -> {
            if (s.startsWith("\"") && s.endsWith("\"")) {
                // Enclosed in double-quotes is a literal
                return functions.lit(new UnquotedText(new TextString(s)).read());
            }
            else {
                // Column
                return functions.col(s);
            }
        }).collect(Collectors.toList()));

        // Perform the actual operation, based on the given parameters
        if (allRequired) {
            if (seqOfCols.size() != this.numberOfSrcFieldsOriginally) {
                // A column was dropped -> return all null
                return dataset.withColumn(this.destField, functions.lit(nullValue.value()));
            }
            else {
                // A column was not dropped -> return all cols
                return dataset.withColumn(this.destField, functions.concat_ws("", seqOfCols));
            }
        }
        else {
            // AllRequired=false, no need to care for dropped columns, if any
            return dataset.withColumn(destField, functions.concat_ws("", seqOfCols));
        }
    }

    /**
     * Removes non-existing fields from the list of field names
     * 
     * @param fields list of fields
     * @return list of fields without non-existing fields
     */
    private List<String> removeNonExistingColumns(List<String> fields, Dataset<Row> dataset) {
        List<String> fieldsRemoved = new ArrayList<>();
        fields.forEach(field -> {
            if (
                Arrays.toString(dataset.columns()).contains(field) || (field.startsWith("\"") && field.endsWith("\""))
            ) {
                fieldsRemoved.add(field);
            }
        });
        return fieldsRemoved;
    }
}
