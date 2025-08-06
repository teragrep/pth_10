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
package com.teragrep.pth10.steps.addtotals;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.commands.transformstatement.addtotals.AddtotalsUDF;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class AddtotalsStep extends AbstractStep implements Serializable {

    private final DPLParserCatalystContext catCtx;
    public final boolean row;
    public final boolean col;
    public final String fieldName;
    public final String labelField;
    public final String label;
    public final List<String> fieldList;
    private Dataset<Row> lastRow;
    private final NumericColumnSum numericColumnSum;

    public AddtotalsStep(
            DPLParserCatalystContext catCtx,
            boolean row,
            boolean col,
            String fieldName,
            String labelField,
            String label,
            List<String> fieldList
    ) {
        super();
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
        this.properties.add(CommandProperty.POST_BATCHCOLLECT);
        this.catCtx = catCtx;
        // basic params
        this.row = row; // display row total of all numeric columns
        this.col = col; // display column total of all numeric rows
        this.fieldList = fieldList; // fields to process
        // col
        this.labelField = labelField; // for column name for label (Empty string = Do not create)
        this.label = label; // for labelField content, before fieldName or as last if row=false/col=true
        // row
        this.fieldName = fieldName; // for row=true fieldName

        // column sum
        this.numericColumnSum = new NumericColumnSum();
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        /*
            UDF per column -> sum result to target column -> repeat until all cols iterated
            through
        
            col=bool: show extra event at end for each column total
            row=bool: show extra column at end for each row total
            fieldList: for which fields, defaults to all
            fieldName: extra columns name, defaults to Total. Fieldname takes priority over label if the same name.
            labelField: if col=true, at the end of that extra event have a new <string> field with ${label}
            label: labelField's column name, defaults to Total
         */

        if (row) {
            UserDefinedFunction addtotalsUDF = functions.udf(new AddtotalsUDF(), DataTypes.StringType);
            dataset = dataset.withColumn(fieldName, functions.lit(0));
            for (String field : dataset.schema().fieldNames()) {
                if ((fieldList.isEmpty() || fieldList.contains(field)) && !field.equals(fieldName)) {
                    dataset = dataset
                            .withColumn(fieldName, functions.col(fieldName).plus(addtotalsUDF.apply(functions.col(field))));
                }
            }
        }

        if (col) {
            if (!labelField.isEmpty() && !labelField.equals(fieldName)) {
                // empty for all except the added event for col=true
                dataset = dataset.withColumn(labelField, functions.lit(null).cast(DataTypes.StringType));
            }

            // TODO: This casts all to string to circumvent typecasting issue later on, find a way to retain types
            //  Base schema should allow for null values!
            for (String field : dataset.schema().fieldNames()) {
                dataset = dataset.withColumn(field, functions.col(field).cast(DataTypes.StringType));
            }

            // Perform the accumulation
            final Iterator<Row> collected = dataset.collectAsList().iterator();
            lastRow = SparkSession
                    .builder()
                    .getOrCreate()
                    .createDataFrame(numericColumnSum.process(collected), dataset.schema());

            // fieldName takes priority over labelField if the same name
            if (!fieldName.equals(labelField)) {
                for (String field : dataset.schema().fieldNames()) {
                    if (field.equals(labelField)) {
                        dataset = dataset
                                .withColumn(field, functions.when(functions.col(field).isNull(), functions.lit("")));
                        lastRow = lastRow.withColumn(field, functions.lit(label));
                        break;
                    }
                }
            }
        }

        catCtx.getStepList().batchCollect().updateLastRow(lastRow);
        return dataset;
    }
}
