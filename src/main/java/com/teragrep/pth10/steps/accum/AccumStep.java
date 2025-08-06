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
package com.teragrep.pth10.steps.accum;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.NullValue;
import com.teragrep.pth10.steps.ParsedResult;
import com.teragrep.pth10.steps.TypeParser;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AccumStep extends AbstractStep implements Serializable {

    private final String sourceField;
    private final String renameField;
    private final NullValue nullValue;

    public AccumStep(final NullValue nullValue, final String sourceField, final String renameField) {
        super();
        this.properties.add(CommandProperty.IGNORE_DEFAULT_SORTING);
        this.nullValue = nullValue;
        this.sourceField = sourceField;
        this.renameField = renameField;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        // group all events under the same group (=0)
        final String groupCol = "$$accum_internal_grouping_col$$";
        Dataset<Row> dsWithGroupCol = dataset.withColumn(groupCol, functions.lit(0));

        // Create output encoder for results
        ExpressionEncoder<Row> outputEncoder;
        if (renameField.isEmpty()) {
            // No rename field: Use source column for results
            dsWithGroupCol = dsWithGroupCol
                    .withColumn(sourceField, functions.col(sourceField).cast(DataTypes.StringType));
            outputEncoder = RowEncoder.apply(dsWithGroupCol.schema());
        }
        else {
            // Rename field: Used 'as <new-field>', returns StringType
            final StructType st = dsWithGroupCol.schema().add(renameField, DataTypes.StringType);
            outputEncoder = RowEncoder.apply(st);
        }

        // group dataset by '0', creating one group
        KeyValueGroupedDataset<Integer, Row> keyValueGroupedDs = dsWithGroupCol
                .groupByKey((MapFunction<Row, Integer>) (r) -> (Integer) r.getAs(groupCol), Encoders.INT());

        // use flatMapGroupsWithState to retain state between rows; grouping really isn't used here.
        // IntermediateState is used to retain state of cumulative sum.
        Dataset<Row> rv = keyValueGroupedDs
                .flatMapGroupsWithState(
                        this::flatMapGroupsWithStateFunc, OutputMode.Append(), Encoders
                                .javaSerialization(IntermediateState.class),
                        outputEncoder, GroupStateTimeout.NoTimeout()
                );

        // Return whilst dropping grouping column
        return rv.drop(groupCol);
    }

    private Iterator<Row> flatMapGroupsWithStateFunc(
            Integer group,
            Iterator<Row> events,
            GroupState<IntermediateState> state
    ) {
        // Get the previous state if applicable, otherwise initialize state
        final IntermediateState currentState;
        if (state.exists()) {
            currentState = state.get();
        }
        else {
            currentState = new IntermediateState();
        }

        // Perform the cumulative sum aggregation
        final List<Row> newEvents = new ArrayList<>();
        while (events.hasNext()) {
            // Holds contents for each row
            final List<Object> rowContents = new ArrayList<>();
            // If a row is to be skipped: when it is of a non-numerical value (string)
            boolean skip = false;
            // Get next row
            final Row r = events.next();

            // Get sourceField content as string
            final String valueAsString = r.getAs(r.fieldIndex(sourceField)).toString();
            // Parse to LONG or DOUBLE. Others will be STRING and skipped.
            TypeParser typeParser = new TypeParser();
            final ParsedResult parsedResult = typeParser.parse(valueAsString);
            if (parsedResult.getType().equals(ParsedResult.Type.LONG)) {
                // got long, accumulate
                currentState.accumulate(parsedResult.getLong());
            }
            else if (parsedResult.getType().equals(ParsedResult.Type.DOUBLE)) {
                // got double, accumulate
                currentState.accumulate(parsedResult.getDouble());
            }
            else {
                // string, skip and return empty
                skip = true;
            }

            // Build new row: First, add already existing fields
            for (int i = 0; i < r.length(); i++) {
                if (renameField.isEmpty() && i == r.fieldIndex(sourceField) && !skip) {
                    // replace old content with cumulative sum if no new field given
                    rowContents
                            .add(
                                    currentState.isLongType() ? currentState.asLong().toString() : currentState
                                            .asDouble()
                                            .toString()
                            );
                }
                else {
                    // return old content if renameField was given or current row is to be skipped
                    rowContents.add(r.get(i));
                }
            }
            // Add new field to row if renameField was used
            if (!renameField.isEmpty()) {
                // add to new field if given
                if (skip) {
                    // on skip return null
                    rowContents.add(nullValue.value());
                }
                else {
                    rowContents
                            .add(
                                    currentState.isLongType() ? currentState.asLong().toString() : currentState
                                            .asDouble()
                                            .toString()
                            );
                }
            }
            // Add new row to collection of new rows
            newEvents.add(RowFactory.create(rowContents.toArray()));
        }

        // Update state and return new events' iterator
        state.update(currentState);
        return newEvents.iterator();
    }
}
