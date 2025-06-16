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
package com.teragrep.pth10.steps.dedup;

import com.teragrep.functions.dpf_02.BatchCollect;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class DedupStep extends AbstractDedupStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(DedupStep.class);

    public DedupStep(
            List<String> listOfFields,
            int maxDuplicates,
            boolean keepEmpty,
            boolean keepEvents,
            boolean consecutive,
            DPLParserCatalystContext catCtx,
            boolean completeOutputMode
    ) {
        super();
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
        this.properties.add(CommandProperty.USES_INTERNAL_BATCHCOLLECT);

        this.listOfFields = listOfFields;
        this.maxDuplicates = maxDuplicates;
        this.keepEmpty = keepEmpty;
        this.keepEvents = keepEvents;
        this.consecutive = consecutive;
        this.catCtx = catCtx;
        this.completeOutputMode = completeOutputMode;

        this.intBc = new BatchCollect(null, catCtx.getDplRecallSize());
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        dataset = intBc.call(dataset, 1L, true);

        this.fieldsProcessed = new ConcurrentHashMap<>();

        final List<Row> listOfRows = dataset.collectAsList();
        final int origSize = listOfRows.size();
        final AtomicReference<Row> previousRow = new AtomicReference<>();

        // TODO bring out the spaghetti mop and clean this mess
        final List<Row> output;
        if (keepEvents) {
            output = listOfRows;
            final StructType schema = dataset.schema();
            // keepEvents
            for (int i = 0; i < output.size(); i++) {
                for (final String fieldName : listOfFields) {
                    Row r = output.get(i);
                    // keepEmpty processing
                    final Object fieldValueObject = r.get(schema.fieldIndex(fieldName));
                    final String fieldValue;
                    if (fieldValueObject == null && !keepEmpty) {
                        output.set(i, nullifyRowField(r, schema, fieldName));
                        continue;// filter out
                    }
                    else if (fieldValueObject == null) {
                        fieldValue = "null";
                    }
                    else {
                        fieldValue = fieldValueObject.toString();
                    }

                    // consecutive=true
                    // return at end of if clause, because consecutive=true ignores
                    // maxDuplicates
                    if (consecutive) {
                        //LOGGER.debug(r.mkString(" "));
                        if (previousRow.get() == null) {
                            //LOGGER.debug("-> Applying row as previous row");
                            previousRow.set(r);
                        }
                        else {
                            final String prevValue = previousRow
                                    .get()
                                    .get(previousRow.get().fieldIndex(fieldName))
                                    .toString();
                            if (prevValue.equals(fieldValue)) {
                                //LOGGER.debug("-> Filtering");
                                output.set(i, nullifyRowField(r, schema, fieldName));
                                continue;
                            }
                            previousRow.set(r);
                        }
                    }

                    // consecutive=false
                    if (fieldsProcessed.containsKey(fieldName)) {
                        if (!fieldsProcessed.get(fieldName).containsKey(fieldValue)) {
                            // specific field value was not encountered yet, add to map
                            fieldsProcessed.get(fieldName).put(fieldValue, 1L);
                        }
                        else {
                            // field:value present in map, check if amount of duplicates is too high
                            long newValue = fieldsProcessed.get(fieldName).get(fieldValue) + 1L;
                            if (newValue > maxDuplicates) {
                                // too many duplicates, filter out
                                output.set(i, nullifyRowField(r, schema, fieldName));
                                continue;
                            }
                            else {
                                // duplicates within given max value, ok to be present
                                fieldsProcessed.get(fieldName).put(fieldValue, newValue);
                            }
                        }
                    }
                    else {
                        // the field was not encountered yet, add to map
                        final Map<String, Long> newMap = new ConcurrentHashMap<>();
                        newMap.put(fieldValue, 1L);
                        fieldsProcessed.put(fieldName, newMap);
                    }
                }
            }
        }
        else {
            // non-keepEvents
            output = listOfRows.stream().filter(r -> {
                boolean doNotFilter = true;
                for (final String fieldName : listOfFields) {
                    // keepEmpty processing
                    final Object fieldValueObject = r.get(r.fieldIndex(fieldName));
                    final String fieldValue;
                    if (fieldValueObject == null && !keepEmpty) {
                        return false; // filter out
                    }
                    else if (fieldValueObject == null) {
                        fieldValue = "null";
                    }
                    else {
                        fieldValue = fieldValueObject.toString();
                    }

                    // consecutive=true
                    // return at end of if clause, because consecutive=true ignores
                    // maxDuplicates
                    if (consecutive) {
                        //LOGGER.debug(r.mkString(" "));
                        if (previousRow.get() == null) {
                            //LOGGER.debug("-> Applying row as previous row");
                            previousRow.set(r);
                        }
                        else {
                            final String prevValue = previousRow
                                    .get()
                                    .get(previousRow.get().fieldIndex(fieldName))
                                    .toString();
                            if (prevValue.equals(fieldValue)) {
                                //LOGGER.debug("-> Filtering");
                                doNotFilter = false;
                            }
                            previousRow.set(r);
                        }

                        return doNotFilter; //ignore rest of the processing
                    }

                    // consecutive=false
                    if (fieldsProcessed.containsKey(fieldName)) {
                        if (!fieldsProcessed.get(fieldName).containsKey(fieldValue)) {
                            // specific field value was not encountered yet, add to map
                            fieldsProcessed.get(fieldName).put(fieldValue, 1L);
                        }
                        else {
                            // field:value present in map, check if amount of duplicates is too high
                            long newValue = fieldsProcessed.get(fieldName).get(fieldValue) + 1L;
                            if (newValue > maxDuplicates) {
                                // too many duplicates, filter out
                                doNotFilter = false;
                            }
                            else {
                                // duplicates within given max value, ok to be present
                                fieldsProcessed.get(fieldName).put(fieldValue, newValue);
                            }
                        }
                    }
                    else {
                        // the field was not encountered yet, add to map
                        final Map<String, Long> newMap = Collections.synchronizedMap(new ConcurrentHashMap<>());
                        newMap.put(fieldValue, 1L);
                        fieldsProcessed.put(fieldName, newMap);
                    }
                }
                return doNotFilter;
            }).collect(Collectors.toList());
        }

        LOGGER.info("Output contains <{}> out of <{}> row(s)", output.size(), origSize);
        return SparkSession.getActiveSession().get().createDataFrame(output, dataset.schema());
    }

    /**
     * Takes the row and generates a new one with the given field nullified
     * 
     * @param r         row
     * @param fieldName field to nullify
     * @return row with the field nullified
     */
    private Row nullifyRowField(final Row r, final StructType schema, String fieldName) {
        final List<Object> newRowValues = new ArrayList<>();

        for (final StructField field : schema.fields()) {
            if (!field.name().equals(fieldName)) {
                newRowValues.add(r.get(schema.fieldIndex(field.name())));
            }
            else {
                if (field.nullable()) {
                    newRowValues.add(catCtx.nullValue.value());
                }
                else {
                    throw new IllegalStateException("Field was not nullable! field=<" + field.name() + ">");
                }

            }
        }
        return RowFactory.create(newRowValues.toArray());
    }
}
