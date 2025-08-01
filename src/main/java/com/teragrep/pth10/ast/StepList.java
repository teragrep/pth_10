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
package com.teragrep.pth10.ast;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.functions.dpf_02.BatchCollect;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class StepList implements VoidFunction2<Dataset<Row>, Long> {

    private enum BreakpointType {
        SEQUENTIAL, POST_BC
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(StepList.class);
    private final List<AbstractStep> list;
    private final Map<BreakpointType, Integer> breakpoints;
    private int aggregateCount = 0;
    private boolean useInternalBatchCollect = false;
    private boolean ignoreDefaultSorting = false;

    private OutputMode outputMode = OutputMode.Append();
    private Consumer<Dataset<Row>> batchHandler = null; // for UI
    private BatchCollect batchCollect; // standard batchCollect, used before sending batch event
    private DPLParserCatalystVisitor catVisitor;

    public void setBatchCollect(BatchCollect batchCollect) {
        this.batchCollect = batchCollect;
    }

    public void setBatchHandler(Consumer<Dataset<Row>> batchHandler) {
        this.batchHandler = batchHandler;
    }

    public BatchCollect batchCollect() {
        return this.batchCollect;
    }

    @Deprecated
    public Consumer<Dataset<Row>> getBatchHandler() {
        return batchHandler;
    }

    public void setCatVisitor(DPLParserCatalystVisitor catVisitor) {
        this.catVisitor = catVisitor;
    }

    public StepList(DPLParserCatalystVisitor catVisitor) {
        this.list = new ArrayList<>();
        this.catVisitor = catVisitor;
        this.batchCollect = new BatchCollect(
                "_time",
                catVisitor.getCatalystContext().getDplRecallSize(),
                catVisitor.getCatalystContext().postBcLimitSize()
        );
        this.breakpoints = new HashMap<>();
    }

    /**
     * Add the specified step to the StepList
     * 
     * @param step step to add
     * @return if adding was a success
     */
    public boolean add(AbstractStep step) {
        return this.list.add(step);
    }

    /**
     * Returns a map containing the field names and their values as toString() for those values provides. If the given
     * index is invalid, returns null. If the value of a field cannot be accessed, returns a ??? value in the map
     * instead.
     * 
     * @param i index between 0 and size-1 of the internal list
     * @return mapping of field-value
     */
    public Map<String, String> getParamsOf(int i) {
        if (i < this.list.size() && i >= 0) {
            Map<String, String> rv = new HashMap<>();
            Field[] fields = this.list.get(i).getClass().getDeclaredFields();
            for (Field f : fields) {
                f.setAccessible(true);
                try {
                    rv.put(f.getName(), f.get(this.list.get(i)).toString());
                }
                catch (IllegalAccessException e) {
                    rv.put(f.getName(), "???");
                }
            }
            return rv;
        }
        else {
            return null;
        }
    }

    /**
     * returns the count of aggregates currently processed
     * 
     * @return the count
     */
    public int getAggregateCount() {
        return aggregateCount;
    }

    /**
     * Execute the steps included in the list
     * 
     * @return DataStreamWriter which can be used to start the query
     */
    public DataStreamWriter<Row> execute() throws StreamingQueryException {
        this.analyze();
        return executeFromStep(0, null);
    }

    public List<AbstractStep> asList() {
        return this.list;
    }

    public Dataset<Row> executeSubsearch(Dataset<Row> ds) throws StreamingQueryException {
        // TODO: Sequential subsearch support
        //this.analyze();
        for (AbstractStep step : this.list) {
            ds = step.get(ds);
        }

        return ds;
    }

    private DataStreamWriter<Row> executeFromStep(int fromStepIndex, Dataset<Row> ds) throws StreamingQueryException {
        for (int i = fromStepIndex; i < this.list.size(); i++) {
            AbstractStep step = this.list.get(i);
            if (
                (breakpoints.containsKey(BreakpointType.SEQUENTIAL) && breakpoints
                        .get(BreakpointType.SEQUENTIAL) == i) || (breakpoints.containsKey(BreakpointType.POST_BC)
                                && breakpoints.get(BreakpointType.POST_BC) == i)
            ) {
                // Switch to sequential; aka run the step inside forEachBatch
                LOGGER.debug("breakpoint encountered at index <{}>", i);

                return ds.writeStream().outputMode(this.outputMode).foreachBatch(this);
            }
            ds = step.get(ds);
        }

        return ds.writeStream().outputMode(this.outputMode).foreachBatch(this);
    }

    private Dataset<Row> executeInBatch(Dataset<Row> ds) throws StreamingQueryException {
        if (!breakpoints.containsKey(BreakpointType.SEQUENTIAL)) { // no sequential ops
            return ds;
        }

        // sequential ops found
        for (int i = breakpoints.get(BreakpointType.SEQUENTIAL); i < this.list.size(); i++) {
            AbstractStep step = this.list.get(i);
            LOGGER.info("Executing seq ops in batch: <{}>", step.toString());
            ds = step.get(ds);
        }
        return ds;
    }

    /**
     * Analyze for parallel/sequential split
     */
    private void analyze() {
        if (this.list.isEmpty()) {
            throw new RuntimeException("StepList was empty, expected at least one step");
        }

        for (int i = 0; i < this.list.size(); i++) {
            AbstractStep step = this.list.get(i);
            LOGGER.info("Analyzing step: <{}>", step.toString());
            step.setAggregatesUsedBefore(aggregateCount > 0);

            if (step.hasProperty(AbstractStep.CommandProperty.USES_INTERNAL_BATCHCOLLECT)) {
                LOGGER.info("[Analyze] Step uses internal batch collect: <{}>", step);
                this.useInternalBatchCollect = true;
                this.batchCollect = null;
            }

            if (step.hasProperty(AbstractStep.CommandProperty.IGNORE_DEFAULT_SORTING)) {
                LOGGER.info("[Analyze] Ignore default sorting: <{}>", step);
                this.ignoreDefaultSorting = true;
                this.batchCollect = new BatchCollect(
                        null,
                        catVisitor.getDPLRecallSize(),
                        catVisitor.getCatalystContext().postBcLimitSize()
                );
            }

            if (step.hasProperty(AbstractStep.CommandProperty.REQUIRE_PRECEDING_AGGREGATE)) {
                if (aggregateCount <= 0) {
                    throw new RuntimeException("Step '" + step + "' requires a preceding aggregate!");
                }
            }

            if (step.hasProperty(AbstractStep.CommandProperty.NO_PRECEDING_AGGREGATE)) {
                if (aggregateCount > 0) {
                    throw new RuntimeException("Step '" + step + "' cannot be used after aggregations!");
                }
            }

            if (step.hasProperty(AbstractStep.CommandProperty.SEQUENTIAL_ONLY)) {
                LOGGER.info("[Analyze] Sequential only command: <{}>", step);
                // set the breakpoint just once
                if (!breakpoints.containsKey(BreakpointType.SEQUENTIAL)) {
                    breakpoints.put(BreakpointType.SEQUENTIAL, i);
                }
            }
            else if (step.hasProperty(AbstractStep.CommandProperty.AGGREGATE)) {
                LOGGER.info("[Analyze] Aggregate command: <{}>", step);
                aggregateCount++;

                // set the breakpoint just once
                if (
                    aggregateCount > 0 && !breakpoints.containsKey(BreakpointType.SEQUENTIAL)
                            && !breakpoints.containsKey(BreakpointType.POST_BC)
                ) {
                    breakpoints.put(BreakpointType.SEQUENTIAL, i + 1);
                    outputMode = OutputMode.Complete();
                }
            }
            else if (step.hasProperty(AbstractStep.CommandProperty.POST_BATCHCOLLECT)) {
                if (!breakpoints.containsKey(BreakpointType.POST_BC)) {
                    LOGGER.info("[Analyze] Post batch collect command: <{}>", step);
                    breakpoints.put(BreakpointType.POST_BC, i);
                }
            }
        }
    }

    /**
     * Sends the processed batch to the {@link #batchHandler}<br>
     * This is where any possible sorting happens through dpf_02
     * 
     * @param ds Processed batch dataset
     * @param id ID of the processed batch dataset
     */
    private void sendBatchEvent(Dataset<Row> ds, Long id) {
        if (this.batchHandler != null) {
            if (outputMode == OutputMode.Complete()) {
                LOGGER.info("------------------ Aggregates (Complete Mode) used, sending batch event!");
                this.batchHandler.accept(ds);
            }
            else if (this.batchCollect == null) {
                LOGGER.info("------------------ No batchCollect present (no sorting column), sending batch event!");
                this.batchHandler.accept(ds);
            }
            else {
                LOGGER.info("------------------ Aggregates NOT USED (before seq. switch), using batchCollect!");
                int index = this.list.size();
                if (breakpoints.containsKey(BreakpointType.POST_BC)) {
                    index = breakpoints.get(BreakpointType.POST_BC);
                }
                this.batchCollect.collect(ds, id, this.list.subList(index, this.list.size()), false);
                this.batchHandler.accept(batchCollect.getCollectedAsDataframe());
            }
        }
    }

    public void call(Dataset<Row> batchDF, Long batchId) throws StreamingQueryException {
        LOGGER.info("StepList batch processing received a new batch <{}>", batchId);

        // timechart empty buckets
        if (
            catVisitor.getCatalystContext().getTimeChartSpanSeconds() != null
                    && !breakpoints.containsKey(BreakpointType.POST_BC)
        ) {
            // create spans
            final long min = catVisitor.getCatalystContext().getDplMinimumEarliest();
            final long max = catVisitor.getCatalystContext().getDplMaximumLatest();
            final long step = catVisitor.getCatalystContext().getTimeChartSpanSeconds();

            final Dataset<Row> rangeDs = catVisitor
                    .getCatalystContext()
                    .getSparkSession()
                    .range((min / step) * step, ((max / step) + 1) * step, step)
                    .select(functions.col("id").cast("timestamp").alias("_range"));
            // left join span to data & continue
            batchDF = rangeDs
                    .join(batchDF, rangeDs.col("_range").equalTo(batchDF.col("_time")), "left")
                    .drop("_time")
                    .withColumnRenamed("_range", "_time")
                    .orderBy("_time");

            // fill null data with "0" for all types, except for the "_time" column
            for (final StructField field : batchDF.schema().fields()) {
                final String name = field.name();
                final DataType dataType = field.dataType();

                if (dataType == DataTypes.StringType) {
                    batchDF = batchDF.na().fill("0", new String[] {
                            name
                    });
                }
                else if (dataType == DataTypes.IntegerType) {
                    batchDF = batchDF.na().fill(0, new String[] {
                            name
                    });
                }
                else if (dataType == DataTypes.LongType) {
                    batchDF = batchDF.na().fill(0L, new String[] {
                            name
                    });
                }
                else if (dataType == DataTypes.DoubleType) {
                    batchDF = batchDF.na().fill(0d, new String[] {
                            name
                    });
                }
                else if (dataType == DataTypes.FloatType) {
                    batchDF = batchDF.na().fill(0f, new String[] {
                            name
                    });
                }
                // skip TimestampType
            }
        }

        // Continue sub list of steps execution, if necessary
        if (!this.list.isEmpty()) {
            LOGGER.info("StepList batch processing - Continuing execution to next ops after breakpoint index");

            Dataset<Row> ret = this.executeInBatch(batchDF);

            LOGGER.info("StepList batch processing - Executed the steps");

            if (ret != null) {
                sendBatchEvent(ret, batchId);
            }
        }
        else {
            // No sequential steps left to execute, return batch as-is
            LOGGER.info("StepList batch processing - No steps left to execute, continue to sending batch event");
            sendBatchEvent(batchDF, batchId);
        }
    }

}
