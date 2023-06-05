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

package com.teragrep.pth10.ast;

import com.teragrep.functions.dpf_02.BatchCollect;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Used for processing each of the batches in a forEachBatch.
 * This batch processor will be controlled using the DataStreamWriter provided by the parse tree walk
 */
public class BatchProcessor implements VoidFunction2<Dataset<Row>, Long>{
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchProcessor.class);

	private static final long serialVersionUID = 1L;
	
	private StructType schema = null;					// schema of batch df

	private ParseTree pt = null; 						// parse tree, from which to continue the parsing in ForEachBatch


	private DPLParserCatalystVisitor catVisitor = null; // visitor carried over from main parsing
	
	private Consumer<Dataset<Row>> batchHandler = null;	// for UI
	
	private BatchCollect batchCollect = null;			// batch collector for when no aggregates
	
	private boolean aggregatesUsed = false;


	/**
	 * Empty constructor, do not use. Will result in a RuntimeException
	 */
	public BatchProcessor() {
		super();
		throw new RuntimeException("BatchProcessor was created without any arguments!");
	}

	/**
	 * Constructor for no aggregations or groupBy columns.
	 * @param catVisitor Catalyst visitor
	 * @param aggregatesUsed Aggregations have been done?
	 * @param pt Parse tree from which to continue the walk within forEachBatch
	 */
	public BatchProcessor(String sortCol, DPLParserCatalystVisitor catVisitor, boolean aggregatesUsed, ParseTree pt) {
		super();
		LOGGER.info("Initializing BatchProcessor: sortCol= " + sortCol + " aggsUsed= " + aggregatesUsed);
		this.catVisitor = catVisitor;
		if ( sortCol != null ) this.batchCollect = new BatchCollect(sortCol, catVisitor.getDPLRecallSize());
		this.aggregatesUsed = aggregatesUsed;
		this.pt = pt;
	}

	/**
	 * BatchProcessor constructor for multiple aggregate expressions and multiple groupBy columns.
	 * @param sortCol column used in batchCollect for sorting, usually _time
	 * @param pt Remaining parse tree, which will be traversed from within the forEachBatch
	 * @param catVisitor The visitor object used for the original walk
	 * @param aggregatesUsed Are any aggregates used?
	 */
	public BatchProcessor(String sortCol, ParseTree pt, DPLParserCatalystVisitor catVisitor, boolean aggregatesUsed) {
		super();
		LOGGER.info("Initializing BatchProcessor: sortCol= " + sortCol + " aggsUsed= " + aggregatesUsed);
		this.pt = pt;
		this.catVisitor = catVisitor;
		if ( sortCol != null ) this.batchCollect = new BatchCollect(sortCol, catVisitor.getDPLRecallSize());
		this.aggregatesUsed = aggregatesUsed;
	}

    /**
	 * Set the batch handler. This is how the final dataset will be accessed.
	 * @param handler Consumer of type DatasetRow
	 */
	public void setBatchHandler(Consumer<Dataset<Row>> handler) {
		this.batchHandler = handler;
	}

	/**
	 * Sends the processed batch to the {@link #batchHandler}<br>
	 * This is where any possible sorting happens through dpf_02
	 * @param ds Processed batch dataset
	 * @param id ID of the processed batch dataset
	 */
	private void sendBatchEvent(Dataset<Row> ds, Long id) {
		if (this.batchHandler != null) {
			if (this.aggregatesUsed) {
				LOGGER.info("------------------ Aggregates used, sending batch event!");
				this.batchHandler.accept(ds);
			}
			else if (this.batchCollect == null) {
				LOGGER.info("------------------ No batchCollect present (no sorting column), sending batch event!");
				this.batchHandler.accept(ds);
			}
			else {
				LOGGER.info("------------------ Aggregates NOT USED, using batch collect!");
				this.batchCollect.collect(ds, id);
				this.batchHandler.accept(batchCollect.getCollectedAsDataframe());
			}
		}
	}

	/**
	 * Called on every new batch by <code>DataStreamWriter.forEachBatch();</code>
	 * Performs the aggregations and group bys, and continues parsing the rest
	 * of the parse tree, if it was provided to the BatchProcessor.
	 * The final dataset will be sent forward using {@link #sendBatchEvent(Dataset, Long)} method.
	 * @param batchDF batch dataset
	 * @param batchId batch id
	 * @throws Exception any exception encountered during the call
	 */
	@Override
	public void call(Dataset<Row> batchDF, Long batchId) throws Exception {
		LOGGER.info("BatchProcessor received new batch #" + batchId);

		this.schema = batchDF.schema();

		// Get the existing context
		DPLParserCatalystContext catCtx = catVisitor.getCatalystContext();

		// timechart empty buckets
		if (catCtx.getTimeChartSpanSeconds() != null) {
			// create spans
			final long min = catCtx.getDplMinimumEarliest();
			final long max = catCtx.getDplMaximumLatest();
			final long step = catCtx.getTimeChartSpanSeconds();

			final Dataset<Row> rangeDs =
					catCtx
							.getSparkSession()
							.range((min/step)*step,
									((max/step)+1) * step, step)
							.select(functions.col("id").cast("timestamp").alias("_range"));
			// left join span to data & continue
			batchDF = rangeDs.join(
					batchDF,
					rangeDs.col("_range").equalTo(batchDF.col("_time")), "left")
					.drop("_time")
					.withColumnRenamed("_range", "_time")
					.orderBy("_time");


			// fill null data with "0" for all types, except for the "_time" column
			for (final StructField field : batchDF.schema().fields()) {
				final String name = field.name();
				final DataType dataType = field.dataType();

				if (dataType == DataTypes.StringType) {
					batchDF = batchDF.na().fill("0", new String[]{name});
				}
				else if (dataType == DataTypes.IntegerType) {
					batchDF = batchDF.na().fill(0, new String[]{name});
				}
				else if (dataType == DataTypes.LongType) {
					batchDF = batchDF.na().fill(0L, new String[]{name});
				}
				else if (dataType == DataTypes.DoubleType) {
					batchDF = batchDF.na().fill(0d, new String[]{name});
				}
				else if (dataType == DataTypes.FloatType) {
					batchDF = batchDF.na().fill(0f, new String[]{name});
				}
				// skip TimestampType
			}
		}

		// Continue sub parse tree parsing, if necessary
		if (this.pt != null && !this.pt.getText().equals("<EOF>")) {
			LOGGER.info("BatchProcessor:: Continuing parsing to: " + pt.getText());

			// Clear the pre-existing processingStack, since that won't be used
			// anymore. Following commands will be run from here using the remainingParseTree instead.
			catVisitor.processingStack.clear();
			catCtx.setDs(batchDF);
			catVisitor.processingStack.push(batchDF);
			
			// Visit the remaining parse tree
			catVisitor.visit(pt);
			LOGGER.info("BatchProcessor:: Traversed the parse tree");

			Dataset<Row> dfFromVisit = catVisitor.processingStack.pop();
			
			if (dfFromVisit != null) {
				// Callback with full parse tree visit (Performs operation defined by visitor's batchConsumer,
				// which can be set using the setConsumer() method.)

				// Get aggregates used from parse tree visit within the batch
				// Useful, when stack "forced" into sequential mode
				// in commands like timechart or sendemail.
				this.aggregatesUsed = catVisitor.getAggregatesUsed();

				if (catVisitor.getStack().isSorted()) {
					this.batchCollect = null;
				}

				sendBatchEvent(dfFromVisit, batchId);
			}
		}
		else {
			// no further parsing needed (parseTree = <EOF>)
			// Callback with aggregated batch (Performs operation defined by visitor's batchConsumer,
			// which can be set using the setConsumer() method.)
			LOGGER.info("BatchProcessor :: Nowhere to walk, continue to sending batch event");
			sendBatchEvent(batchDF, batchId);
		}
	}
}
