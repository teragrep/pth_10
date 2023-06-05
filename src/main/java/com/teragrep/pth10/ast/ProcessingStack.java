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

import com.teragrep.functions.dpf_02.SortByClause;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * <h1>ProcessingStack class</h1>
 * <p>Used to hold the Spark DataFrame being processed.<br>
 * Has two StackModes, PARALLEL (which is the default) and SEQUENTIAL.<br>
 * StackMode can be changed from PARALLEL to SEQUENTIAL, but not vice versa.<br>
 * StreamingOutputMode can be set once to COMPLETE or APPEND.</p>
 * 
 * <p>SEQUENTIAL StackMode is used, when multiple aggregations will be performed on the DataFrame.<br>
 * The remaining parsetree will be saved to the ProcessingStack and continued on batchprocessing's
 * forEachBatch function.<br>
 * PARALLEL StackMode is used when there is no multiple aggregations, and the DataFrame will be
 * available as-is on the ProcessingStack.</p>
 * 
 * @author p000043u
 *
 */
public class ProcessingStack implements Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingStack.class);

	private static final long serialVersionUID = 1L;

	/*
        One aggregate = PARALLEL mode
        More than one aggregate = SEQUENTIAL mode, and continue parsing from within DataStreamWriter.forEachBatch,
        as spark doesn't support multiple aggregations with a structured streaming dataset.
    */
    
	// colors for tracebuffer
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    
    /** Enables or disables prints used for debugging */
    private static final boolean DEBUG_ENABLED = false;
    
    /** Stack containing dataframes from basic operations */
    private Deque<Dataset<Row>> parallelOpsStack = new LinkedList<Dataset<Row>>();
    
    /** Stack containing dataframes and DataStreamWriters from operations that require
    * using forEachBatch instead of Spark structured streaming */
    private Deque<SequentialOperationItem> sequentialOpsStack = new LinkedList<SequentialOperationItem>();
    
    /** Used to grab DataStreamWriter for the next item in stack */
    private SequentialOperationItem lastRemovedItemFromSeqOpStack = null; 
    
    // sequential or parallel stack mode
    private StackMode stackMode = null;
    
    // complete or append streaming output mode
    private StreamingOutputMode streamOutputMode = null;
    
    /** the visitor used for walking the parse tree */
    private DPLParserCatalystVisitor catVisitor = null;
    
    /** contains the rest of the parse tree when in sequential mode
    * forEachBatch continues from this tree, and the original walk will be stopped */
    private ParseTree remainingParseTree = null; 
    
    // tracebuffer for debugging
    private List<String> tracebuf = new ArrayList<>(50);

	/** List of sortByClauses, which are used by the sort command. */
	List<SortByClause> listOfSortByClauses = new ArrayList<>();

	// Used to set StackMode
    public enum StackMode {
    	PARALLEL, SEQUENTIAL
    }
    
    // Used to set StreamingOutputMode
    public enum StreamingOutputMode {
    	APPEND, COMPLETE
    }

	// Used to determine if the dataset has been sorted and if _time ordering should not be done in batchCollect
	private boolean isSorted = false;
	public boolean isSorted() {
		return isSorted;
	}

	public void setSorted(boolean sorted) {
		this.isSorted = sorted;
	}

    /** Constructor for stack - Defaults to PARALLEL - COMPLETE mode. */
    public ProcessingStack() {
    	this.stackMode = StackMode.PARALLEL;
    	this.streamOutputMode = StreamingOutputMode.COMPLETE;
    }

	/**
	 * Constructor for stack with visitor, defaults to PARALLEL - COMPLETE mode.
	 * @param visitor Catalyst visitor
	 */
	public ProcessingStack(DPLParserCatalystVisitor visitor) {
    	this.stackMode = StackMode.PARALLEL;
    	this.streamOutputMode = StreamingOutputMode.COMPLETE;
    	this.catVisitor = visitor;
    }
    
    // Get DPL visitor
    public DPLParserCatalystVisitor getCatVisitor() {
    	return this.catVisitor;
    }
    
    // Get tracebuffer (for debugging)
    public List<String> getTracebuf() {
        return this.tracebuf;
    }
        
    // Get current StackMode
    public StackMode getStackMode() {
        return this.stackMode;
    }

    /** Set from PARALLEL to SEQUENTIAL
    * Cannot revert back to PARALLEL */
    public void setStackMode(StackMode newMode) {
    	if (this.stackMode == StackMode.SEQUENTIAL && newMode == StackMode.PARALLEL) {
    		throw new IllegalArgumentException("[ProcessingStack.setStackMode] Can't set to PARALLEL mode when current mode is SEQUENTIAL!");
    	}
    	this.stackMode = newMode;
    }
    
    // Get stream output mode
    public StreamingOutputMode getStreamingOutputMode() {
    	return this.streamOutputMode;
    }
    
    // Set stream output mode
    public void setStreamingOutputMode(StreamingOutputMode som) {
    	if (this.streamOutputMode != null) {
    		throw new IllegalArgumentException("[ProcessingStack.setStreamingOutputMode] Streaming output mode can only be set once!");
    	}
    	
    	this.streamOutputMode = som;
    }
    
    // Get each pipeline
    public Deque<Dataset<Row>> getParallelOpsStack() {
    	return this.parallelOpsStack;
    }
    
    public Deque<SequentialOperationItem> getSequentialOpsStack() {
    	return this.sequentialOpsStack;
    }
    
    /** Set saved parsetree */
    public void setParseTree(ParseTree pt) {
    	this.remainingParseTree = pt;
		LOGGER.info("Stack parse tree set to: " + (this.remainingParseTree != null ? this.remainingParseTree.getText() : "<null>"));
    }

	/** Get saved parsetree */
    public ParseTree getParseTree() {
    	return this.remainingParseTree;
    }

	// Set & get sortByClause list
	public void setListOfSortByClauses(List<SortByClause> newList) {
		this.listOfSortByClauses = newList;
	}

	public List<SortByClause> getListOfSortByClauses() {
		return this.listOfSortByClauses;
	}

    /** Pops from current stack */
    public Dataset<Row> pop() {
    	if (DEBUG_ENABLED) LOGGER.info("ProcessingStack :: Popping from stack in " + this.stackMode + " stack mode");
    	Dataset<Row> item = null;
    	
    	// On parallel mode, dataframe can be popped as-is
    	if (this.stackMode == StackMode.PARALLEL) {
    		item = parallelOpsStack.pop();
    	}
    	// On sequential mode, pop SequentialOperationItem and get dataframe from it
    	else if (this.stackMode == StackMode.SEQUENTIAL) {
    		SequentialOperationItem sequentialOp = sequentialOpsStack.pop();
    		this.lastRemovedItemFromSeqOpStack = sequentialOp;
    		
    		Dataset<Row> sequentialOpDataframe = sequentialOp.getDataframe();
    		if (sequentialOpDataframe != null) {
    			item = sequentialOpDataframe;
    		}
    		else { // the dataframe should not be null, so throw exception
    			throw new RuntimeException("[ProcessingStack.pop] Sequential stack mode error: Dataset was null");
    		}
    		
    	}
        
    	tracebuf.add(ANSI_RED+ "Stack.pop value:" + item + "  stack content=" + this + ANSI_RESET);
        return item;
    }

    /** Pop operation with tracebuffer debugging */
    public Dataset<Row> pop(String tracePoint){
        Dataset<Row> item = this.pop();
        tracebuf.add(ANSI_RED+"Called from "+tracePoint+" Stack.pop value:"+item+"  stack content="+this+ANSI_RESET);
        return item;
    }

    /** Pushes to current stack */
    public Dataset<Row> push(Dataset<Row> item) {
    	if (DEBUG_ENABLED) LOGGER.info("ProcessingStack :: Pushing into stack in " + this.stackMode + " stack mode");
    	
    	tracebuf.add(ANSI_RED+"Stack.push value:"+item+"  stack content="+this+ANSI_RESET);
    	
    	// Pushes dataset to stack normally on parallel mode
    	if (this.stackMode == StackMode.PARALLEL) {
    		if (parallelOpsStack.size() > 0) {
    			 throw new RuntimeException("Pushing item to non-empty stack size="+parallelOpsStack.size()+ " content:"+this);
    		}
    		parallelOpsStack.push(item);
    		return item;
    	}
    	// Pushes SequentialOpItem with dataset into stack. Also grabs DataStreamWriter from previous item, if exists.
    	// Makes sure that the streamwriter is kept in the stack at all times
    	else if (this.stackMode == StackMode.SEQUENTIAL) {
    		if (sequentialOpsStack.size() > 0) {
    			throw new RuntimeException("Pushing item to non-empty stack size="+sequentialOpsStack.size()+ " content:"+this);
    		}
    		// When the sequential item gets pushed from here, it means no data stream writer nor parsetree is included
    		// save last popped item into lastRemovedItemFromSeqOpStack and get the writer and parsetree from there.
    		SequentialOperationItem newSeqItem = new SequentialOperationItem(item);
    		if (this.lastRemovedItemFromSeqOpStack != null) {
    			newSeqItem.setStreamWriter(this.lastRemovedItemFromSeqOpStack.getStreamWriter());
    		}
    		
    		sequentialOpsStack.push(newSeqItem);
    		return item;
    	}
       
    	throw new RuntimeException("[ProcessingStack.push] Tried to push without any StackMode set !");
    }

    /** Push operation with tracebuffer */
    public Dataset<Row> push(String tracePoint, Dataset<Row> item) {
        tracebuf.add(ANSI_RED+"Called from "+tracePoint+"Stack.push value:"+item+"  stack content="+this+ANSI_RESET);
        return this.push(item);
    }

    /** Push operation with tracebuffer and setting stackMode */
    public Dataset<Row> push(String tracePoint, Dataset<Row> item, StackMode stackMode) {
    	StackMode currentMode = this.stackMode;
    	StackMode nextMode = stackMode;
        if (currentMode == StackMode.SEQUENTIAL && nextMode == StackMode.PARALLEL) {
            throw new IllegalArgumentException("[ProcessingStack.push] Can't change SEQUENTIAL stackMode back to PARALLEL.");
        }
        this.setStackMode(stackMode);
        return this.push(tracePoint, item);
    }
    
    /** push() for singular aggregation expression and group by column */
    public Dataset<Row> push(String tracePoint, Dataset<Row> item, StackMode mode, Column expr, Column groupByCol, boolean aggregatesUsed) {
    	LOGGER.info(String.format("[ProcessingStack.push] isStreaming=%s | isNull=%s | expr=%s | stackMode=%s | streamingMode=%s",
				(item != null) ? item.isStreaming() : "NULL", item == null, (expr != null) ? expr.expr().sql() : "NULL", mode, this.streamOutputMode));
    	
    	this.setStackMode(mode);

    	if (this.stackMode == StackMode.PARALLEL) {
    		// Parallel stack mode
    		// GroupByCol exists, apply groupBy
    		if (groupByCol != null) {
    			this.parallelOpsStack.push(item.groupBy(groupByCol).agg(expr));
    		}
    		// If it does not, just aggregate
    		else {
    			this.parallelOpsStack.push(item.agg(expr));
    		}
    	}
    	else {
    		// Sequential stack mode
    		DataStreamWriter<Row> dsw = null;

			// Sort by _time in batch collect, unless sort command was used
			String sortCol = "_time";
			if (this.isSorted()) {
				sortCol = null;
			}

			// Create batchProcessor, give it the remaining parse tree, visitor and expressions to aggregate with.
    		BatchProcessor batchProcessor = new BatchProcessor(sortCol,
					this.remainingParseTree, this.catVisitor, aggregatesUsed);
    		
    		// Sets the batch handler to be the consumer given to DPLParserCatalystVisitor with setConsumer().
    		// This should be set from pth_07.
    		// The consumer allows accessing data batch-by-batch.
    		batchProcessor.setBatchHandler(this.catVisitor.getConsumer());

    		if (item.isStreaming())
    		{
    			// Streaming dataset
    			// Append output mode
    			if (!aggregatesUsed) {
    				dsw = item.writeStream().outputMode(OutputMode.Append())
        					.foreachBatch(batchProcessor);
    			}
    			// Complete output mode
    			else {
    				dsw = item.writeStream().outputMode(OutputMode.Complete())
        					.foreachBatch(batchProcessor);
    			}
    			
    			
    			
    			// Finally, push streamwriter and dataframe to stack.
    			SequentialOperationItem sequentialOp = new SequentialOperationItem(dsw, item);
    			
    			this.sequentialOpsStack.push(sequentialOp);
    		}
    		else {
    			// Not a streaming dataset, perform aggregation and push to stack
    			SequentialOperationItem sequentialOp = new SequentialOperationItem();
    			if (expr != null && groupByCol != null) {
    				sequentialOp.setDataframe(item.groupBy(groupByCol).agg(expr));
    			
    			}
    			else if (expr != null) {
    				sequentialOp.setDataframe(item.agg(expr));
    			}
    			else {
    				sequentialOp.setDataframe(item);
    			}
    			this.sequentialOpsStack.push(sequentialOp);
    			
    		}
    		
    	}
    	
    	return item;
    	
    }
    
    /** push() for multiple aggregations and group bys */
    public Dataset<Row> push(String tracePoint, Dataset<Row> item, StackMode mode, Column expr, Seq<Column> seqOfExpr, Seq<Column> seqOfGroupByCol, boolean aggregatesUsed) {
		LOGGER.info(String.format("[ProcessingStack.push] isStreaming=%s | isNull=%s | expr=%s | stackMode=%s | streamingMode=%s",
				(item != null) ? item.isStreaming() : "NULL", item == null, (expr != null) ? expr.expr().sql() : "NULL", mode, this.streamOutputMode));

    	this.setStackMode(mode);
    	if (this.stackMode == StackMode.PARALLEL) {
    		// Parallel stack mode
    		// GroupByCol exists, apply groupBy
    		if (seqOfGroupByCol != null) {
    			this.parallelOpsStack.push(item.groupBy(seqOfGroupByCol).agg(expr, seqOfExpr));
    		}
    		// If not, just perform aggregate only
    		else {
    			this.parallelOpsStack.push(item.agg(expr, seqOfExpr));
    		}
    		
    	}
    	else {
    		// Sequential stack mode
    		DataStreamWriter<Row> dsw = null;

			// Sort by _time in batch collect, unless sort command was used
			String sortCol = "_time";
			if (this.isSorted()) {
				sortCol = null;
			}

			// Create batchProcessor, give it the remaining parse tree, visitor and expressions to aggregate with.
    		BatchProcessor batchProcessor = new BatchProcessor(sortCol, this.remainingParseTree, this.catVisitor, aggregatesUsed);
    		
    		// Sets the batch handler to be the consumer given to DPLParserCatalystVisitor with setConsumer().
    		// This should be set from pth_07.
    		// The consumer allows accessing data batch-by-batch.
    		batchProcessor.setBatchHandler(this.catVisitor.getConsumer());

    		if (item.isStreaming())
    		{
    			// Append output mode
    			if (!aggregatesUsed) {
    				dsw = item.writeStream().outputMode(OutputMode.Append())
        					.foreachBatch(batchProcessor);
    			}
    			// Complete output mode
    			else {
    				dsw = item.writeStream().outputMode(OutputMode.Complete())
        					.foreachBatch(batchProcessor);
    			}
    			
    			
    			// Add streamwriter and dataframe to stack
    			SequentialOperationItem sequentialOp = new SequentialOperationItem(dsw, item);
    			
    			this.sequentialOpsStack.push(sequentialOp);
    		}
    		else {
    			// Not a streaming dataset, perform aggregate and push to stack
    			SequentialOperationItem sequentialOp = new SequentialOperationItem();
    			if (expr != null && seqOfGroupByCol != null) {
    				sequentialOp.setDataframe(item.groupBy(seqOfGroupByCol).agg(expr, seqOfExpr));
    			}
    			else if (expr != null){
    				sequentialOp.setDataframe(item.agg(expr, seqOfExpr));
    			}
    			else {
    				sequentialOp.setDataframe(item);
    			}
    			
    			this.sequentialOpsStack.push(sequentialOp);
    		}
    		
    	}
    	
    	return item;
    	
    }

	/** push() for aggregations */ // TODO remove other aggregation pushes
	public Dataset<Row> push(Dataset<Row> item, StackMode mode,
							 boolean aggregatesUsed) {


		this.setStackMode(mode);
		if (this.stackMode == StackMode.PARALLEL) {
			// Parallel stack mode
			parallelOpsStack.push(item);
		}
		else {
			// Sequential stack mode
			DataStreamWriter<Row> dsw = null;

			// Sort by _time in batch collect, unless sort command was used
			String sortCol = "_time";
			if (this.isSorted()) {
				sortCol = null;
			}

			// Create batchProcessor, give it the remaining parse tree, visitor and expressions to aggregate with.
			BatchProcessor batchProcessor = new BatchProcessor(sortCol,
					this.remainingParseTree, this.catVisitor, aggregatesUsed);

			// Sets the batch handler to be the consumer given to DPLParserCatalystVisitor with setConsumer().
			// This should be set from pth_07.
			// The consumer allows accessing data batch-by-batch.
			batchProcessor.setBatchHandler(this.catVisitor.getConsumer());

			if (item.isStreaming())
			{
				// Append output mode
				if (!aggregatesUsed) {
					dsw = item.writeStream().outputMode(OutputMode.Append())
							.foreachBatch(batchProcessor);
				}
				// Complete output mode
				else {
					dsw = item.writeStream().outputMode(OutputMode.Complete())
							.foreachBatch(batchProcessor);
				}


				// Add streamwriter and dataframe to stack
				SequentialOperationItem sequentialOp = new SequentialOperationItem(dsw, item);

				this.sequentialOpsStack.push(sequentialOp);
			}
			else {
				// Not a streaming dataset, perform aggregate and push to stack
				SequentialOperationItem sequentialOp = new SequentialOperationItem();
				sequentialOp.setDataframe(item);
				this.sequentialOpsStack.push(sequentialOp);
			}

		}

		return item;

	}

    /** Clears current stack */
    public void clear() {
        tracebuf.add(ANSI_GREEN+"Stack.clear current stack content="+this+ANSI_RESET);
        if (this.stackMode == StackMode.SEQUENTIAL) {
        	sequentialOpsStack.clear();
        }
        else if (this.stackMode == StackMode.PARALLEL) {
        	parallelOpsStack.clear();
        }
        else {
        	throw new RuntimeException("[ProcessingStack.clear] Invalid StackMode set");
        }
    }
    
    /** Clears specified stack */
    public void clear(StackMode stackMode) {
    	switch (stackMode) {
	    	case SEQUENTIAL:
	    		sequentialOpsStack.clear();
	    	case PARALLEL:
	    		parallelOpsStack.clear();
	    	default:
	    		throw new RuntimeException("[ProcessingStack.clear] Invalid StackMode given");
    	}
    }
    
    /** Is current stack empty or not? */
    public boolean isEmpty() {
    	switch (this.stackMode) {
	    	case SEQUENTIAL:
	    		return sequentialOpsStack.isEmpty();
	    	case PARALLEL:
	    		return parallelOpsStack.isEmpty();
	    	default:
	    		throw new RuntimeException("[ProcessingStack.isEmpty] Invalid StackMode set");
    	}
    }
    
    /** Is specified stack empty or not? */
    public boolean isEmpty(StackMode stackMode) {
    	switch (stackMode) {
	    	case SEQUENTIAL:
	    		return sequentialOpsStack.isEmpty();
	    	case PARALLEL:
	    		return parallelOpsStack.isEmpty();
	    	default:
	    		throw new RuntimeException("[ProcessingStack.isEmpty] Invalid StackMode given");
    	}
    }
    
    /** Returns the size of the current stack */
    public int size() {
    	switch (this.stackMode) {
	    	case SEQUENTIAL:
	    		return sequentialOpsStack.size();
	    	case PARALLEL:
	    		return parallelOpsStack.size();
	    	default:
	    		throw new RuntimeException("[ProcessingStack.size] Invalid StackMode set");
    	}
    }
    
    /** Returns the size of the specified stack */
    public int size(StackMode stackMode) {
    	switch (stackMode) {
	    	case SEQUENTIAL:
	    		return sequentialOpsStack.size();
	    	case PARALLEL:
	    		return parallelOpsStack.size();
	    	default:
	    		throw new RuntimeException("[ProcessingStack.size] Invalid StackMode given");
    	}
    }
}
