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

package com.teragrep.pth10.ast.commands.transformstatement.accum;

import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StringNode;
import com.teragrep.pth10.ast.bo.Token;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class AccumTransformation extends DPLParserBaseVisitor<Node> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AccumTransformation.class);

	private List<String> traceBuffer = null;
	private Dataset<Row> ds = null;
	private ProcessingStack processingPipe = null;
	private Document doc = null;
	
	private boolean aggregatesUsed = false;
	private String aggregateField = null;

	public AccumTransformation( List<String> buf, Dataset<Row> ds) {
		this.traceBuffer = buf;
		this.ds = ds;
	}
	
	public AccumTransformation( List<String> buf, ProcessingStack stack) {
		this.traceBuffer = buf;
		this.processingPipe = stack;
	}
	
	public boolean getAggregatesUsed() {
		return this.aggregatesUsed;
	}
	
	public String getAggregateField() {
		return this.aggregateField;
	}
	
	public void setAggregatesUsed(boolean newValue) {
		this.aggregatesUsed = newValue;
	}
	
	public void setAggregateField(String newValue) {
		this.aggregateField = newValue;
	}
	
	/* 
	 * -- Command info: --
	 * 
	 * accum <field> [AS <newfield>]
	 * 
	 * Each event where <field> is a number, the accum command calculates a running total or sum of the numbers.
	 * The results can be returned to the same field or a new field
	 * 
	 * Example:
	 * Origin field		|		Result field
	 * 1						1
	 * 2						3
	 * 3						6
	 * 4						10
	 * 5						15
	 * 
	 * -- Grammar rules: --
	 * 
	 * accumTransformation
	 * 	: COMMAND_MODE_ACCUM fieldType (t_accum_fieldRenameInstruction)?
	 *  ;
	 * 
	 * t_accum_fieldRenameInstruction
	 *  : COMMAND_ACCUM_MODE_AS fieldType
	 *  ;
	 * 
	 * COMMAND_MODE_ACCUM 
	 *  : 'accum' -> pushMode(COMMAND_ACCUM_MODE), pushMode(GET_FIELD)
	 *  ;
	 *  
	 * COMMAND_ACCUM_MODE_AS
	 *  : ('AS'|'as'|'As') -> pushMode(GET_FIELD)
	 *  ;
	 * */
	@Override
	public Node visitAccumTransformation(DPLParser.AccumTransformationContext ctx) {
		traceBuffer.add(String.format("Child count: %s in AccumTransformation: %s", ctx.getChildCount(), ctx.getText()));	
		Node rv;

		//throw new UnsupportedOperationException("accum is not yet implemented in catalyst emit mode");
		rv = AccumTransformationEmitCatalyst(ctx);

		return rv;
	}
	
	/**
	 * Emit catalyst from AccumTransformation
	 * @param ctx
	 * @return
	 */
	public Node AccumTransformationEmitCatalyst(DPLParser.AccumTransformationContext ctx) {
		// syntax: accum fieldType (t_accum_fieldRenameInstruction)?
		// child#:   0      1                    2
		
		String srcField = ctx.getChild(1).getText();
		String newField = null;
		
		// if accum fieldType t_accum_fieldRenameInstruction
		if (ctx.getChildCount() == 3) {
			ParseTree newFieldParseTree = ctx.getChild(2);
			
			if (newFieldParseTree instanceof DPLParser.T_accum_fieldRenameInstructionContext) {
				newField = visit(newFieldParseTree).toString();
			}
		}
		// Incorrect amount of params
		else if (ctx.getChildCount() != 2) {
			throw new RuntimeException("Error in accum command; wrong amount of params");
		}

		ds = processingPipe.pop(); // pop from ProcessingStack
		
		// Build catalyst
		
		LOGGER.info(ds.isStreaming() ? ">> accum is using a streaming dataframe, as expected" : "accum NO STREAMING DATAFRAME IS USED !!");
		
		/*if (!ds.isStreaming()) {
			// Only for static / batch data, not for streaming dataframe as non time-based windowing is not supported
			// Specify a window between current row and everything that came before it
			
			WindowSpec w = Window.rowsBetween(Window.unboundedPreceding(), Window.currentRow());
			Dataset<Row> res = ds.withColumn(newField != null ? newField : srcField, functions.sum(functions.col(srcField)).over(w));
			processingPipe.push(res);
			return new CatalystNode(res);
		}*/
		
		// Streaming DataFrame:
		
		// (flat)map groups with state
		Dataset<Row> dsWithEventTime = ds.withColumn("accum_int_event_time", functions.current_timestamp()).coalesce(1);  // TODO repartition to 1 or not?
		KeyValueGroupedDataset<Timestamp, Row> kvgds = dsWithEventTime.groupByKey(
									(r) -> 
									{
										return (Timestamp) r.getAs("accum_int_event_time");
									}, 
									Encoders.TIMESTAMP());
		
		// Initialize AccumulatedSum
		AccumulatedSum as = new AccumulatedSum();
		
		List<Row> arrayOfAccumRows = new ArrayList<Row>();
		final String f_newField = newField;
		
		// Output schema changes depending on if source field is the target field or if there is a new target field given
		ExpressionEncoder<Row> outputEncoder = RowEncoder.apply(dsWithEventTime.schema());
		if (newField != null) {
			outputEncoder = RowEncoder.apply(dsWithEventTime.schema().add(newField, DataTypes.LongType));
		}
		
		Dataset<Row> res1 = kvgds.flatMapGroupsWithState((key, value, state) -> 
		{
			while (value.hasNext()) {
				Row r = value.next();

				// If source values are in string format, try to parse
				// otherwise, just cast as long
				// Also need to make sure that if source field is StringType,
				// the accum result needs to be StringType too if returned to src field
				Long val = null;
				Object srcFieldVal = r.getAs(srcField);
				Boolean valueOriginallyString = false;
				
				if (srcFieldVal instanceof String) {
					valueOriginallyString = true;
					val = Long.parseLong((String)srcFieldVal);
				}
				else {
					val = (Long) srcFieldVal;
				}
				
				as.addNumber(val); /* Add current row's source value to accumulator */
				
				// Append result of accumulator to input rows
				List<Object> rowContents = new ArrayList<Object>();
				String[] fieldNamesForRow = r.schema().fieldNames();
				for (int i = 0; i < r.length(); i++) {
					// Replace original source field with accum result field if new field was not
					// given in the initial command, otherwise add the original
					if (f_newField == null && fieldNamesForRow[i].equals(srcField)) {
						rowContents.add(valueOriginallyString ? as.calculateSum().toString() : as.calculateSum()); /* Add current sum to row */
					}
					else {
						rowContents.add(r.get(i));
					}
				}
				// Add accumulated sum to new field if it was given in the initial command
				if (f_newField != null) rowContents.add(as.calculateSum()); /* Add current sum to row (if new field) */
				
				arrayOfAccumRows.add(RowFactory.create(rowContents.toArray()));
			}
			
			// return result as iterator
			return arrayOfAccumRows.iterator();
		}, this.aggregatesUsed ? OutputMode.Complete() : OutputMode.Append(), RowEncoder.apply(dsWithEventTime.schema()), outputEncoder, GroupStateTimeout.NoTimeout());
		
		
		// Drop accum_int_event_time as it was used for accum, was not present in source dataset
		Dataset<Row> res = res1.drop("accum_int_event_time");
		processingPipe.push(res);		
		return new CatalystNode(res);
	}

	/**
	 * Emit XML from AccumTransformation
	 * @param ctx
	 * @return
	 */
	public Node AccumTransformationEmitXml(DPLParser.AccumTransformationContext ctx) {
		// TODO Implement XML emit mode
		return null;
	}
	
	/**
	 * t_accum_fieldRenameInstruction
	 *  : COMMAND_ACCUM_MODE_AS fieldType
	 *  ;
	 */
	public Node visitT_accum_fieldRenameInstruction(DPLParser.T_accum_fieldRenameInstructionContext ctx) {
		// child #0 is the keyword "AS"
		// child #1 is fieldType
		
		String newFieldName = ctx.getChild(1).getText();
		return new StringNode(new Token(Type.STRING, newFieldName));
	}
}
