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

package com.teragrep.pth10.ast.commands.transformstatement;

import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth10.steps.stats.StatsStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base transformation class for the <code>stats</code> command
 * Performs the given aggregation on the given dataset via the ProcessingStack.
 */
public class StatsTransformation extends DPLParserBaseVisitor<Node> {
	private static final Logger LOGGER = LoggerFactory.getLogger(StatsTransformation.class);

	private Map<String, Object> symbolTable = null;
	private List<String> traceBuffer = null;
	private ProcessingStack processingPipe = null;
	private boolean aggregatesUsed = false;
	private String aggregateField = null;
	
	List<Object> queueOfAggregates = new ArrayList<>(); // contains all aggregate ColumnNodes
	
	List<String> byFields = null;
	List<Column> listOfByFields = null; // seq of fields to be used for groupBy

	public StatsStep statsStep = null;
	
	public StatsTransformation(Map<String, Object> symbolTable, List<String> buf, ProcessingStack stack) {
		this.symbolTable = symbolTable;
		this.traceBuffer = buf;
		this.processingPipe = stack;
	}
	
	public void setAggregatesUsed(boolean newValue) {
		this.aggregatesUsed = newValue;
	}
	
	public boolean getAggregatesUsed() {
		return this.aggregatesUsed;
	}
	
	/* 
	 * Command info:
	 * Performs the AggregateFunction with given fieldRenameInstruction and byInstruction
	 * Example command:
	 * index=index_A | stats avg(offset) AS avg_offset BY sourcetype
	 * 
	 * Tree:
	 * --------------StatsTransformation-------------------
	 * ---|------------------------|------------------|----------------------------|------------
	 * COMMAND_MODE_STATS aggregateFunction t_stats_fieldRenameInstruction t_stats_byInstruction
	 * ------------------------------------------------|--------------------------|-------------
	 * --------------------------------------COMMAND_STATS_MODE_AS fieldType--COMMAND_STATS_MODE_BY fieldListType
	 * */
	public Node visitStatsTransformation(DPLParser.StatsTransformationContext ctx) {
		Node rv = statsTransformationEmitCatalyst(ctx);
		return rv;
	}
	
	public Node statsTransformationEmitCatalyst(DPLParser.StatsTransformationContext ctx) {
		Node rv = null;
		Dataset<Row> ds = null;
		if (!this.processingPipe.isEmpty()) {
			ds = this.processingPipe.pop();
		}
		this.statsStep = new StatsStep(ds);

		// Process children
		// COMMAND_MODE_STATS t_stats_partitions? t_stats_allnum? t_stats_delim? t_stats_agg
		for (int i = 0; i < ctx.getChildCount(); ++i) {
			ParseTree child = ctx.getChild(i);
			LOGGER.info("Processing child: " + child.getText());
			if (child instanceof TerminalNode) {
				LOGGER.info("typeof child = TerminalNode");
				continue; /* Skip stats keyword */
			}
			else if (child instanceof DPLParser.T_stats_aggContext) {
				visit(child);
			}
			// FIXME Implement: t_stats_partitions , t_stats_allnum , t_stats_delim
		}


		List<Column> listOfCompleteAggregations = new ArrayList<>(); // contains all aggregate Columns
		
		for (int i = 0; i < queueOfAggregates.size(); ++i) {
			Object item = queueOfAggregates.get(i);
			
			// If next in queue is a column
			if (item instanceof ColumnNode) {
				
				// Check if out of index
				Object nextToItem = null;
				if (queueOfAggregates.size()-1 >= i+1) {
					nextToItem = queueOfAggregates.get(i + 1);
				}
				
				// Check for fieldRename
				if (nextToItem instanceof String) {
					listOfCompleteAggregations.add(((ColumnNode)item).getColumn().name((String)nextToItem));
					i++;
				}
				// No fieldRename
				else {
					listOfCompleteAggregations.add(((ColumnNode)item).getColumn());
				}
			}
			
		}

		this.statsStep.setListOfAggregationExpressions(listOfCompleteAggregations);
		this.statsStep.setListOfGroupBys(listOfByFields);
		ds = this.statsStep.get();

		processingPipe.push(ds);
		rv = new CatalystNode(ds);

		this.setAggregatesUsed(true);
		return rv;
	}
	
	// AS fieldType
	public Node visitT_stats_fieldRenameInstruction(DPLParser.T_stats_fieldRenameInstructionContext ctx) {
		return new StringNode(new Token(Type.STRING, ctx.getChild(1).getText()));
	}
	
	// BY fieldListType
	public Node visitT_stats_byInstruction(DPLParser.T_stats_byInstructionContext ctx) {
		// Child #0 "BY"
		// Child #1 fieldListType
		return visit(ctx.getChild(1));
	}
	
	// fieldListType : fieldType ((COMMA)? fieldType)*?
	public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
		List<String> fields = new ArrayList<>();
		ctx.children.forEach(child -> {
			String field = child.getText();
			fields.addAll(Arrays.asList(field.split(",")));
		});
		
		return new StringListNode(fields);
	}
	
	// t_stats_agg
	public Node visitT_stats_agg(DPLParser.T_stats_aggContext ctx) {
		AggregateFunction aggregateFunction = new AggregateFunction((Dataset<Row>)null, this.traceBuffer);
		
		ctx.children.forEach(child -> {
			// AS fieldType
			if (child instanceof DPLParser.T_stats_fieldRenameInstructionContext) {
				LOGGER.info("typeof child = fieldRenameInstructionCtx");
				queueOfAggregates.add(visit(child).toString());
			}
			// BY fieldListType
			else if (child instanceof DPLParser.T_stats_byInstructionContext) {
				LOGGER.info("typeof child = byInstructionCtx");
				byFields = ((StringListNode)visit(child)).asList();
				listOfByFields = byFields.stream().map(field -> {
					return functions.col(field);
				}).collect(Collectors.toList());
			}
			// other; aggregateFunction visit
			else if (child instanceof DPLParser.AggregateFunctionContext) {
				LOGGER.info("typeof child = AggregateFunctionCtx");
				queueOfAggregates.add((ColumnNode) aggregateFunction.visitAggregateFunction((DPLParser.AggregateFunctionContext) child));
			}
		});
		
		return null;
		
	}
}
