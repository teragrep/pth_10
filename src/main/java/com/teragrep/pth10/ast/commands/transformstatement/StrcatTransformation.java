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
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.steps.strcat.StrcatStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base transformation class for the command <code>strcat</code><br>
 * Used to concatenate two or more field values and/or string literals into a destination field
 */
public class StrcatTransformation extends DPLParserBaseVisitor<Node> {
	
	private List<String> traceBuffer = null;
	private Map<String, Object> symbolTable = null;
	private Dataset<Row> ds = null;
	private ProcessingStack processingPipe = null;
	private Document doc = null;
	public StrcatStep strcatStep = null;

	
	public StrcatTransformation(Map<String, Object> symbolTable, List<String> buf, Dataset<Row> ds) {
		this.symbolTable = symbolTable;
		this.traceBuffer = buf;
		this.ds = ds;
	}
	
	public StrcatTransformation(Map<String, Object> symbolTable, List<String> buf, ProcessingStack stack) {
		this.symbolTable = symbolTable;
		this.traceBuffer = buf;
		this.processingPipe = stack;
	}
	
	/**
	 * <pre>
	 * -- Command info: --
	 * 
	 * {@literal strcat [allrequired=<bool>] <source-fields> <dest-field>}
	 * Concatenates string values from 2 or more fields, combines string values and
	 * literals into a new field. The destination field name is specified at the end of
	 * the strcat command. allrequired is not a required argument, and it can be omitted.
	 * 
	 * -- Grammar rules: --
	 * 
	 * strcatTransformation
	 *  : COMMAND_MODE_STRCAT (t_strcat_allrequiredParameter)? t_strcat_srcfieldsParameter fieldType
	 *  ;
	 * 
	 * t_strcat_allrequiredParameter
	 *  : COMMAND_STRCAT_MODE_ALLREQUIRED booleanType
	 *  ;
	 *  
	 * t_strcat_srcfieldsParameter
	 *  : (fieldType | stringType) (fieldType | stringType)+
	 *  ;
	 *  
	 * -- SQL: --
	 * 
	 * strcat allRequired=bool field1 field2 ... fieldN destField
	 *  to
	 * SELECT CONCAT(field1, field2, ..., fieldN) AS destField FROM ˇtemporaryDPLViewˇ
	 *  </pre>
	 * */
	@Override
	public Node visitStrcatTransformation(DPLParser.StrcatTransformationContext ctx) {
		traceBuffer.add(String.format("Child count: %s in StrcatTransformation: %s", ctx.getChildCount(), ctx.getText()));
		return strcatTransformationEmitCatalyst(ctx);
	}
	
	/**
	 * Emit catalyst from strcatTransformation
	 * @param ctx StrcatTransformationContext
	 * @return CatalystNode containing resultset
	 */
	public Node strcatTransformationEmitCatalyst(DPLParser.StrcatTransformationContext ctx) {
		// syntax: strcat allrequired src-fields dest-field
		// child#	0		1			2			3
		Dataset<Row> ds = null;
		if (!this.processingPipe.isEmpty()) {
			ds = this.processingPipe.pop();
		}
		this.strcatStep = new StrcatStep(ds);
		
		ParseTree allRequiredParseTree = ctx.getChild(1);
		
		String allRequired = null;
		Boolean allRequiredAsBoolean = false; // default is false based on example docs
		
		List<String> srcFields = null;
		int numberOfSrcFields = 0;
		
		String destField = null;

		// check if child#1 is allRequired parameter, as it can be optional
		if (allRequiredParseTree instanceof DPLParser.T_strcat_allrequiredParameterContext) {
			traceBuffer.add("strcatTransformation: catalyst emit mode; allrequiredParameter found");
			
			// visit allRequiredParameter
			allRequired = visit(allRequiredParseTree).toString();
			
			// convert allRequired to boolean
			if (allRequired.equalsIgnoreCase("t") || allRequired.equalsIgnoreCase("true")) { 
				allRequiredAsBoolean = true;
			}
			
			numberOfSrcFields = ctx.getChild(2).getChildCount();

			srcFields = ((StringListNode)visit(ctx.getChild(2))).asList();
			destField = Util.stripQuotes(ctx.getChild(3).getText());
		}
		// Otherwise, the second child (child#1) will be t_strcat_srcfieldsParameter
		else {
			traceBuffer.add("strcatTransformation: catalyst emit mode; no allrequiredParameter");

			srcFields = ((StringListNode)visit(allRequiredParseTree)).asList();
			destField = Util.stripQuotes(ctx.getChild(2).getText());
		}

		// Build catalyst
		this.strcatStep.setAllRequired(allRequiredAsBoolean);
		this.strcatStep.setDestField(destField);
		this.strcatStep.setListOfFields(srcFields);
		this.strcatStep.setNumberOfSrcFieldsOriginally(numberOfSrcFields);
		Dataset<Row> res = this.strcatStep.get();

		processingPipe.push(res);
		return new CatalystNode(res);
	}

	/** <pre>
	 * t_strcat_allrequiredParameter
	 *  : COMMAND_STRCAT_MODE_ALLREQUIRED booleanType
	 *  ;
	 * 
	 *  If the parameter exists, the second child (child#1) contains the boolean type whether or not all source fields are required
	 *  </pre>
	 */
	@Override
	public Node visitT_strcat_allrequiredParameter(DPLParser.T_strcat_allrequiredParameterContext ctx) {
		String isAllRequired = ctx.getChild(1).getText();
		StringNode rv = new StringNode(new Token(Type.STRING, isAllRequired));
		return rv;
	}
	
	/** <pre>
	 * 	t_strcat_srcfieldsParameter
	 *   : (fieldType | stringType) (fieldType | stringType)+
	 *   ;
	 *  
	 *  Contains all the source fields, one or more.
	 *   Adds all fields into an array, while stripping quotes from each one of the fields.</pre>
	 */
	@Override
	public Node visitT_strcat_srcfieldsParameter(DPLParser.T_strcat_srcfieldsParameterContext ctx) {
		List<String> srcFields = new ArrayList<String>();
		ctx.children.forEach(child -> {
			String nameOfField = child.getText();
			srcFields.add(nameOfField);
		});
		
		StringListNode rv = new StringListNode(srcFields);
		return rv;
	}
}
