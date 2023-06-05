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

package com.teragrep.pth10.ast.commands.transformstatement.rex4j;

import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StringNode;
import com.teragrep.pth10.ast.bo.Token;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.steps.rex4j.Rex4jStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that contains the necessary implemented visitor functions for the rex4j command.<br>
 *
 * Rex4j provides a way to extract data from fields and generate new fields based on the extracted
 * data.<br>
 *
 * Rex4j also has a replace mode (mode=sed) that can use sed-based syntax to replace
 * values in the given field. If no field is specified, field is set to "_raw" by default.
 */
public class Rex4jTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Rex4jTransformation.class);

    private List<String> traceBuffer = null;
    private Map<String, Object> symbolTable = new HashMap<>();
    ProcessingStack processingPipe = null;
    public Rex4jStep rex4jStep = null;

    public Rex4jTransformation(Map<String, Object> symbolTable, List<String> buf, ProcessingStack stack)
    {
        this.symbolTable = symbolTable;
        this.processingPipe = stack;
        this.traceBuffer = buf;
    }

    public Rex4jTransformation(Map<String, Object> symbolTable, List<String> buf, Dataset<Row> ds)
    {
        this.symbolTable = symbolTable;
        this.traceBuffer = buf;
    }

    public Rex4jTransformation(Map<String, Object> symbolTable, List<String> buf, Document doc)
    {
        this.symbolTable = symbolTable;
        this.traceBuffer = buf;
    }

    /**
     * Main visitor function, from where the rest of the parse tree for this command will be walked
     * @param ctx
     * @return
     */
    public Node visitRex4jTransformation(DPLParser.Rex4jTransformationContext ctx) {
        traceBuffer.add(ctx.getChildCount() + " Rex4jTransformation:" + ctx.getText());

        return rexTransformationEmitCatalyst(ctx);
    }

    /**
     * 	<pre>
		rex4jTransformation
	
        : COMMAND_MODE_REX4J (t_rex4j_fieldParameter)? ((regexStringType (t_rex4j_maxMatchParameter)? (t_rex4j_offsetFieldParameter)? )|( t_rex4j_modeSedParameter regexStringType))
	
        ;
        
        rex4j field=host "regex string" --OR-- rex4j field=host mode=sed "s\abc\def\g"
        </pre>
     */
    public Node rexTransformationEmitCatalyst(DPLParser.Rex4jTransformationContext ctx) {
    	Dataset<Row> ds = null;
        if (!this.processingPipe.isEmpty()) {
            ds = this.processingPipe.pop();
        }
        this.rex4jStep = new Rex4jStep(ds);

        Dataset<Row> res;
        String sedMode = null;
        String field = "_raw"; // The field that you want to extract information from.
        traceBuffer.add(ctx.getChildCount() + " Rex4jTransformation:" + ctx.getText());
        
        // Optional fieldname, default is _raw
        if(ctx.t_rex4j_fieldParameter()!= null) {
            field = visit(ctx.t_rex4j_fieldParameter()).toString();
        }
        traceBuffer.add("Field used for extraction: " + field);

        // Check if mode=sed is present or not
        if (ctx.t_rex4j_modeSedParameter() != null) {
            LOGGER.info("Rex4j sed mode parameter detected");
            sedMode = visit(ctx.t_rex4j_modeSedParameter()).toString();
        }

        // maxMatchParameter
        if (ctx.t_rex4j_maxMatchParameter() != null) {
            Node n = visit(ctx.t_rex4j_maxMatchParameter());
            int maxMatch = Integer.parseInt(n.toString());
            LOGGER.info("got parameter MaxMatch= " + maxMatch);
            this.rex4jStep.setMaxMatch(maxMatch);
        }

        DPLParser.RegexStringTypeContext string = ctx.regexStringType();
        String regexStr = Util.stripQuotes(string.getText());

        this.rex4jStep.setField(field);
        this.rex4jStep.setSedMode(sedMode);
        this.rex4jStep.setRegexStr(regexStr);

        res = this.rex4jStep.get();
        
        processingPipe.push(res);
        return new CatalystNode(res);
    }

    @Override public Node visitT_rex4j_fieldParameter(DPLParser.T_rex4j_fieldParameterContext ctx) {
        String s = ctx.getChild(1).getText();
        s=Util.stripQuotes(s);
        StringNode rv =  new StringNode(new Token(Type.STRING, s));
        return rv;
    }

    @Override public Node visitT_rex4j_maxMatchParameter(DPLParser.T_rex4j_maxMatchParameterContext ctx) {
        String s = ctx.getChild(1).getText();
        s=Util.stripQuotes(s);
        StringNode rv =  new StringNode(new Token(Type.STRING,s));
        LOGGER.info("visitT_rex4j_maxMatchParameter:"+rv);
        return rv;
    }
    @Override public Node visitT_rex4j_modeSedParameter(DPLParser.T_rex4j_modeSedParameterContext ctx) {
        TerminalNode sedMode = (TerminalNode) ctx.getChild(1);
        //DPLLexer.COMMAND_REX4J_MODE_REGEXP_REPLACE
        return  new StringNode(new Token(Type.STRING, sedMode.getSymbol().toString()));
    }
    @Override public Node visitT_rex4j_offsetFieldParameter(DPLParser.T_rex4j_offsetFieldParameterContext ctx) {
        throw new RuntimeException("rex4j_offsetFieldParameter not supported yet");
//        return visitChildren(ctx);
    }

}
