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

package com.teragrep.pth10.ast.commands.evalstatement;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.evalstatement.UDFs.*;
import com.teragrep.pth10.steps.eval.EvalStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base statement for evaluation functions.
 * Called from {@link com.teragrep.pth10.ast.commands.transformstatement.EvalTransformation}
 */
public class EvalStatement extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EvalStatement.class);

    private boolean debugEnabled = false;
    private List<String> traceBuffer = null;
    private Document doc = null;
    private DPLParserCatalystContext catCtx = null;
    ProcessingStack processingStack = null;

    private List<String> dplHosts = new ArrayList<>();
    private List<String> dplSourceTypes = new ArrayList<>();
    private boolean aggregatesUsed = false;
    private String aggregateField = null;

    private Stack<String> timeFormatStack = new Stack<>();

    // Symbol table for mapping DPL default column names
    private Map<String, String> symbolTable = new HashMap<>();
    
    // Boolean expression field name TODO
    private String booleanExpFieldName = null;
    public EvalStep evalStep = null;

    /**
     * Initialize evalStatement
     * @param catCtx Catalyst context object
     * @param stack ProcessingStack
     * @param buf traceBuffer for debugging purposes
     */
    public EvalStatement(DPLParserCatalystContext catCtx, ProcessingStack stack, List<String> buf) {
        this.doc = null;
        this.traceBuffer = buf;
        this.catCtx = catCtx;
        this.processingStack = stack;
    }

    /**
     * Are any aggregates done to the dataset being processed?
     * @return bool
     */
    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    public String getAggregateField() {
        return this.aggregateField;
    }

    /**
     * Main visitor function for evalStatement
     * @param ctx main parse tree
     * @return node
     */
    public Node visitEvalStatement(DPLParser.EvalStatementContext ctx) {
        Node rv = null;
        LOGGER.info("visitEvalStatement incoming:"+ctx.getText());

        traceBuffer.add("visitEvalStatementEmit Catalyst incoming:" + ctx.getText());
        rv = evalStatementEmitCatalyst(ctx);
        return rv;
    }

    /**
     * Deprecated XML emit function
     * @param ctx
     * @return node
     * @deprecated not used
     */
    private Node evalStatementEmitXml(DPLParser.EvalStatementContext ctx) {
//		if(debugEnabled)
        traceBuffer.add(ctx.getChildCount() + " visitEvalStatement(xml) " + ctx.getText() + " type="
                + ctx.getChild(0).getClass().getName());

        Node rv = null;
        Node left = visit(ctx.getChild(0));
        //if(LOGGER.isDebugEnabled())
        traceBuffer.add(" visitEvalStatement " + ctx.getChildCount() + " incoming logical expression:" + ctx.getText());
        if (ctx.getChildCount() == 1) {
            // leaf
            rv = left;
            traceBuffer.add(" visitEvalStatement(xml) return leaf:" + left);
        } else if (ctx.getChildCount() == 2) {
            throw new RuntimeException(
                    "Unbalanced evalStatement operation:" + ctx.getText());
        } else if (ctx.getChildCount() == 3) {
            // logical operation xxx AND/OR/XOR xxx
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Token op = getOperation((TerminalNode) ctx.getChild(1));
            traceBuffer.add("--X visitEvalStatement(xml) operation:" + operation.getText().toUpperCase());
            traceBuffer.add("--X visitEvalStatement(xml) op:" + op.getType().toString());
            Node right = visit(ctx.getChild(2));
            Element el = doc.createElement(operation.getText().toUpperCase());
            el.appendChild(((ElementNode) left).getElement());
            el.appendChild(((ElementNode) right).getElement());
            ElementNode eln = new ElementNode(el);
            rv = eln;
            traceBuffer.add("evalStatementEmitXml:" + eln.toString());
        }
        return (ElementNode) rv;
    }

    private Node evalStatementEmitCatalyst(DPLParser.EvalStatementContext ctx) {
//		if(debugEnabled)
        traceBuffer.add(ctx.getChildCount() + " visitEvalStatement(Catalyst) " + ctx.getText() + " type="
                + ctx.getChild(0).getClass().getName());

        Node rv = null;
        Node left = visit(ctx.getChild(0));
        //if(LOGGER.isDebugEnabled())
        traceBuffer.add(" visitEvalStatement " + ctx.getChildCount() + " incoming logical expression:" + ctx.getText());
        if (ctx.getChildCount() == 1) {
            // leaf
            rv = left;
            traceBuffer.add(" visitEvalStatement(catalyst) return leaf:" + left);
        } else if (ctx.getChildCount() == 2) {
            throw new RuntimeException(
                    "Unbalanced evalStatement operation:" + ctx.getText());
        } else if (ctx.getChildCount() == 3) {
            // logical operation xxx AND/OR/XOR xxx
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Token op = getOperation((TerminalNode) ctx.getChild(1));
            traceBuffer.add("--X visitEvalStatement(catalyst) operation:" + operation.getText().toUpperCase());
            traceBuffer.add("--X visitEvalStatement(catalyst) op:" + op.getType().toString());
            LOGGER.info("--X visitEvalStatement(catalyst) operation:" + operation.getText().toUpperCase());
            LOGGER.info("--X visitEvalStatement(catalyst) op:" + op.getType().toString());
            Node right = visit(ctx.getChild(2));

            Column col = ((ColumnNode) left).getColumn();
            Column r = ((ColumnNode) right).getColumn();
            switch (operation.getSymbol().getType()) {
                case DPLLexer.EVAL_LANGUAGE_MODE_EQ: {
                    rv = new ColumnNode(col.equalTo(r));
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                    rv = new ColumnNode(col.notEqual(r));
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                    rv = new ColumnNode(col.lt(r));
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LTE: {
                    rv = new ColumnNode(col.leq(r));
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                    rv = new ColumnNode(col.gt(r));
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                    rv = new ColumnNode(col.geq(r));
                    break;
                }
            }
            traceBuffer.add("evalStatementEmitCatalyst:" + rv.toString());
        }
        return (ColumnNode) rv;
    }

    public Node visitL_evalStatement_subEvalStatement(DPLParser.L_evalStatement_subEvalStatementContext ctx) {
        traceBuffer.add(ctx.getChildCount() + " -XXX- VisitSubEvalStatements:" + ctx.getText());
        LOGGER.info(ctx.getChildCount() + " -XXX- VisitSubEvalStatements:" + ctx.getChild(0).getText());
        // Consume parenthesis and return actual evalStatement
        Node rv =visit(ctx.getChild(0));
        LOGGER.info(ctx.getChildCount() + " -XXX- VisitSubEvalStatements rv:" + rv+ " class="+rv.getClass().getName());
        //return new ColumnNode(functions.expr("true"));
        return rv;
    }
    @Override public Node visitEvalFunctionStatement(DPLParser.EvalFunctionStatementContext ctx) {
        traceBuffer.add(ctx.getChildCount() + "VisitEvalFunctionStatement:" + ctx.getText());
        Node rv =visit(ctx.getChild(0));
        traceBuffer.add(ctx.getChildCount() + "VisitEvalFunctionStatement rv:" + rv);
        return rv;
    }

    /**
     * evalCompareStatement : (decimalType|fieldType|stringType)
     * (DEQ|EQ|LTE|GTE|LT|GT|NEQ|LIKE|Like) evalStatement ;
     **/
    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        traceBuffer.add("visitEvalCompareStatement incoming:" + ctx.getText());
        Node rv = evalCompareStatementEmitCatalyst(ctx);

        return rv;
    }

    /**
     * evalCompareStatement : (decimalType|fieldType|stringType)
     * (DEQ|EQ|LTE|GTE|LT|GT|NEQ|LIKE|Like) evalStatement ;
     **/
    private Node evalCompareStatementEmitCatalyst(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        Node rv = null;
        Column lCol = null;
        Column rCol = null;
        traceBuffer.add(ctx.getChildCount() + " EvalCompareStatementEmit(Catalyst) " + ctx.getText());
        Node left = visit(ctx.getChild(0));
        if(left != null){
            lCol=((ColumnNode)left).getColumn();
        }
        Node right =  visit(ctx.getChild(2));
        if(right != null){
            rCol=((ColumnNode)right).getColumn();
        }
        // Add operation between columns operation =,<,>,......
        Column col  = addOperation(lCol, (TerminalNode)ctx.getChild(1), rCol);
        rv = new ColumnNode(col);
        traceBuffer.add("evalCompareStatement rv:" + col.expr().sql());
        return rv;
    }

    /**
     * Converts a {@link TerminalNode} containing an operation into a {@link Token}
     * @param operation TerminalNode of an operation
     * @return Token
     */
    private Token getOperation(TerminalNode operation) {
        Token op = null;
        switch (operation.getSymbol().getType()) {
            case DPLLexer.EVAL_LANGUAGE_MODE_EQ: {
                op = new Token(Type.EQUALS);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                op = new Token(Type.NOT_EQUALS);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                op = new Token(Type.GT);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                op = new Token(Type.GE);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                op = new Token(Type.LT);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_LTE: {
                op = new Token(Type.LE);
                break;
            }
            default: {
                traceBuffer.add("Unknown operation:" + operation.getSymbol().getType());
            }
        }
        return op;
    }

    /**
     * Generates a column based on a source, value and operation
     * @param source Left hand side
     * @param operation Operation
     * @param value Right hand side
     * @return Final resulting column
     */
    private Column addOperation(Column source, TerminalNode operation, Column value) {
        Column rv = null;
        switch (operation.getSymbol().getType()) {
            case DPLLexer.EVAL_LANGUAGE_MODE_EQ:
            case DPLLexer.EVAL_LANGUAGE_MODE_DEQ:{
                rv = source.equalTo(value);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                rv = source.notEqual(value);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                rv = source.gt(value);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                rv = source.geq(value);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                rv = source.lt(value);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_LTE: {
                rv = source.leq(value);
                break;
            }
            default: {
                traceBuffer.add("Unknown operation:" + operation.getSymbol().getType());
                throw new RuntimeException("EvalStatement Unknown operation:" + operation.getSymbol());
            }
        }
        return rv;
    }

    public Node visitL_evalStatement_evalLogicStatement(DPLParser.L_evalStatement_evalLogicStatementContext ctx) {
        traceBuffer.add("visitEvalLogicStatement incoming:" + ctx.getText());
        Node rv = evalLogicStatementEmitCatalyst(ctx);
        return rv;
    }

    private Node evalLogicStatementEmitCatalyst(DPLParser.L_evalStatement_evalLogicStatementContext ctx) {
        Node rv = null;
        Column lCol = null;
        Column rCol = null;
        traceBuffer.add(ctx.getChildCount() + " visitEvalLogicStatement(Catalyst) incoming:" + ctx.getText());
        LOGGER.info(ctx.getChildCount() + " visitEvalLogicStatement(Catalyst) incoming:" + ctx.getText());
        Node l =  visit(ctx.getChild(0));
        LOGGER.info(" visitEvalLogicStatement(Catalyst) left:" + l.getClass().getName());
        if (ctx.getChildCount() == 1) {
            // leaf
            rv = l;
        } else if (ctx.getChildCount() == 2) {
            // Should not come here at all
            Node r = visit(ctx.getChild(1));
            rv = r;
        } else if (ctx.getChildCount() == 3) {
            // logical operation xxx AND/OR/XOR xxx
            TerminalNode op = (TerminalNode) ctx.getChild(1);
            Token oper = null;

            //traceBuffer.add("Operation="+op+" symbol:"+op.getSymbol()+ " text:"+op.getSymbol().getText());
            LOGGER.info("Operation="+op+" symbol:"+op.getSymbol()+ " text:"+op.getSymbol().getText());
            if (op.getSymbol().getType() == DPLLexer.EVAL_LANGUAGE_MODE_AND) {
                oper = new Token(Type.AND);
            }
            if (op.getSymbol().getType() == DPLLexer.EVAL_LANGUAGE_MODE_OR) {
                oper = new Token(Type.OR);
            }
            traceBuffer.add("Operation token=" + oper);
            Node r = visit(ctx.getChild(2));
            LOGGER.info(" visitEvalLogicStatement(Catalyst) right:" + r.getClass().getName());

            if(l instanceof ColumnNode && r instanceof ColumnNode){
                Column col=null;
                Column lcol = ((ColumnNode) l).getColumn();
                Column rcol = ((ColumnNode) r).getColumn();
                if (op.getSymbol().getType() == DPLLexer.EVAL_LANGUAGE_MODE_AND) {
                    col=rcol.and(rcol);
                }
                if (op.getSymbol().getType() == DPLLexer.EVAL_LANGUAGE_MODE_OR) {
                    col=lcol.or(rcol);
                }
                LOGGER.info("visitEvalLogicStatement(Catalyst) with oper="+col.expr().sql());
                rv=new ColumnNode(col);
            }
        }
        traceBuffer.add(" EvalLogicStatement generated:" + rv.toString());
        LOGGER.info(" EvalLogicStatement(Catalyst) generated:" + rv.toString()+" class="+rv.getClass().getName());
        return rv;
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
    	LOGGER.info("visit normal field type");
        traceBuffer.add("Visit fieldtype:" + ctx.getChild(0).getText());
        Node rv = null;

        Column col = functions.lit(ctx.getChild(0).getText()); //new Column(ctx.getChild(0).getText());
        rv = new ColumnNode(col);
        traceBuffer.add("visitFieldType rv:" + col.expr().sql());

        return rv;
    }

    /**
     * subEvalStatement : PARENTHESIS_L EvalStatement PARENTHESIS_R
     * ;
     */
    @Override
    public Node visitSubEvalStatement(DPLParser.SubEvalStatementContext ctx) {
        traceBuffer.add("SubEvalStatement:" + ctx.getText());
        LOGGER.info("SubEvalStatement:" + ctx.getText());
        Node rv = subEvalStatementEmitCatalyst(ctx);
        return rv;
    }

    private Node subEvalStatementEmitCatalyst(DPLParser.SubEvalStatementContext ctx) {
        traceBuffer.add("SubEvalStatement(Catalyst):" + ctx.getText());
        return visit(ctx.getChild(1));
    }

    @Override
    public Node visitT_eval_evalParameter(DPLParser.T_eval_evalParameterContext ctx) {
        Node rv = null;
        Node n = visit(ctx.getChild(2));
        Node field = visit(ctx.getChild(0));

        traceBuffer.add("visitT_eval_evalParameter return:"+field.toString()+" node="+((ColumnNode)n).getColumn().expr().sql());

        // Step initialization and dataframe pop from stack
        Dataset<Row> ds = null;
        if (!this.processingStack.isEmpty()) {
            ds = this.processingStack.pop();
        }
        this.evalStep = new EvalStep(ds);
        this.evalStep.setLeftSide(field.toString()); // eval a = ...
        this.evalStep.setRightSide(((ColumnNode)n).getColumn()); // ... = right side

        ds = this.evalStep.get();

        processingStack.push(ds);
        rv = new CatalystNode(ds);

        return rv;
    }

    public Node visitEvalMethodIf(DPLParser.EvalMethodIfContext ctx) {
        Node rv = null;
        traceBuffer.add(ctx.getChildCount() + " EvalMethodIf:" + ctx.getText());

        rv = evalMethodIfEmitCatalyst(ctx);
        traceBuffer.add("visitEvalMethodIf rv:"+((ColumnNode)rv).getColumn().expr().sql());

        return rv;
    }

    private Node evalMethodIfEmitCatalyst(DPLParser.EvalMethodIfContext ctx) {
        Node rv;
        traceBuffer.add("EvalMethodIf(Catalyst) incoming:" + ctx.getText());
        Node ifPart = visit(ctx.getChild(2));
        traceBuffer.add("visitEvalMethod if=" + ifPart.getClass().getName());
        Column ifCol = ((ColumnNode)ifPart).getColumn(); // FIXME not used?
        Column resultTrue = ((ColumnNode)visit(ctx.getChild(4))).getColumn();
        Column condCol = ((ColumnNode)ifPart).getColumn();
        Column resultFalse = ((ColumnNode)visit(ctx.getChild(6))).getColumn();
        // Construct if-clause
        Column ifExpr = functions.when(condCol,resultTrue).otherwise(resultFalse);
//        ifExpr = functions.expr("CASE WHEN substring(_raw, 0, 7)='127.0.0.123' THEN length(_raw) ELSE 0 END");
        rv = new ColumnNode(ifExpr);
        traceBuffer.add("evalMethodIfEmitCatalyst rv:" + ifExpr.expr().sql());
        return rv;
    }

    /**
     * substring() eval method
     * Takes a substring out of the given string based on given indices
     * @param ctx
     * @return column node
     */
    @Override
    public Node visitEvalMethodSubstr(DPLParser.EvalMethodSubstrContext ctx) {
        Node rv = evalMethodSubstrEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodSubstrEmitCatalyst(DPLParser.EvalMethodSubstrContext ctx) {
        ColumnNode rv;
        traceBuffer.add(ctx.getChildCount() + " EvalMethodSubstr(Catalyst):" + ctx.getText());
        String par1=visit(ctx.getChild(2)).toString();
        String par2=visit(ctx.getChild(4)).toString();
        String par3=visit(ctx.getChild(6)).toString();
        rv = new ColumnNode(functions.substring(new Column(par1),Integer.parseInt(par2),Integer.parseInt(par3)));
        traceBuffer.add("Generate EvalSubstr:" + rv.getColumn().expr().sql());
        return rv;
    }

    /**
     * true() eval method
     * returns TRUE
     * @param ctx
     * @return column node
     */
    public Node visitEvalMethodTrue(DPLParser.EvalMethodTrueContext ctx) {
        Node rv = null;
        traceBuffer.add("Generate EvalMethodTrue:" + ctx.getText());
        rv = evalMethodTrueEmitCatalyst(ctx);

        return rv;
    }
    
    private Node evalMethodTrueEmitCatalyst(DPLParser.EvalMethodTrueContext ctx) {
        traceBuffer.add("Generate EvalMethodTrue(Catalyst):" + ctx.getText());
        Column col = functions.lit(true);
        traceBuffer.add("Generate EvalMethodTrue(Catalyst): rv="+col.expr().sql());
        return new ColumnNode(col);
    }

    /**
     * false() eval method
     * returns FALSE
     * @param ctx
     * @return column node
     */
    public Node visitEvalMethodFalse(DPLParser.EvalMethodFalseContext ctx) {
        Node rv = null;
        traceBuffer.add("Generate EvalMethodFalse:" + ctx.getText());
        rv = evalMethodFalseEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodFalseEmitCatalyst(DPLParser.EvalMethodFalseContext ctx) {
        traceBuffer.add("Generate EvalMethodFalse(Catalyst):" + ctx.getText());
        Column col = functions.lit(false);//functions.exp("false");
        traceBuffer.add("Generate EvalMethodFalse(Catalyst) rv:"+col.expr().sql());
        return new ColumnNode(col);
    }

    /**
     * null() eval method
     * Returns NULL
     * @param ctx
     * @return column node
     */
    @Override public Node visitEvalMethodNull(DPLParser.EvalMethodNullContext ctx) {
    	LOGGER.info("Visit eval method null");
    	Node rv = evalMethodNullEmitCatalyst(ctx);
    	
    	return rv;
    }
    
    private Node evalMethodNullEmitCatalyst(DPLParser.EvalMethodNullContext ctx) {
    	Column col = functions.lit(null).cast("string");
    	return new ColumnNode(col);
    }

    /**
     * nullif() eval method
     * Returns NULL if x==y, otherwise x
     * @param ctx
     * @return column node
     */
    @Override public Node visitEvalMethodNullif(DPLParser.EvalMethodNullifContext ctx) {
    	Node rv = evalMethodNullifEmitCatalyst(ctx);
    	
    	return rv;
    }

    private Node evalMethodNullifEmitCatalyst(DPLParser.EvalMethodNullifContext ctx) {
    	Node rv = null;
    	
    	// Get params x and y
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column yCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
    	
    	// If x == y, return null
    	Column col = functions.when(xCol.equalTo(yCol), functions.lit(null).cast("string"));
    	// otherwise, return x
    	col = col.otherwise(xCol);
    	
    	rv = new ColumnNode(col);
    	return rv;
    }

    /**
     * searchmatch(x) eval method
     * Returns TRUE if the search string matches the event
     * @param ctx
     * @return column node
     */
	@Override
	public Node visitEvalMethodSearchmatch(DPLParser.EvalMethodSearchmatchContext ctx) {
		Node rv = evalMethodSearchmatchEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodSearchmatchEmitCatalyst(DPLParser.EvalMethodSearchmatchContext ctx) {
		Node rv = null;

		// searchmatch(x) : Returns TRUE if the search string (first and only argument) matches the event
		
		String searchStr = Util.stripQuotes(ctx.getChild(2).getText());
		// LOGGER.info("search string= " + searchStr);
		
		// fields array should contain x=... , y=..., etc.
		String[] fields = searchStr.split(" ");
		
		Column res = null; 			// final result will be in this column
		Column andColumn = null; 	// column used to build the AND logical statement
		
		for (int i = 0; i < fields.length; i++) {
			// LOGGER.info("field(" + i + ")= " + fields[i]);
			
			// Split to field and literal based on operator
			String[] operands = fields[i].split("(<=|>=|<|=|>)", 2);
			
			Column field = functions.col(operands[0].trim());
			Column literal = functions.lit(operands[1].trim());
			String literalString = operands[1].trim();	
			
			// Test if field equals/leq/geq/lt/gt to literal
			if (andColumn == null) {
				if (fields[i].contains("<=")) {
					andColumn = field.leq(literal);
				}
				else if (fields[i].contains(">=")) {
					andColumn = field.geq(literal);
				}
				else if (fields[i].contains("<")) {
					andColumn = field.lt(literal);
				}
				else if (fields[i].contains("=")) {
					if (!literalString.equals("*")) 
						andColumn = field.equalTo(literal);
					else 
						andColumn = field.isNotNull();
				}
				else if (fields[i].contains(">")) {
					andColumn = field.gt(literal);
				}
			}
			else {
				if (fields[i].contains("<=")) {
					andColumn = andColumn.and(field.leq(literal));
				}
				else if (fields[i].contains(">=")) {
					andColumn = andColumn.and(field.geq(literal));
				}
				else if (fields[i].contains("<")) {
					andColumn = andColumn.and(field.lt(literal));
				}
				else if (fields[i].contains("=")) {
					if (!literalString.equals("*")) 
						andColumn = andColumn.and(field.equalTo(literal));
					else 
						andColumn = andColumn.and(field.isNotNull());
				}
				else if (fields[i].contains(">")) {
					andColumn = andColumn.and(field.gt(literal));
				}
				
			}
		}
		
		// When everything equals, res=true, otherwise res=false
		res = functions.when(andColumn, functions.lit(true)).otherwise(functions.lit(false));
		
		rv = new ColumnNode(res);
		return rv;
	}


    /**
     * now() eval method
     * Returns the current system time
     * @param ctx
     * @return column node
     */
    public Node visitEvalMethodNow(DPLParser.EvalMethodNowContext ctx) {
        Node rv = evalMethodNowEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodNowEmitCatalyst(DPLParser.EvalMethodNowContext ctx) {
    	Node rv = null;
    	
    	Column res = functions.lit(System.currentTimeMillis() / 1000L);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * len() eval method
     * Returns the length of the field contents
     * @param ctx
     * @return column node
     */
    public Node visitEvalMethodLen(DPLParser.EvalMethodLenContext ctx) {
        Node rv = evalMethodLenEmitCatalyst(ctx);
        return rv;
    }

    public Node evalMethodLenEmitCatalyst(DPLParser.EvalMethodLenContext ctx) {
        ColumnNode rv;
        String inField=visit(ctx.getChild(2)).toString();
        traceBuffer.add(ctx.getChildCount() + " Generate EvalMethodLen len(" + inField+")");
        rv = new ColumnNode(functions.length(new Column(inField)));
        traceBuffer.add("Generate EvalMethodLen(Catalyst) generated:"+rv.getColumn().expr().sql());
        return rv;
    }

    /**
     * lower() eval method
     * Returns the field contents in all lowercase characters
     * @param ctx
     * @return column node
     */
    @Override public Node visitEvalMethodLower(DPLParser.EvalMethodLowerContext ctx) {
    	Node rv = evalMethodLowerEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodLowerEmitCatalyst(DPLParser.EvalMethodLowerContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column res = functions.lower(xCol);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * upper() eval method
     * Returns the field contents in all uppercase characters
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodUpper(DPLParser.EvalMethodUpperContext ctx) {
    	Node rv = evalMethodUpperEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodUpperEmitCatalyst(DPLParser.EvalMethodUpperContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column res = functions.upper(xCol);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * urldecode() eval method
     * Returns the given URL decoded, e.g. replaces %20 etc. with appropriate human readable characters
     * @param ctx
     * @return column node
     */
    @Override public Node visitEvalMethodUrldecode(DPLParser.EvalMethodUrldecodeContext ctx) {
    	Node rv = evalMethodUrldecodeEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodUrldecodeEmitCatalyst(DPLParser.EvalMethodUrldecodeContext ctx) {
    	Node rv = null;
    	
    	Column encodedUrl = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Register and call urlDecode UDF
    	UserDefinedFunction urlDecode = functions.udf(new UrlDecode(), DataTypes.StringType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("urlDecode", urlDecode);
    	
    	Column res = functions.callUDF("urlDecode", encodedUrl);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * ltrim() eval method
     * Returns the field contents with trimString trimmed from left side if given, otherwise spaces and tabs
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodLtrim(DPLParser.EvalMethodLtrimContext ctx) {
    	Node rv = evalMethodLtrimEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodLtrimEmitCatalyst(DPLParser.EvalMethodLtrimContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	String trimString = null;
    	
    	if (ctx.getChildCount() > 4) {
    		// Set trimString if given in command
    		trimString = ctx.getChild(4).getText();
    	}
    	
    	// Without trimString, remove spaces and tabs
    	Column res = trimString == null ? functions.ltrim(xCol, "\t ") : functions.ltrim(xCol, trimString);
    	
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * replace() eval method
     * Returns a string with replaced parts as defined by the regex string
     * @param ctx
     * @return column node
     */
    @Override public Node visitEvalMethodReplace(DPLParser.EvalMethodReplaceContext ctx) {
    	Node rv = evalMethodReplaceEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodReplaceEmitCatalyst(DPLParser.EvalMethodReplaceContext ctx) {
    	Node rv = null;
    	
    	// Substitutes string Z for every occurrence of regex string Y in string X
    	// replace ( x , y , z )
    	
    	if (ctx.getChildCount() != 8) {
    		throw new UnsupportedOperationException("Eval method 'replace' requires three arguments: source string, regex string and substitute string.");
    	}
    	
    	Column srcString = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column regexString = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
    	Column substituteString = ((ColumnNode) visit(ctx.getChild(6))).getColumn();
    	
    	Column res = functions.regexp_replace(srcString, regexString, substituteString);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }


    /**
     * rtrim() eval method
     * Returns the field contents with trimString trimmed from right side if given, otherwise spaces and tabs
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodRtrim(DPLParser.EvalMethodRtrimContext ctx) {
    	Node rv = evalMethodRtrimEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodRtrimEmitCatalyst(DPLParser.EvalMethodRtrimContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	String trimString = null;
    	
    	if (ctx.getChildCount() > 4) {
    		// Set trimString if given in command
    		trimString = ctx.getChild(4).getText();
    	}
    	
    	// Without trimString, remove spaces and tabs
    	Column res = trimString == null ? functions.rtrim(xCol, "\t ") : functions.rtrim(xCol, trimString);
    	
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * trim() eval method
     * Returns the field contents with trimString trimmed from both sides if given, otherwise spaces and tabs
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodTrim(DPLParser.EvalMethodTrimContext ctx) {
    	Node rv = evalMethodTrimEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodTrimEmitCatalyst(DPLParser.EvalMethodTrimContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	String trimString = null;
    	
    	if (ctx.getChildCount() > 4) {
    		// Set trimString if given in command
    		trimString = ctx.getChild(4).getText();
    	}
    	
    	// Without trimString, remove spaces and tabs
    	Column res = trimString == null ? functions.trim(xCol, "\t ") : functions.trim(xCol, trimString);
    	
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * split() eval method
     * Returns field split with delimiter
     * @param ctx
     * @return
     */
    public Node visitEvalMethodSplit(DPLParser.EvalMethodSplitContext ctx) {
        Node rv = evalMethodSplitEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodSplitEmitCatalyst(DPLParser.EvalMethodSplitContext ctx) {
    	Node rv = null;
    	
    	// Get field and the delimiter to use for split
    	Column field = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	String delimiter = ctx.getChild(4).getText();
    	
    	// Split field with spark built-in function
    	Column res = functions.split(field, Util.stripQuotes(delimiter));
    	
    	rv = new ColumnNode(res);
        return rv;
    }

    /**
     * relative_time() eval method
     * Returns timestamp based on given unix epoch and relative time modifier
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodRelative_time(DPLParser.EvalMethodRelative_timeContext ctx) {
		Node rv = evalMethodRelative_timeEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodRelative_timeEmitCatalyst(DPLParser.EvalMethodRelative_timeContext ctx) {
		Node rv = null;

		Column unixTime = ((ColumnNode) visit(ctx.getChild(2))).getColumn().cast("long");
		Column relativeTimeSpecifier = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
		
		// Register and call UDF Relative_time
		UserDefinedFunction Relative_timeUDF = functions.udf(new Relative_time(), DataTypes.LongType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("Relative_timeUDF", Relative_timeUDF);

		Column res = functions.callUDF("Relative_timeUDF", unixTime, relativeTimeSpecifier);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * strftime() eval method
     * Returns a timestamp based on given unix epoch and format string
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodStrftime(DPLParser.EvalMethodStrftimeContext ctx) {
		Node rv = evalMethodStrftimeEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodStrftimeEmitCatalyst(DPLParser.EvalMethodStrftimeContext ctx) {
		Node rv = null;

        LOGGER.debug(ctx.getChild(2).getText());
		Column unixTime = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		String formatStr = Util.stripQuotes(ctx.getChild(4).getText());
		
		// formatStr needs to function with example format, however spark uses
		// https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
		
		// Replace all occurrences of example date format to Spark date format
		// TODO There should be a better way of doing this.
		formatStr = formatStr
				// Date
				.replaceAll("%Y", "y") // full year
				.replaceAll("%m", "MM") // month 1-12
				.replaceAll("%d", "dd") // day 1-31
				.replaceAll("%b", "MMM") // abbrv. month name
				.replaceAll("%B", "MMMM") // full month name
				// Time
				.replaceAll("%H", "HH") // hour 0-23
				.replaceAll("%k", "H") // hour without leading zeroes
				.replaceAll("%M", "mm") // minute 0-59
				.replaceAll("%S", "ss") // second 0-59
				.replaceAll("%I", "hh") // hour 0-12
				.replaceAll("%p", "a") // am/pm
				.replaceAll("%T", "HH:mm:ss") // hour:min:sec
				.replaceAll("%f", "S") // microsecs
				// Time zone
				.replaceAll("%Z", "zz") // timezone abbreviation
				.replaceAll("%z", "XXX") // timezone offset +0000
				// Other
				.replaceAll("%%", "%"); // percent sign
		
		LOGGER.info("formatstr= " + formatStr);

        // call udf which converts possible timestamp/string to unixtime first
        //SparkSession sparkSession = SparkSession.builder().getOrCreate();
        //sparkSession.udf().register("udf_timetounixtime", new TimeToUnixTime(), DataTypes.LongType);

		Column res = functions.from_unixtime(unixTime, formatStr);

		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * strptime() eval method
     * Returns an unix epoch based on given timestamp and format string
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodStrptime(DPLParser.EvalMethodStrptimeContext ctx) {
		Node rv = evalMethodStrptimeEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodStrptimeEmitCatalyst(DPLParser.EvalMethodStrptimeContext ctx) {
		Node rv = null;

		Column stringTime = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		String formatStr = Util.stripQuotes(ctx.getChild(4).getText());
		
		// formatStr needs to function with example format, however spark uses
		// https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
		
		// Replace all occurrences of example date format to Spark date format
		// TODO There should be a better way of doing this.
		formatStr = formatStr
				// Date
				.replaceAll("%Y", "y") // full year
				.replaceAll("%m", "MM") // month 1-12
				.replaceAll("%d", "dd") // day 1-31
				.replaceAll("%b", "MMM") // abbrv. month name
				.replaceAll("%B", "MMMM") // full month name
				// Time
				.replaceAll("%H", "HH") // hour 0-23
				.replaceAll("%k", "H") // hour without leading zeroes
				.replaceAll("%M", "mm") // minute 0-59
				.replaceAll("%S", "ss") // second 0-59
				.replaceAll("%I", "hh") // hour 0-12
				.replaceAll("%p", "a") // am/pm
				.replaceAll("%T", "HH:mm:ss") // hour:min:sec
				.replaceAll("%f", "S") // microsecs
				// Time zone
				.replaceAll("%Z", "zz") // timezone abbreviation
				.replaceAll("%z", "XXX") // timezone offset +0000
				// Other
				.replaceAll("%%", "%"); // percent sign
		
		LOGGER.info("formatstr= " + formatStr);
		Column res = functions.unix_timestamp(stringTime, formatStr);
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * pow() eval method
     * Returns the field to the power of n
     * @param ctx
     * @return
     */
    public Node visitEvalMethodPow(DPLParser.EvalMethodPowContext ctx) {
    	Node rv = evalMethodPowEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodPowEmitCatalyst(DPLParser.EvalMethodPowContext ctx) {
    	LOGGER.info("Visiting eval method pow");
    	Node rv = null;
    	// child 2 and 4 are x and y
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column yCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
    	
    	Column col = functions.pow(xCol, yCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * abs() eval method
     * Returns absolute value
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodAbs(DPLParser.EvalMethodAbsContext ctx) {
    	Node rv = evalMethodAbsEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodAbsEmitCatalyst(DPLParser.EvalMethodAbsContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column col = functions.abs(xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * ceiling() / ceil() eval method
     * Returns the value rounded up
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodCeiling(DPLParser.EvalMethodCeilingContext ctx) {
    	Node rv = evalMethodCeilingEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodCeilingEmitCatalyst(DPLParser.EvalMethodCeilingContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.ceil(xCol);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * exact() eval method
     * Acts as a passthrough
     * More details: {@link #evalMethodExactEmitCatalyst(DPLParser.EvalMethodExactContext)}
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodExact(DPLParser.EvalMethodExactContext ctx) {
		Node rv = evalMethodExactEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodExactEmitCatalyst(DPLParser.EvalMethodExactContext ctx) {
		Node rv = null;

		// The current implementation of arithmetic operations
		// return the most accurate result by default.
		// Thus exact() is essentially pointless.
		// To achieve truncated/rounded results, for example sigfig(x) can be used.
		
		// This function will act as a passthrough to allow more compatibility for
		// existing commands.
		
		Column res = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
				
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * exp() eval method
     * Returns e^n
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodExp(DPLParser.EvalMethodExpContext ctx) {
    	Node rv = evalMethodExpEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodExpEmitCatalyst(DPLParser.EvalMethodExpContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.exp(xCol);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * floor() eval method
     * Rounds down to nearest integer
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodFloor(DPLParser.EvalMethodFloorContext ctx) {
    	Node rv = evalMethodFloorEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodFloorEmitCatalyst(DPLParser.EvalMethodFloorContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.floor(xCol);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * ln() eval method
     * Returns the natural logarithmic of n
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodLn(DPLParser.EvalMethodLnContext ctx) {
    	Node rv = evalMethodLnEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodLnEmitCatalyst(DPLParser.EvalMethodLnContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.log(xCol);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * log() eval method
     * Returns the nth logarithmic of given number
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodLog(DPLParser.EvalMethodLogContext ctx) {
    	Node rv = evalMethodLogEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodLogEmitCatalyst(DPLParser.EvalMethodLogContext ctx) {
    	Node rv = null;
    	
    	// log ( x , y )
    	
    	if (ctx.getChildCount() != 6) {
    		throw new UnsupportedOperationException("Eval method 'log' requires two arguments, x (Number from which the logarithm is calculated) and y (base).");
    	}
    	
    	Column numberCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	double base = Double.valueOf(ctx.getChild(4).getText());   	
    	Column res = functions.log(base, numberCol);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * cos() eval method
     * Returns the cosine of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodCos(DPLParser.EvalMethodCosContext ctx) {
		Node rv = evalMethodCosEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodCosEmitCatalyst(DPLParser.EvalMethodCosContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		Column res = functions.cos(xCol);
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * cosh() eval method
     * Returns the hyperbolic cosine of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodCosh(DPLParser.EvalMethodCoshContext ctx) {
		Node rv = evalMethodCoshEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodCoshEmitCatalyst(DPLParser.EvalMethodCoshContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		Column res = functions.cosh(xCol);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * acos() eval method
     * Returns arc cosine of the field value
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodAcos(DPLParser.EvalMethodAcosContext ctx) {
    	Node rv = evalMethodAcosEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodAcosEmitCatalyst(DPLParser.EvalMethodAcosContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column col = functions.acos(xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * acosh() eval methdo
     * Returns the inverse hyperbolic cosine of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodAcosh(DPLParser.EvalMethodAcoshContext ctx) {
		Node rv = evalMethodAcoshEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodAcoshEmitCatalyst(DPLParser.EvalMethodAcoshContext ctx) {
		Node rv = null;
		
		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// TODO Spark internal function acosh(x) available in >=3.1.0
		UserDefinedFunction acoshUDF = functions.udf(new InverseHyperbolicFunction("acosh"), DataTypes.DoubleType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("acoshUDF", acoshUDF);
		
		Column res = functions.callUDF("acoshUDF", xCol);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * sin() eval method
     * Returns the sine of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodSin(DPLParser.EvalMethodSinContext ctx) {
		Node rv = evalMethodSinEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodSinEmitCatalyst(DPLParser.EvalMethodSinContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		Column res = functions.sin(xCol);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * sinh() eval method
     * Returns hyperbolic sine of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodSinh(DPLParser.EvalMethodSinhContext ctx) {
		Node rv = evalMethodSinhEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodSinhEmitCatalyst(DPLParser.EvalMethodSinhContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		Column res = functions.sinh(xCol);
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * asin() eval method
     * Returns arc sine of the field value
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodAsin(DPLParser.EvalMethodAsinContext ctx) {
    	Node rv = evalMethodAsinEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodAsinEmitCatalyst(DPLParser.EvalMethodAsinContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column col = functions.asin(xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * asinh() eval method
     * Returns inverse hyperbolic sine of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodAsinh(DPLParser.EvalMethodAsinhContext ctx) {
		Node rv = evalMethodAsinhEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodAsinhEmitCatalyst(DPLParser.EvalMethodAsinhContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// TODO Spark internal function asinh(x) available in >=3.1.0
		UserDefinedFunction asinhUDF = functions.udf(new InverseHyperbolicFunction("asinh"), DataTypes.DoubleType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("asinhUDF", asinhUDF);
		
		Column res = functions.callUDF("asinhUDF", xCol);
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * tan() eval method
     * Returns the tangent of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodTan(DPLParser.EvalMethodTanContext ctx) {
		Node rv = evalMethodTanEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodTanEmitCatalyst(DPLParser.EvalMethodTanContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		Column res = functions.tan(xCol);
		rv = new ColumnNode(res);

		return rv;
	}


    /**
     * tanh() eval method
     * Returns the hyperbolic tangent of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodTanh(DPLParser.EvalMethodTanhContext ctx) {
		Node rv = evalMethodTanhEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodTanhEmitCatalyst(DPLParser.EvalMethodTanhContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		Column res = functions.tanh(xCol);
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * atan() eval method
     * Returns the arc tangent of the field value
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodAtan(DPLParser.EvalMethodAtanContext ctx) {
    	Node rv = evalMethodAtanEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodAtanEmitCatalyst(DPLParser.EvalMethodAtanContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column col = functions.atan(xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * atan2() eval method
     * Returns the arc tangent of Y,X
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodAtan2(DPLParser.EvalMethodAtan2Context ctx) {
		Node rv = evalMethodAtan2EmitCatalyst(ctx);
	    return rv;
	}

	private Node evalMethodAtan2EmitCatalyst(DPLParser.EvalMethodAtan2Context ctx) {
		Node rv = null;

		Column yCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		Column xCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
		
		Column res = functions.atan2(yCol, xCol);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * atanh() eval method
     * Returns the inverse hyperbolic tangent of the field value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodAtanh(DPLParser.EvalMethodAtanhContext ctx) {
		Node rv = evalMethodAtanhEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodAtanhEmitCatalyst(DPLParser.EvalMethodAtanhContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// TODO Spark internal function atanh(x) available in >=3.1.0
		UserDefinedFunction atanhUDF = functions.udf(new InverseHyperbolicFunction("atanh"), DataTypes.DoubleType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("atanhUDF", atanhUDF);
		
		Column res = functions.callUDF("atanhUDF", xCol);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * hypot() eval method
     * Returns the hypotenuse, when X and Y are the edges forming the 90 degree angle of a triangle
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodHypot(DPLParser.EvalMethodHypotContext ctx) {
		Node rv = evalMethodHypotEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodHypotEmitCatalyst(DPLParser.EvalMethodHypotContext ctx) {
		Node rv = null;

		Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		Column yCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
		
		Column res = functions.hypot(xCol, yCol);
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * pi() eval method
     * Returns constant pi to 11 digits of precision
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodPi(DPLParser.EvalMethodPiContext ctx) {
    	Node rv = evalMethodPiEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodPiEmitCatalyst(DPLParser.EvalMethodPiContext ctx) {
    	Node rv = null;
    	
    	// Return pi constant
    	final double PI = 3.14159265358d;
    	Column res = functions.lit(PI);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * min() eval method
     * Returns the minimum of the given arguments
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMin(DPLParser.EvalMethodMinContext ctx) {
		Node rv = evalMethodMinEmitCatalyst(ctx);
        return rv;
	}

	private Node evalMethodMinEmitCatalyst(DPLParser.EvalMethodMinContext ctx) {
		Node rv = null;

		// min ( x0 , x1 , x2 , ... , xn )
		List<Column> listOfColumns = new ArrayList<>();
		for (int i = 2; i <= ctx.getChildCount()-2; i = i + 2) {
			listOfColumns.add(((ColumnNode) visit(ctx.getChild(i))).getColumn());
		}
		
		Seq<Column> seqOfColumns = JavaConversions.asScalaBuffer(listOfColumns);
		Column res = functions.array_min(functions.array(seqOfColumns));
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * max() eval method
     * Returns the maximum of the given arguments
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMax(DPLParser.EvalMethodMaxContext ctx) {
		Node rv = evalMethodMaxEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMaxEmitCatalyst(DPLParser.EvalMethodMaxContext ctx) {
		Node rv = null;

		// max ( x0 , x1 , x2 , ... , xn )
		List<Column> listOfColumns = new ArrayList<>();
		for (int i = 2; i <= ctx.getChildCount()-2; i = i + 2) {
			listOfColumns.add(((ColumnNode) visit(ctx.getChild(i))).getColumn());
		}
		
		Seq<Column> seqOfColumns = JavaConversions.asScalaBuffer(listOfColumns);
		Column res = functions.array_max(functions.array(seqOfColumns));
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * random() eval method
     * Returns a pseudo-random integer from range 0 to 2^31 - 1
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodRandom(DPLParser.EvalMethodRandomContext ctx) {
    	Node rv = evalMethodRandomEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodRandomEmitCatalyst(DPLParser.EvalMethodRandomContext ctx) {
    	Node rv = null;
    	
    	// Register and call UDF randomNumber
    	UserDefinedFunction randomNumber = functions.udf(new RandomNumber(), DataTypes.IntegerType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("randomNumber", randomNumber);
    	
    	Column res = functions.callUDF("randomNumber");
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * sqrt() eval methdo
     * Returns the square root of the field value
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodSqrt(DPLParser.EvalMethodSqrtContext ctx) {
    	Node rv = evalMethodSqrtEmitCatalyst(ctx);
       	return rv;
    }
    
    private Node evalMethodSqrtEmitCatalyst(DPLParser.EvalMethodSqrtContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.sqrt(xCol);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * round() eval method
     * Returns x rounded to y decimal places, or integer if y missing
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodRound(DPLParser.EvalMethodRoundContext ctx) {
    	Node rv = evalMethodRoundEmitCatalyst(ctx);
      	return rv;
    }
    
    private Node evalMethodRoundEmitCatalyst(DPLParser.EvalMethodRoundContext ctx) {
    	Node rv = null;
    	
    	// round ( x , y )
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	int decimalPlaces = 0;
    	
    	if (ctx.getChildCount() > 4) {
    		decimalPlaces = Integer.valueOf(ctx.getChild(4).getText());
    	}
    	
    	Column res = functions.round(xCol, decimalPlaces);
    	rv = new ColumnNode(res);
    	
    	return rv;
    }

    /**
     * sigfig() eval method
     * Returns the field value reduced to the significant figures
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodSigfig(DPLParser.EvalMethodSigfigContext ctx) {
		Node rv = evalMethodSigfigEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodSigfigEmitCatalyst(DPLParser.EvalMethodSigfigContext ctx) {
		Node rv = null;

		// The computation for sigfig is based on the type of calculation that generates the number
		// * / result should have minimum number of significant figures of all of the operands
		// + - result should have the same amount of sigfigs as the least precise number of all of the operands
		
		
		// This column contains the result of the calculation
		Column calculation = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		// This is the original input given by the user
		String calcText = ctx.getChild(2).getText();
		
		// Split the input based on operators
		// For example, if user gives a command "offset * 1.1", it would be split into
		// "offset" and "1.1"
		String[] operands = calcText.split("\\*|\\+|-|/");	
			
		// Test whether or not the operand is numeric (0-9.0-9)
		Pattern numericPattern = Pattern.compile("^\\d+(\\.\\d*)?");
		List<Column> listOfCols = new ArrayList<>();
		for (int i = 0; i < operands.length; i++) {
			Matcher matcher = numericPattern.matcher(operands[i]);
			
			if (!matcher.matches()) {
				// Not numeric, add to listOfCols as col
				listOfCols.add(functions.col(operands[i]));
			}
			else {
				// Numeric, add as lit
				listOfCols.add(functions.lit(operands[i]));
			}
		}
		
		// Convert into seqOfCols because spark doesn't support java lists
		Seq<Column> seqOfCols = JavaConversions.asScalaBuffer(listOfCols);
		
		// Form an array from the seqOfCols to be given to the UDF
		Column array = functions.array(seqOfCols);
				
		// Register and call UDF Sigfig
		UserDefinedFunction SigfigUDF = functions.udf(new Sigfig(), DataTypes.DoubleType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("SigfigUDF", SigfigUDF);

		// Call UDF with calculation (result), original user input as string and array of operands as objects
		Column res = functions.callUDF("SigfigUDF", calculation, functions.lit(calcText), array);
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * case() eval method
     * Alternating conditions and values, returns the first value where condition is true
     * @param ctx
     * @return
     */
    public Node visitEvalMethodCase(DPLParser.EvalMethodCaseContext ctx) {
    	Node rv = evalMethodCaseEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodCaseEmitCatalyst(DPLParser.EvalMethodCaseContext ctx) {
    	Node rv = null;
    	
    	// case ( x , y , x2 , y2 , x3, y3 , ... )
    	
    	if (ctx.getChildCount() % 2 != 0) {
    		throw new UnsupportedOperationException("The amount of arguments given was invalid. Make sure each condition has a matching value given as an argument.");
    	}
    	
    	Column condition = null;
    	Column value = null;
    	Column res = null;

    	for (int i = 0; i < ctx.getChildCount(); ++i) {    
    		// Skip TerminalNode (keyword case, parenthesis, comma)
    		if (ctx.getChild(i) instanceof TerminalNode)
    			continue;
    		
    		condition = ((ColumnNode) visit(ctx.getChild(i))).getColumn();
    		value = ((ColumnNode) visit(ctx.getChild(i+2))).getColumn();
    		// Skip to i=i+2, so the value doesn't get read as a condition
    		i = i + 2;
    		
    		// When condition is true, return value. Otherwise leave as null.
    		if (res == null)
    			res = functions.when(condition.equalTo(functions.lit(true)), value);
    		else
    			res = res.when(condition.equalTo(functions.lit(true)), value);

    	}
    	
    	rv = new ColumnNode(res);
    	return rv;
    }


    /**
     * validate() eval method
     * Opposite of 'case(x,y)', returns the first y where x=false
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodValidate(DPLParser.EvalMethodValidateContext ctx) {
    	Node rv = evalMethodValidateEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodValidateEmitCatalyst(DPLParser.EvalMethodValidateContext ctx) {
    	Node rv = null;
    	
    	// validate ( x , y , x2 , y2 , x3, y3 , ... )
    	
    	if (ctx.getChildCount() % 2 != 0) {
    		throw new UnsupportedOperationException("The amount of arguments given was invalid. Make sure each condition has a matching value given as an argument.");
    	}
    	
    	Column condition = null;
    	Column value = null;
    	Column res = null;

    	for (int i = 0; i < ctx.getChildCount(); ++i) {    
    		// Skip TerminalNode (keyword validate, parenthesis, comma)
    		if (ctx.getChild(i) instanceof TerminalNode)
    			continue;
    		
    		condition = ((ColumnNode) visit(ctx.getChild(i))).getColumn();
    		value = ((ColumnNode) visit(ctx.getChild(i+2))).getColumn();
    		// Skip to i=i+2, so the value doesn't get read as a condition
    		i = i + 2;
    		
    		// When condition is false, return value. Otherwise leave as null.
    		if (res == null)
    			res = functions.when(condition.equalTo(functions.lit(false)), value);
    		else
    			res = res.when(condition.equalTo(functions.lit(false)), value);

    	}
    	
    	rv = new ColumnNode(res);
    	return rv;
    }


    /**
     * cidrmatch() eval method
     * x= cidr subnet, y= ip address to match with the subnet x
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodCidrmatch(DPLParser.EvalMethodCidrmatchContext ctx) {
    	Node rv = evalMethodCidrmatchEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodCidrmatchEmitCatalyst(DPLParser.EvalMethodCidrmatchContext ctx) {
    	Node rv = null;
    	
    	// Get arguments as columns
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column yCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
    	
    	// Register and use UDF cidrMatch
    	UserDefinedFunction cidrMatch = functions.udf(new Cidrmatch(), DataTypes.BooleanType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("cidrMatch", cidrMatch);
    	
    	Column col = functions.callUDF("cidrMatch", xCol, yCol);
    	
    	rv = new ColumnNode(col);
    	return rv;
    }

    /**
     * coalesce() eval method
     * Returns the first non-null argument
     * @param ctx
     * @return
     */
    public Node visitEvalMethodCoalesce(DPLParser.EvalMethodCoalesceContext ctx) {
    	Node rv = evalMethodCoalesceEmitCatalyst(ctx);
    	return rv;
    }
    
    // coalesce ( x , x2 , x3 , ... )
    private Node evalMethodCoalesceEmitCatalyst(DPLParser.EvalMethodCoalesceContext ctx) {
    	Node rv = null;
    	
    	// List for all the arguments
    	List<Column> columnList = new ArrayList<>();
    	Column res = null;
    	
    	// Skip all the non-interesting bits (commas, parenthesis) with the for loop itself
    	for (int i = 2; i <= ctx.getChildCount()-2; i = i + 2) {
    		ColumnNode currentItemNode = ((ColumnNode) visit(ctx.getChild(i)));
    		Column currentItem = currentItemNode.getColumn();
    		
			columnList.add(currentItem);
    	}
    	
    	res = functions.coalesce(JavaConversions.asScalaBuffer(columnList));
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * in() eval method
     * Returns if the first column's value is any of the other arguments
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIn(DPLParser.EvalMethodInContext ctx) {
    	Node rv = evalMethodInEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodInEmitCatalyst(DPLParser.EvalMethodInContext ctx) {
    	Node rv = null;
    	
    	// Get field as column
    	Column field = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Rest of the arguments are values, and are processed as strings
    	List<String> valueList = new ArrayList<>();
    	for (int i = 4; i < ctx.getChildCount() - 1; i=i+2) {
    		String value = ctx.getChild(i).getText();
    		valueList.add(Util.stripQuotes(value));
    	}
    	
    	field = field.isInCollection(valueList);
    	
    	rv = new ColumnNode(field);
    	
    	return rv;
    }

    /**
     * like() eval method
     * Returns TRUE if field is like pattern
     * Pattern supports wildcards % (multi char) and _ (single char)
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodLike(DPLParser.EvalMethodLikeContext ctx) {
    	Node rv = evalMethodLikeEmitCatalyst(ctx);
        return rv;
    }
    
    private Node evalMethodLikeEmitCatalyst(DPLParser.EvalMethodLikeContext ctx) {
    	Node rv = null;
    	
    	// like ( text , pattern )
    	Column textCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	String pattern = ctx.getChild(4).getText();
    	
    	rv = new ColumnNode(textCol.like(Util.stripQuotes(pattern)));
    	
    	return rv;
    }


    /**
     * match() eval method
     * Returns true if regex matches the subject
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodMatch(DPLParser.EvalMethodMatchContext ctx) {
    	Node rv = evalMethodMatchEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodMatchEmitCatalyst(DPLParser.EvalMethodMatchContext ctx) {
    	LOGGER.info("Visit eval method match");
    	Node rv = null;
    	
    	// match ( subject , regex )
    	Column subjectCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column regexCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
    	
    	this.booleanExpFieldName = ctx.getChild(2).getText(); // TODO for mvfilter()
    	
    	// Register and use UDF regexMatch
    	UserDefinedFunction regexMatch = functions.udf(new RegexMatch(), DataTypes.BooleanType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("regexMatch", regexMatch);
    	
    	Column col = functions.callUDF("regexMatch", subjectCol, regexCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }


    /**
     * tostring() eval method
     * Returns different types of strings based on given second argument
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodTostring(DPLParser.EvalMethodTostringContext ctx) {
    	Node rv = evalMethodTostringEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodTostringEmitCatalyst(DPLParser.EvalMethodTostringContext ctx) {
    	Node rv = null;
    	
    	// Requires at least one argument X (input)
    	// If X (input) is a number, Y can be provided (optionally) with arguments "hex", "commas" or "duration"
    	// "hex" => Converts X to hexadecimal
    	// "commas" => Formats X with commas. If number includes decimals, the function rounds to nearest two decimal places.
    	// "duration" => Converts seconds X to the readable time format HH:MM:SS.
    	
    	// tostring ( X ) --OR-- tostring ( X , Y )
    	
    	Column inputCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	String options = null;
    	if (ctx.getChildCount() > 4) options = Util.stripQuotes(ctx.getChild(4).getText());
    	
    	Column col = null;
    	// Base case without options (Y)
    	if (options == null) {
    		col = inputCol;
    	}
    	// With options (Y)
    	else {
    		// "hex", "commas" or "duration"
    		switch (options.toLowerCase()) {
	    		case "hex":
	    			col = functions.concat(functions.lit("0x"), functions.hex(inputCol));
	    			break;
	    		case "commas":
	    			col = functions.format_number(inputCol, 2);
	    			break;
	    		case "duration":
	    			// input is seconds -> output HH:MM:SS
	    			col = functions.from_unixtime(inputCol, "HH:mm:ss");
	    			break;
	    		default:
	    			throw new UnsupportedOperationException("Unsupported optional argument supplied: '" + options + "'.The argument must be 'hex', 'commas' or 'duration' instead.");
    		}
    	}
    	
    	rv = new ColumnNode(col.cast("string"));
    	return rv;
    }


    /**
     * tonumber() eval method
     * Returns number string converted to given base, defaults to base-10
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodTonumber(DPLParser.EvalMethodTonumberContext ctx) {
    	Node rv = evalMethodTonumberEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodTonumberEmitCatalyst(DPLParser.EvalMethodTonumberContext ctx) {
    	Node rv = null;
    	
    	// tonumber ( numstr , base )
    	// if base is not given, defaults to base-10
    	Column numstrCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column baseCol = functions.lit(10);
    	
    	if (ctx.getChildCount() > 4) baseCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
    	
    	// Register and use UDF toNumber
    	UserDefinedFunction toNumber = functions.udf(new Tonumber(), DataTypes.IntegerType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("toNumber", toNumber);
    	
    	Column col = functions.callUDF("toNumber", numstrCol, baseCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }


    /**
     * md5() eval method
     * Returns the md5 checksum of given field
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodMd5(DPLParser.EvalMethodMd5Context ctx) {
    	Node rv = evalMethodMd5EmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodMd5EmitCatalyst(DPLParser.EvalMethodMd5Context ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.md5(xCol);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * sha1() eval method
     * Returns the sha1 checksum of given field
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodSha1(DPLParser.EvalMethodSha1Context ctx) {
    	Node rv = evalMethodSha1EmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodSha1EmitCatalyst(DPLParser.EvalMethodSha1Context ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.sha1(xCol);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * sha256() eval method
     * Returns the sha256 checksum of given field
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodSha256(DPLParser.EvalMethodSha256Context ctx) {
    	Node rv = evalMethodSha256EmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodSha256EmitCatalyst(DPLParser.EvalMethodSha256Context ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.sha2(xCol, 256);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * sha512() eval method
     * Returns the sha512 checksum of given field
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodSha512(DPLParser.EvalMethodSha512Context ctx) {
    	Node rv = evalMethodSha512EmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodSha512EmitCatalyst(DPLParser.EvalMethodSha512Context ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	Column res = functions.sha2(xCol, 512);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * isbool() eval method
     * Returns whether or not the field value is a boolean
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIsbool(DPLParser.EvalMethodIsboolContext ctx) {
    	Node rv = evalMethodIsboolEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodIsboolEmitCatalyst(DPLParser.EvalMethodIsboolContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Register and use UDF isType
    	UserDefinedFunction isType = functions.udf(new IsType("Boolean"), DataTypes.BooleanType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("isBool", isType);
    	
    	Column col = functions.callUDF("isBool", xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * isint() eval method
     * Returns whether or not the field value is an integer
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIsint(DPLParser.EvalMethodIsintContext ctx) {
    	Node rv = evalMethodIsintEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodIsintEmitCatalyst(DPLParser.EvalMethodIsintContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Register and use UDF isType
    	UserDefinedFunction isType = functions.udf(new IsType("Integer"), DataTypes.BooleanType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("isInt", isType);
    	
    	Column col = functions.callUDF("isInt", xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * isnum() eval method
     * Returns whether or not the field value is a numeric
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIsnum(DPLParser.EvalMethodIsnumContext ctx) {
    	Node rv = evalMethodIsnumEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodIsnumEmitCatalyst(DPLParser.EvalMethodIsnumContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Register and use UDF isType
    	UserDefinedFunction isType = functions.udf(new IsType("Numeric"), DataTypes.BooleanType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("isNum", isType);
    	
    	Column col = functions.callUDF("isNum", xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * isstr() eval method
     * Returns whether or not the field value is a string
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIsstr(DPLParser.EvalMethodIsstrContext ctx) {
    	Node rv = evalMethodIsstrEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodIsstrEmitCatalyst(DPLParser.EvalMethodIsstrContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Register and use UDF isType
    	UserDefinedFunction isType = functions.udf(new IsType("String"), DataTypes.BooleanType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("isStr", isType);
    	
    	Column col = functions.callUDF("isStr", xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * typeof() eval method
     * Returns the type of the field
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodTypeof(DPLParser.EvalMethodTypeofContext ctx) {
    	Node rv = evalMethodTypeofEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodTypeofEmitCatalyst(DPLParser.EvalMethodTypeofContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	
    	// Register and use UDF typeOf
    	UserDefinedFunction typeOf = functions.udf(new TypeOf(), DataTypes.StringType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("typeOf", typeOf);
    	
    	Column col = functions.callUDF("typeOf", xCol);
    	rv = new ColumnNode(col);
    	
    	return rv;
    }

    /**
     * isnull() eval method
     * Returns whether or not the field value is a null
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIsnull(DPLParser.EvalMethodIsnullContext ctx) {
    	Node rv = evalMethodIsnullEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodIsnullEmitCatalyst(DPLParser.EvalMethodIsnullContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column res = functions.isnull(xCol);
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * isnotnull() eval method
     * Returns whether or not the field value is a non-null
     * @param ctx
     * @return
     */
    @Override public Node visitEvalMethodIsnotnull(DPLParser.EvalMethodIsnotnullContext ctx) {
    	Node rv = evalMethodIsnotnullEmitCatalyst(ctx);
    	return rv;
    }
    
    private Node evalMethodIsnotnullEmitCatalyst(DPLParser.EvalMethodIsnotnullContext ctx) {
    	Node rv = null;
    	
    	Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
    	Column res = xCol.isNotNull();
    	
    	rv = new ColumnNode(res);
    	return rv;
    }

    /**
     * commands() eval method
     * Returns the commands used in given search string
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodCommands(DPLParser.EvalMethodCommandsContext ctx) {
		Node rv = evalMethodCommandsEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodCommandsEmitCatalyst(DPLParser.EvalMethodCommandsContext ctx) {
		Node rv = null;

		Column searchString = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// Register and call UDF Commands
		UserDefinedFunction CommandsUDF = functions.udf(new Commands(), DataTypes.createArrayType(DataTypes.StringType, false));
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("CommandsUDF", CommandsUDF);

		Column res = functions.callUDF("CommandsUDF", searchString);
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * mvappend() eval method
     * Returns a multivalue field with all arguments as values
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvappend(DPLParser.EvalMethodMvappendContext ctx) {
		Node rv = evalMethodMvappendEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvappendEmitCatalyst(DPLParser.EvalMethodMvappendContext ctx) {
		Node rv = null;

		// mvappend ( x0, x1, ..., xn )
		// Fields to append can be found from children i = 2, 4, 6, 8, ...
		
		List<Column> listOfFields = new ArrayList<>();
		for (int i = 2; i <= ctx.getChildCount() - 2; i = i + 2) {
			Column field = ((ColumnNode) visit(ctx.getChild(i))).getColumn();
			if (field != null) listOfFields.add(field);
		}

		Column res = functions.array(JavaConversions.asScalaBuffer(listOfFields));
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvcount() eval method
     * Returns the amount of items in the multivalue field
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvcount(DPLParser.EvalMethodMvcountContext ctx) {
		Node rv = evalMethodMvcountEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvcountEmitCatalyst(DPLParser.EvalMethodMvcountContext ctx) {
		Node rv = null;

		// mvcount ( mvfield )
		
		// mvfield (Spark ArrayType)
		Column mvfield = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
				
		// Get size of array with spark builtin function size()
		Column sizeCol = functions.size(mvfield);
		
		// Return null if empty, otherwise return what functions.size() returns
		Column res = functions.when(sizeCol.notEqual(functions.lit(0)), sizeCol)
								  .otherwise(functions.lit(null));
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvdedup() eval method
     * Returns the given multivalue field with deduplicated values
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvdedup(DPLParser.EvalMethodMvdedupContext ctx) {
		Node rv = evalMethodMvdedupEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvdedupEmitCatalyst(DPLParser.EvalMethodMvdedupContext ctx) {
		Node rv = null;

		// Get multivalue field
		Column mvfield = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// Call and register dedup udf
		UserDefinedFunction mvDedupUDF = functions.udf(new Mvdedup(), DataTypes.createArrayType(DataTypes.StringType, false));
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("mvDedupUDF", mvDedupUDF);
		
		Column res = functions.callUDF("mvDedupUDF", mvfield);
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvfilter() eval method
     * Returns the values in a multivalue field that pass the given regex filter
     * TODO Implement, requires? work on the parser side
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvfilter(DPLParser.EvalMethodMvfilterContext ctx) {
		Node rv = null;

        throw new UnsupportedOperationException("Eval method 'Mvfilter' is not supported.");
        //rv = evalMethodMvfilterEmitCatalyst(ctx);
		//return rv;
	}

	// TODO WIP
	// need to implement multivalue field support on boolean expressions
	private Node evalMethodMvfilterEmitCatalyst(DPLParser.EvalMethodMvfilterContext ctx) {
		Node rv = null;

		// TODO implement
		// mvfilter ( x ) , where x is an arbitrary boolean expression
		// x can reference **only one** field at a time
		// example: mvfilter(match(email, "\.net$") OR match(email, "\.org$"))
		// |-> returns all values in field email that end in .net or .org
		
		Column booleanExp = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// booleanExpFieldName gets set when a boolean expression is found and contains a field name
		// right now, only match command sets this.
		Column booleanExpField = functions.col(this.booleanExpFieldName);
		
		Column res = functions.when(booleanExp.equalTo(functions.lit(true)), booleanExpField);
		res = functions.array(res);
		
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvfind() eval method
     * Returns the values that match the given regex in the multivalue field provided
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvfind(DPLParser.EvalMethodMvfindContext ctx) {
		Node rv = evalMethodMvfindEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvfindEmitCatalyst(DPLParser.EvalMethodMvfindContext ctx) {
		Node rv = null;

		// mvfind ( mvfield, "regex" )
		ColumnNode mvfieldNode = ((ColumnNode) visit(ctx.getChild(2)));
		Column mvfield = mvfieldNode.getColumn();
		
    	ColumnNode regexNode = ((ColumnNode) visit(ctx.getChild(4)));
    	Column regex = regexNode.getColumn();
    	    	
    	// Register and use UDF regexMatch with multivalue flag set to true
    	boolean isMultiValue = true;
    	UserDefinedFunction regexMatch = functions.udf(new RegexMatch(isMultiValue), DataTypes.IntegerType);
    	SparkSession ss = SparkSession.builder().getOrCreate();
    	ss.udf().register("regexMatch", regexMatch);
    	
    	Column col = functions.callUDF("regexMatch", mvfield, regex);
    	rv = new ColumnNode(col);
		
		return rv;
	}

    /**
     * mvindex() eval method
     * Returns the values of the multivalue field between the given indices
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvindex(DPLParser.EvalMethodMvindexContext ctx) {
		Node rv = evalMethodMvindexEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvindexEmitCatalyst(DPLParser.EvalMethodMvindexContext ctx) {
		Node rv = null;

		ColumnNode mvFieldNode = ((ColumnNode) visit(ctx.getChild(2)));
		Column mvField = mvFieldNode.getColumn();
		
		ColumnNode startIndexNode = ((ColumnNode) visit(ctx.getChild(4)));
		Column startIndex = startIndexNode.getColumn();
		
		ColumnNode endIndexNode = null;
		Column endIndex = null;
		
		// mvindex ( mvfield , start ) --OR-- mvindex ( mvfield , start , end )
		// child count: 6 without end, 8 with end
		
		// "end" param is optional
		if (ctx.getChildCount() > 6) {
			endIndexNode = ((ColumnNode) visit(ctx.getChild(6)));
			endIndex = endIndexNode.getColumn();
		}
		
		// Register and use UDF
		UserDefinedFunction mvIndexUDF = functions.udf(new Mvindex(), DataTypes.createArrayType(DataTypes.StringType, false));
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("mvIndexUDF", mvIndexUDF);
		
		Column res = functions.callUDF("mvIndexUDF", mvField, startIndex, endIndex == null ? functions.lit(-1) : endIndex);
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvjoin() eval method
     * Returns the multivalue field's items concatenated with given delimiter in between each value
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvjoin(DPLParser.EvalMethodMvjoinContext ctx) {
		Node rv = evalMethodMvjoinEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvjoinEmitCatalyst(DPLParser.EvalMethodMvjoinContext ctx) {
		Node rv = null;

		// mvjoin ( mvfield , str )
		
		ColumnNode mvfieldNode = ((ColumnNode) visit(ctx.getChild(2)));
		Column mvfield = mvfieldNode.getColumn();
		
		ColumnNode strNode = ((ColumnNode) visit(ctx.getChild(4)));
		Column str = strNode.getColumn();
				
		// Register and call UDF Mvjoin
		UserDefinedFunction MvjoinUDF = functions.udf(new Mvjoin(), DataTypes.StringType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("MvjoinUDF", MvjoinUDF);
		
		Column res = functions.callUDF("MvjoinUDF", mvfield, str);
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvrange() eval method
     * Returns a multivalue field with numbers from start to end with step.
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvrange(DPLParser.EvalMethodMvrangeContext ctx) {
		Node rv = evalMethodMvrangeEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvrangeEmitCatalyst(DPLParser.EvalMethodMvrangeContext ctx) {
		Node rv = null;

		// get columns of arguments
		Column start = ((ColumnNode) visit(ctx.getChild(2))).getColumn();	
		Column end = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
		Column step = ((ColumnNode) visit(ctx.getChild(6))).getColumn();
		
		// Register and call UDF Mvrange
		UserDefinedFunction MvrangeUDF = functions.udf(new Mvrange(), DataTypes.createArrayType(DataTypes.StringType, false));
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("MvrangeUDF", MvrangeUDF);

		Column res = functions.callUDF("MvrangeUDF", start, end, step);
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * mvsort() eval method
     * Returns the given multivalue field sorted
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvsort(DPLParser.EvalMethodMvsortContext ctx) {
		Node rv = evalMethodMvsortEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvsortEmitCatalyst(DPLParser.EvalMethodMvsortContext ctx) {
		Node rv = null;

		Column mvfield = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		Column res = functions.array_sort(mvfield);
		
		rv = new ColumnNode(res);
		return rv;
	}

    /**
     * mvzip() eval method
     * Returns the two multivalue field's values "zipped" together, optionally with a delimiter
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodMvzip(DPLParser.EvalMethodMvzipContext ctx) {
		Node rv = evalMethodMvzipEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodMvzipEmitCatalyst(DPLParser.EvalMethodMvzipContext ctx) {
		Node rv = null;

		// get columns of arguments
		Column mvfield1 = ((ColumnNode) visit(ctx.getChild(2))).getColumn();	
		Column mvfield2 = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
		Column delimiter = null;
		
		// delimiter is optional, default is comma
		if (ctx.getChildCount() > 6) {
			delimiter = ((ColumnNode) visit(ctx.getChild(6))).getColumn();
		}
		
		// Register and call UDF Mvzip
		// Spark built-in function arrays_zip() exists, but it does not support specifying the delimiter
		UserDefinedFunction MvzipUDF = functions.udf(new Mvzip(), DataTypes.createArrayType(DataTypes.StringType, false));
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("MvzipUDF", MvzipUDF);

		Column res = functions.callUDF("MvzipUDF", mvfield1, mvfield2, delimiter != null ? delimiter : functions.lit(","));
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * JSONValid() eval method
     * Returns whether or not the given field contains valid json
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodJSONValid(DPLParser.EvalMethodJSONValidContext ctx) {
		Node rv = evalMethodJSONValidEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodJSONValidEmitCatalyst(DPLParser.EvalMethodJSONValidContext ctx) {
		Node rv = null;

		Column json = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
		
		// Register and call UDF JSONValid
		UserDefinedFunction JSONValidUDF = functions.udf(new JSONValid(), DataTypes.BooleanType);
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("JSONValidUDF", JSONValidUDF);

		Column res = functions.callUDF("JSONValidUDF", json);
		rv = new ColumnNode(res);
		
		return rv;
	}

    /**
     * spath() eval method
     * Processes the spath/xpath expression and returns the results
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodSpath(DPLParser.EvalMethodSpathContext ctx) {
		Node rv = evalMethodSpathEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodSpathEmitCatalyst(DPLParser.EvalMethodSpathContext ctx) {
		Node rv = null;

		// Get the input (json or xml) and spath/xpath expression
		Column input = ((ColumnNode) visit(ctx.getChild(2))).getColumn(); // json or xml
		Column spathExpr = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
		
		// Register and call UDF Spath 
		UserDefinedFunction SpathUDF = functions.udf(new Spath(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
		SparkSession ss = SparkSession.builder().getOrCreate();
		ss.udf().register("SpathUDF", SpathUDF);

		Column res = functions.callUDF("SpathUDF", input, spathExpr, functions.lit(""), functions.lit(""));
		rv = new ColumnNode(res);

		return rv;
	}

    /**
     * time() eval method
     * Returns the current time in seconds
     * @param ctx
     * @return
     */
	@Override
	public Node visitEvalMethodTime(DPLParser.EvalMethodTimeContext ctx) {
		Node rv = evalMethodTimeEmitCatalyst(ctx);
		return rv;
	}

	private Node evalMethodTimeEmitCatalyst(DPLParser.EvalMethodTimeContext ctx) {
		Node rv = null;

		// Get current time in seconds, and add nanoseconds converted to seconds on top
		Instant now = Instant.now();
		double currentTimeInSecs = ((double)now.getEpochSecond()) + ((((double)now.getNano() / 1000d) / 1000d) / 1000d);

		// Known formatting type
		DecimalFormat df = new DecimalFormat("0.000000");
		Column res = functions.lit(df.format(currentTimeInSecs));
		
		rv = new ColumnNode(res);
		return rv;
	}
    
    //
    // -- Eval types (field, integer, etc.) --
    //

    public Node visitEvalFieldType(DPLParser.EvalFieldTypeContext ctx) {
        Node rv = null;
        traceBuffer.add("visitEvalFieldType incoming:"+ctx.getText());
        LOGGER.info("Visit Eval Field type");

        Column col = new Column(Util.stripQuotes(ctx.getChild(0).getText()));
        rv = new ColumnNode(col);
        traceBuffer.add("visitEvalFieldType rv:" + col.expr().sql());

        return rv;
    }

    public Node visitEvalIntegerType(DPLParser.EvalIntegerTypeContext ctx) {
        Node rv = null;
        LOGGER.info("Visit Eval Integer type");

        Column col = functions.lit(Integer.parseInt(ctx.getChild(0).getText())); //new Column(ctx.getChild(0).getText());
        rv = new ColumnNode(col);

        return rv;
    }

    public Node visitEvalNumberType(DPLParser.EvalNumberTypeContext ctx) {
        Node rv = null;
        LOGGER.info("Visit Eval Number type");

        // Column col = functions.lit(Integer.parseInt(ctx.getChild(0).getText())); //new Column(ctx.getChild(0).getText());
        // Column col = functions.lit(Double.valueOf(ctx.getChild(0).getText()));

        // evalNumberType can be EVAL_LANGUAGE_MODE_DECIMAL or EVAL_LANGUAGE_MODE_INTEGER
        Column col = null;
        TerminalNode evalNumberTypeNode = (TerminalNode) ctx.getChild(0);

        switch (evalNumberTypeNode.getSymbol().getType()) {
            case DPLLexer.EVAL_LANGUAGE_MODE_DECIMAL:
                col = functions.lit(Double.valueOf(evalNumberTypeNode.getText()));
                break;
            case DPLLexer.EVAL_LANGUAGE_MODE_INTEGER:
                col = functions.lit(Integer.parseInt(evalNumberTypeNode.getText()));
                break;
            default:
                throw new NumberFormatException("Expected decimal or integer");
        }

        rv = new ColumnNode(col);

        return rv;
    }

    public Node visitEvalStringType(DPLParser.EvalStringTypeContext ctx) {
        Node rv = null;
        LOGGER.info("Visit Eval String type");

        Column col = functions.lit(Util.stripQuotes(ctx.getChild(0).getText()));
        rv = new ColumnNode(col);

        return rv;
    }

    @Override
    public Node visitL_evalStatement_evalCalculateStatement_multipliers(DPLParser.L_evalStatement_evalCalculateStatement_multipliersContext ctx) {
        return evalCalculateStatementEmitCatalyst(ctx);
    }

    @Override
    public Node visitL_evalStatement_evalCalculateStatement_minus_plus(DPLParser.L_evalStatement_evalCalculateStatement_minus_plusContext ctx) {
        return evalCalculateStatementEmitCatalyst(ctx);
    }

    public Node evalCalculateStatementEmitCatalyst(ParserRuleContext ctx) {
    	Column leftSide = ((ColumnNode) visit(ctx.getChild(0))).getColumn();
    	TerminalNode opNode = (TerminalNode) ctx.getChild(1);
    	Column rightSide = ((ColumnNode) visit(ctx.getChild(2))).getColumn();

        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("EvalArithmetic", new EvalArithmetic(), DataTypes.StringType);
        Column res = functions.callUDF("EvalArithmetic", leftSide, functions.lit(opNode.getText()), rightSide);
        return new ColumnNode(res);

        /* Column res = null;
    	switch (opNode.getSymbol().getType()) {
	    	case DPLLexer.EVAL_LANGUAGE_MODE_PLUS: // '+'
	    		Column plus = leftSide.plus(rightSide);
	    		res = plus;
	    		break;
	    	case DPLLexer.EVAL_LANGUAGE_MODE_MINUS: // '-'
	    		res = leftSide.minus(rightSide);
	    		break;
	    	case DPLLexer.EVAL_LANGUAGE_MODE_WILDCARD: // '*'
	    		res = leftSide.multiply(rightSide);
	    		break;
	    	case DPLLexer.EVAL_LANGUAGE_MODE_SLASH: // '/'
	    		res = leftSide.divide(rightSide);
	    		break;
	    	case DPLLexer.EVAL_LANGUAGE_MODE_PERCENT: // '%'
	    		res = leftSide.mod(rightSide);
	    		break;
	    	default:
	    		throw new UnsupportedOperationException("Unknown EvalCalculateStatement operation: " + opNode.getText());
    	}
    	
    	return new ColumnNode(res);*/
    }

    @Override
    public Node visitL_evalStatement_evalConcatenateStatement(DPLParser.L_evalStatement_evalConcatenateStatementContext ctx) {
        throw new UnsupportedOperationException("evalConcatenateStatement not supported yet");
         /* was as bellow, in SQL mode
        if (doc != null) {
            throw new RuntimeException("evalConcatenateStatementEmitSql not implemented yet: " + ctx.getText());
//            return null;
        }
        return evalConcatenateStatementEmitSql(ctx);


    public StringNode evalConcatenateStatementEmitSql(DPLParser.L_evalStatement_evalConcatenateStatementContext ctx) {
        String sql = null;
        boolean useConcat = false;
        traceBuffer.add(ctx.getChildCount() + " VisitEvalConcatenateStatement:" + ctx.getText());

        Node left = visit(ctx.getChild(0));
        TerminalNode operation = (TerminalNode) ctx.getChild(1);
        Node right = visit(ctx.getChild(2));

        String leftOperand = left.toString();
        String rightOperand = right.toString();
        // check whether operand name exist in symbol-table and replace it
        if (symbolTable.get(leftOperand) != null) {
            leftOperand = symbolTable.get(leftOperand);
        }
        if (symbolTable.get(rightOperand) != null) {
            rightOperand = symbolTable.get(rightOperand);
        }
        sql = "CONCAT(" + leftOperand + ", " + rightOperand + ")";
        traceBuffer.add("Generate EvalConcatenate:" + sql);
        StringNode sNode = new StringNode(new Token(Type.STRING, sql));
        return sNode;
    }
         */
    }

}
