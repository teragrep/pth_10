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
package com.teragrep.pth10.ast.commands.evalstatement;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.QuotedText;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.commands.evalstatement.UDFs.*;
import com.teragrep.pth10.steps.eval.EvalStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.ParserRuleContext;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.teragrep.jue_01.GlobToRegEx.regexify;

/**
 * Base statement for evaluation functions. Called from
 * {@link com.teragrep.pth10.ast.commands.transformstatement.EvalTransformation}
 */
public class EvalStatement extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EvalStatement.class);
    private final DPLParserCatalystContext catCtx;

    // Boolean expression field name TODO
    private String booleanExpFieldName = null;
    public EvalStep evalStep = null;

    /**
     * Initialize evalStatement
     * 
     * @param catCtx Catalyst context object
     */
    public EvalStatement(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public Node visitL_evalStatement_subEvalStatement(DPLParser.L_evalStatement_subEvalStatementContext ctx) {
        LOGGER.info("VisitSubEvalStatements: children=<{}> text=<{}>", ctx.getChildCount(), ctx.getChild(0).getText());
        // Consume parenthesis and return actual evalStatement
        Node rv = visit(ctx.getChild(0));
        LOGGER
                .debug(
                        "VisitSubEvalStatements children=<{}> rv=<{}> class=<{}>", ctx.getChildCount(), rv,
                        rv.getClass().getName()
                );
        //return new ColumnNode(functions.expr("true"));
        return rv;
    }

    @Override
    public Node visitEvalFunctionStatement(DPLParser.EvalFunctionStatementContext ctx) {
        Node rv = visit(ctx.getChild(0));
        return rv;
    }

    /**
     * evalCompareStatement : (decimalType|fieldType|stringType) (DEQ|EQ|LTE|GTE|LT|GT|NEQ|LIKE|Like) evalStatement ;
     **/
    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        Node rv = evalCompareStatementEmitCatalyst(ctx);

        return rv;
    }

    /**
     * evalCompareStatement : (decimalType|fieldType|stringType) (DEQ|EQ|LTE|GTE|LT|GT|NEQ|LIKE|Like) evalStatement ;
     **/
    private Node evalCompareStatementEmitCatalyst(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        Node rv = null;
        Column lCol = null;
        Column rCol = null;
        Node left = visit(ctx.getChild(0));
        if (left != null) {
            lCol = ((ColumnNode) left).getColumn();
        }
        Node right = visit(ctx.getChild(2));
        if (right != null) {
            rCol = ((ColumnNode) right).getColumn();
        }
        // Add operation between columns operation =,<,>,......
        Column col = addOperation(lCol, (TerminalNode) ctx.getChild(1), rCol);
        rv = new ColumnNode(col);
        return rv;
    }

    /**
     * Generates a column based on a source, value and operation
     * 
     * @param source    Left hand side
     * @param operation Operation
     * @param value     Right hand side
     * @return Final resulting column
     */
    private Column addOperation(Column source, TerminalNode operation, Column value) {
        Column rv = null;

        if (operation.getSymbol().getType() == DPLLexer.EVAL_LANGUAGE_MODE_LIKE) {
            SparkSession ss = catCtx.getSparkSession();
            ss.udf().register("LikeComparison", new LikeComparison(), DataTypes.BooleanType);
            rv = functions.callUDF("LikeComparison", source, value);
        }
        else {
            SparkSession ss = catCtx.getSparkSession();
            ss.udf().register("EvalOperation", new EvalOperation(), DataTypes.BooleanType);
            rv = functions.callUDF("EvalOperation", source, functions.lit(operation.getSymbol().getType()), value);
        }
        return rv;
    }

    public Node visitL_evalStatement_evalLogicStatement(DPLParser.L_evalStatement_evalLogicStatementContext ctx) {
        Node rv = evalLogicStatementEmitCatalyst(ctx);
        return rv;
    }

    private Node evalLogicStatementEmitCatalyst(DPLParser.L_evalStatement_evalLogicStatementContext ctx) {
        final Column col;
        LOGGER
                .debug(
                        "VisitEvalLogicStatement(Catalyst) incoming: children=<{}> text=<{}>", ctx.getChildCount(),
                        ctx.getText()
                );

        final List<DPLParser.EvalStatementContext> evalStatements = ctx.evalStatement();

        ColumnNode leftSide = (ColumnNode) visit(evalStatements.get(0));
        ColumnNode rightSide = (ColumnNode) visit(evalStatements.get(1));

        if (ctx.EVAL_LANGUAGE_MODE_OR() != null) {
            col = leftSide.getColumn().or(rightSide.getColumn());
        }
        else if (ctx.EVAL_LANGUAGE_MODE_AND() != null) {
            col = leftSide.getColumn().and(rightSide.getColumn());
        }
        else {
            throw new IllegalArgumentException("Unexpected operation in logic statement: " + ctx.getText());
        }

        final ColumnNode rv = new ColumnNode(col);
        LOGGER.debug(" EvalLogicStatement(Catalyst) generated=<{}> class=<{}>", rv, rv.getClass().getName());
        return rv;
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        LOGGER.debug("visit normal field type");
        Node rv = null;

        Column col = functions.lit(ctx.getChild(0).getText()); //new Column(ctx.getChild(0).getText());
        rv = new ColumnNode(col);

        return rv;
    }

    /**
     * subEvalStatement : PARENTHESIS_L EvalStatement PARENTHESIS_R ;
     */
    @Override
    public Node visitSubEvalStatement(DPLParser.SubEvalStatementContext ctx) {
        LOGGER.debug("SubEvalStatement: text=<{}>", ctx.getText());
        Node rv = subEvalStatementEmitCatalyst(ctx);
        return rv;
    }

    private Node subEvalStatementEmitCatalyst(DPLParser.SubEvalStatementContext ctx) {
        return visit(ctx.getChild(1));
    }

    @Override
    public Node visitT_eval_evalParameter(DPLParser.T_eval_evalParameterContext ctx) {
        Node n = visit(ctx.evalStatement());
        Node field = visit(ctx.fieldType());

        // Step initialization
        this.evalStep = new EvalStep();
        this.evalStep.setLeftSide(field.toString()); // eval a = ...
        this.evalStep.setRightSide(((ColumnNode) n).getColumn()); // ... = right side

        return new StepNode(this.evalStep);
    }

    public Node visitEvalMethodIf(DPLParser.EvalMethodIfContext ctx) {
        Node rv = null;

        rv = evalMethodIfEmitCatalyst(ctx);

        return rv;
    }

    private Node evalMethodIfEmitCatalyst(DPLParser.EvalMethodIfContext ctx) {
        // if-clause: if(<CONDITION>, <IF TRUE>, <IF FALSE>)
        /* In java, it would be:
                if (<condition>) {
                    // <if-true>
                } else {
                    // <if-false>
         */

        // Get params
        ColumnNode logical = (ColumnNode) visit(ctx.evalStatement(0));
        ColumnNode ifTrue = (ColumnNode) visit(ctx.evalStatement(1));
        ColumnNode ifFalse = (ColumnNode) visit(ctx.evalStatement(2));

        // Register and call ifClause UDF
        UserDefinedFunction ifClauseUdf = functions
                .udf(new IfClause(), DataTypes.createArrayType(DataTypes.StringType, true));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("ifClause", ifClauseUdf);
        Column res = functions.callUDF("ifClause", logical.getColumn(), ifTrue.getColumn(), ifFalse.getColumn());

        return new ColumnNode(res);
    }

    /**
     * substring() eval method Takes a substring out of the given string based on given indices
     * 
     * @param ctx EvalMethodSubstrContext
     * @return column node containing substr column
     */
    @Override
    public Node visitEvalMethodSubstr(DPLParser.EvalMethodSubstrContext ctx) {
        Node rv = evalMethodSubstrEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodSubstrEmitCatalyst(DPLParser.EvalMethodSubstrContext ctx) {
        ColumnNode rv;
        Column exp;
        String par1 = visit(ctx.getChild(2)).toString();
        String par2 = visit(ctx.getChild(4)).toString();
        // TODO: In spark >=3.5.0 change to use functions.substring() as it supports not
        //  providing the length argument.
        if (ctx.evalStatement().size() > 2) {
            String par3 = visit(ctx.getChild(6)).toString();
            exp = functions.expr(String.format("substring(%s, %s, %s)", par1, par2, par3));
        }
        else {
            exp = functions.expr(String.format("substring(%s, %s)", par1, par2));
        }

        rv = new ColumnNode(exp);
        return rv;
    }

    /**
     * true() eval method returns TRUE
     * 
     * @param ctx EvalMethodTrueContext
     * @return column node
     */
    public Node visitEvalMethodTrue(DPLParser.EvalMethodTrueContext ctx) {
        Node rv = null;
        rv = evalMethodTrueEmitCatalyst(ctx);

        return rv;
    }

    private Node evalMethodTrueEmitCatalyst(DPLParser.EvalMethodTrueContext ctx) {
        Column col = functions.lit(true);
        return new ColumnNode(col);
    }

    /**
     * false() eval method returns FALSE
     * 
     * @param ctx EvalMethodFalseContext
     * @return column node
     */
    public Node visitEvalMethodFalse(DPLParser.EvalMethodFalseContext ctx) {
        Node rv = null;
        rv = evalMethodFalseEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodFalseEmitCatalyst(DPLParser.EvalMethodFalseContext ctx) {
        Column col = functions.lit(false);//functions.exp("false");
        return new ColumnNode(col);
    }

    /**
     * null() eval method Returns NULL
     * 
     * @param ctx EvalMethodNullContext
     * @return column node
     */
    @Override
    public Node visitEvalMethodNull(DPLParser.EvalMethodNullContext ctx) {
        LOGGER.debug("Visit eval method null");
        Node rv = evalMethodNullEmitCatalyst(ctx);

        return rv;
    }

    private Node evalMethodNullEmitCatalyst(DPLParser.EvalMethodNullContext ctx) {
        Column col = functions.lit(catCtx.nullValue.value()).cast(DataTypes.StringType);
        return new ColumnNode(col);
    }

    /**
     * nullif() eval method Returns NULL if x==y, otherwise x
     * 
     * @param ctx EvalMethodNullifContext
     * @return column node
     */
    @Override
    public Node visitEvalMethodNullif(DPLParser.EvalMethodNullifContext ctx) {
        Node rv = evalMethodNullifEmitCatalyst(ctx);

        return rv;
    }

    private Node evalMethodNullifEmitCatalyst(DPLParser.EvalMethodNullifContext ctx) {
        Node rv = null;

        // Get params x and y
        Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column yCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();

        // If x == y, return null
        Column col = functions
                .when(xCol.equalTo(yCol), functions.lit(catCtx.nullValue.value()).cast(DataTypes.StringType));
        // otherwise, return x
        col = col.otherwise(xCol);

        rv = new ColumnNode(col);
        return rv;
    }

    /**
     * searchmatch(x) eval method Returns TRUE if the search string matches the event
     * 
     * @param ctx EvalMethodSearchmatchContext
     * @return column node
     */
    @Override
    public Node visitEvalMethodSearchmatch(DPLParser.EvalMethodSearchmatchContext ctx) {
        return evalMethodSearchmatchEmitCatalyst(ctx);
    }

    private Node evalMethodSearchmatchEmitCatalyst(DPLParser.EvalMethodSearchmatchContext ctx) {
        // searchmatch(x) : Returns TRUE if the search string (first and only argument) matches the event

        String searchStr = new UnquotedText(new TextString(ctx.getChild(2).getText())).read(); // strip quotes
        LOGGER.info("Got search string <[{}]> in searchmatch.", searchStr);

        // fields array should contain x=... , y=..., etc.
        String[] fields = searchStr.split(" ");

        List<Column> columns = new ArrayList<>(); // list of all the Columns used in searchmatch

        for (String f : fields) {
            // Split to field and literal based on operator
            String[] operands = f.split("(<=|>=|<|=|>)", 2);

            // implicit _raw filtering
            if (operands.length == 1) {
                String regexifiedString = "(?i)" + regexify(operands[0]); // (?i) to make it case-insensitive
                columns.add(functions.col("_raw").rlike(regexifiedString));
                // field=rlike
            }
            else {
                Column field = functions.col(operands[0].trim());
                Column literal = functions.lit(operands[1].trim());
                String literalString = operands[1].trim();
                String regexifiedString = "(?i)" + regexify(literalString); // (?i) to make it case-insensitive

                // Test if field equals/leq/geq/lt/gt to literal
                if (f.contains("<=")) {
                    columns.add(field.leq(literal));
                }
                else if (f.contains(">=")) {
                    columns.add(field.geq(literal));
                }
                else if (f.contains("<")) {
                    columns.add(field.lt(literal));
                }
                else if (f.contains("=")) {
                    columns.add(field.rlike(regexifiedString));
                }
                else if (f.contains(">")) {
                    columns.add(field.gt(literal));
                }
            }
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("The search string was empty in searchmatch.");
        }

        // Make the list of colummns into one column
        Column andColumn = columns.get(0); // initialize with the first column
        for (int i = 1; i < columns.size(); i++) { // goes through the rest of the columns, if there are any
            andColumn = andColumn.and(columns.get(i));
        }

        // When everything equals, res=true, otherwise res=false
        Column res = functions.when(andColumn, functions.lit(true)).otherwise(functions.lit(false));

        return new ColumnNode(res);
    }

    /**
     * now() eval method Returns the current system time
     * 
     * @param ctx EvalMethodNowContext
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
     * len() eval method Returns the length of the field contents
     * 
     * @param ctx EvalMethodLenContext
     * @return column node
     */
    public Node visitEvalMethodLen(DPLParser.EvalMethodLenContext ctx) {
        Node rv = evalMethodLenEmitCatalyst(ctx);
        return rv;
    }

    public Node evalMethodLenEmitCatalyst(DPLParser.EvalMethodLenContext ctx) {
        ColumnNode rv;
        String inField = visit(ctx.getChild(2)).toString();
        rv = new ColumnNode(functions.length(new Column(inField)));
        return rv;
    }

    /**
     * lower() eval method Returns the field contents in all lowercase characters
     * 
     * @param ctx EvalMethodLowerContext
     * @return column node
     */
    @Override
    public Node visitEvalMethodLower(DPLParser.EvalMethodLowerContext ctx) {
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
     * upper() eval method Returns the field contents in all uppercase characters
     * 
     * @param ctx EvalMethodUpperContext
     * @return ColumnNode containing column for upper() eval method
     */
    @Override
    public Node visitEvalMethodUpper(DPLParser.EvalMethodUpperContext ctx) {
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
     * urldecode() eval method Returns the given URL decoded, e.g. replaces %20 etc. with appropriate human readable
     * characters
     * 
     * @param ctx EvalMethodUrldecodeContext
     * @return column node
     */
    @Override
    public Node visitEvalMethodUrldecode(DPLParser.EvalMethodUrldecodeContext ctx) {
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
     * ltrim() eval method Returns the field contents with trimString trimmed from left side if given, otherwise spaces
     * and tabs
     * 
     * @param ctx EvalMethodLtrimContext
     * @return ColumnNode containing column for trim() eval method
     */
    @Override
    public Node visitEvalMethodLtrim(DPLParser.EvalMethodLtrimContext ctx) {
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
     * replace() eval method Returns a string with replaced parts as defined by the regex string
     * 
     * @param ctx EvalMethodReplaceContext
     * @return column node
     */
    @Override
    public Node visitEvalMethodReplace(DPLParser.EvalMethodReplaceContext ctx) {
        Node rv = evalMethodReplaceEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodReplaceEmitCatalyst(DPLParser.EvalMethodReplaceContext ctx) {
        Node rv = null;

        // Substitutes string Z for every occurrence of regex string Y in string X
        // replace ( x , y , z )

        if (ctx.getChildCount() != 8) {
            throw new UnsupportedOperationException(
                    "Eval method 'replace' requires three arguments: source string, regex string and substitute string."
            );
        }

        Column srcString = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column regexString = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
        Column substituteString = ((ColumnNode) visit(ctx.getChild(6))).getColumn();

        Column res = functions.regexp_replace(srcString, regexString, substituteString);
        rv = new ColumnNode(res);

        return rv;
    }

    /**
     * rtrim() eval method Returns the field contents with trimString trimmed from right side if given, otherwise spaces
     * and tabs
     * 
     * @param ctx EvalMethodRtrimContext
     * @return ColumnNode containing column for rtrim() eval method
     */
    @Override
    public Node visitEvalMethodRtrim(DPLParser.EvalMethodRtrimContext ctx) {
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
     * trim() eval method Returns the field contents with trimString trimmed from both sides if given, otherwise spaces
     * and tabs
     * 
     * @param ctx EvalMethodTrimContext
     * @return ColumnNode containing Column for trim() eval method
     */
    @Override
    public Node visitEvalMethodTrim(DPLParser.EvalMethodTrimContext ctx) {
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
     * split() eval method Returns field split with delimiter
     * 
     * @param ctx EvalMethodSplitContext
     * @return ColumnNode containing the Column for split() eval method
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
        Column res = functions.split(field, new UnquotedText(new TextString(delimiter)).read());

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * relative_time() eval method Returns timestamp based on given unix epoch and relative time modifier
     * 
     * @param ctx EvalMethodRelative_timeContext
     * @return ColumnNode containing Column for relative_time() eval method
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
     * strftime() eval method Returns a timestamp based on given unix epoch and format string
     * 
     * @param ctx EvalMethodStrftimeContext
     * @return ColumnNode containing column for strftime() eval method
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
        String formatStr = new UnquotedText(new TextString(ctx.getChild(4).getText())).read();

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

        LOGGER.debug("formatstr= <{}>", formatStr);

        // call udf which converts possible timestamp/string to unixtime first
        //SparkSession sparkSession = SparkSession.builder().getOrCreate();
        //sparkSession.udf().register("udf_timetounixtime", new TimeToUnixTime(), DataTypes.LongType);

        Column res = functions.from_unixtime(unixTime, formatStr);

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * strptime() eval method Returns an unix epoch based on given timestamp and format string
     * 
     * @param ctx EvalMethodStrptimeContext
     * @return ColumnNode containing the column for strptime() eval method
     */
    @Override
    public Node visitEvalMethodStrptime(DPLParser.EvalMethodStrptimeContext ctx) {
        Node rv = evalMethodStrptimeEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodStrptimeEmitCatalyst(DPLParser.EvalMethodStrptimeContext ctx) {
        Node rv = null;

        Column stringTime = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        String formatStr = new UnquotedText(new TextString(ctx.getChild(4).getText())).read();

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

        LOGGER.debug("Format String=<{}>", formatStr);
        Column res = functions.unix_timestamp(stringTime, formatStr);

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * pow() eval method Returns the field to the power of n
     * 
     * @param ctx EvalMethodPowContext
     * @return ColumnNode for pow() eval method
     */
    public Node visitEvalMethodPow(DPLParser.EvalMethodPowContext ctx) {
        Node rv = evalMethodPowEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodPowEmitCatalyst(DPLParser.EvalMethodPowContext ctx) {
        Node rv = null;
        // child 2 and 4 are x and y
        Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column yCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();

        Column col = functions.pow(xCol, yCol);
        rv = new ColumnNode(col);

        return rv;
    }

    /**
     * abs() eval method Returns absolute value
     * 
     * @param ctx EvalMethodAbs
     * @return ColumnNode for abs() eval method
     */
    @Override
    public Node visitEvalMethodAbs(DPLParser.EvalMethodAbsContext ctx) {
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
     * ceiling() / ceil() eval method Returns the value rounded up
     * 
     * @param ctx EvalMethodCeiling
     * @return ColumnNode for ceiling() / ceil() eval method
     */
    @Override
    public Node visitEvalMethodCeiling(DPLParser.EvalMethodCeilingContext ctx) {
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
     * exact() eval method Acts as a passthrough More details:
     * {@link #evalMethodExactEmitCatalyst(DPLParser.EvalMethodExactContext)}
     * 
     * @param ctx EvalMethodExactContext
     * @return ColumnNode containg Column for exact() eval method
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
     * exp() eval method Returns e^n
     * 
     * @param ctx EvalMethodExpContext
     * @return ColumnNode containing column for exp() eval method
     */
    @Override
    public Node visitEvalMethodExp(DPLParser.EvalMethodExpContext ctx) {
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
     * floor() eval method Rounds down to nearest integer
     * 
     * @param ctx EvalMethodFloor
     * @return ColumnNode containing column for floor() eval method
     */
    @Override
    public Node visitEvalMethodFloor(DPLParser.EvalMethodFloorContext ctx) {
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
     * ln() eval method Returns the natural logarithmic of n
     * 
     * @param ctx EvalMethodLnContext
     * @return ColumnNode containing column for ln() eval method
     */
    @Override
    public Node visitEvalMethodLn(DPLParser.EvalMethodLnContext ctx) {
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
     * log() eval method Returns the nth logarithmic of given number
     * 
     * @param ctx EvalMethodLogContext
     * @return ColumnNode containing the column for log() eval method
     */
    @Override
    public Node visitEvalMethodLog(DPLParser.EvalMethodLogContext ctx) {
        Node rv = evalMethodLogEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodLogEmitCatalyst(DPLParser.EvalMethodLogContext ctx) {
        Node rv;

        // log ( num , base )
        Column numberCol;
        double base = 10d;
        if (ctx.evalStatement().size() == 1) {
            // num param
            numberCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        }
        else if (ctx.evalStatement().size() == 2) {
            // num, base params
            numberCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
            base = Double.parseDouble(ctx.getChild(4).getText());
        }
        else {
            throw new UnsupportedOperationException(
                    "Eval method 'log' supports two parameters: Number (required) and base (optional)."
            );
        }

        Column res = functions.log(base, numberCol);
        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * cos() eval method Returns the cosine of the field value
     * 
     * @param ctx EvalMethodCosContext
     * @return ColumnNode containing the column for cos() eval method
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
     * cosh() eval method Returns the hyperbolic cosine of the field value
     * 
     * @param ctx EvalMethodCoshContext
     * @return ColumnNode containing the column for the cosh() eval method
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
     * acos() eval method Returns arc cosine of the field value
     * 
     * @param ctx EvalMethodAcosContext
     * @return ColumnNode containing the column for acos() eval method
     */
    @Override
    public Node visitEvalMethodAcos(DPLParser.EvalMethodAcosContext ctx) {
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
     * acosh() eval method Returns the inverse hyperbolic cosine of the field value
     * 
     * @param ctx EvalMethodAcoshContext
     * @return ColumnNode containing the column for acosh() eval method
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
     * sin() eval method Returns the sine of the field value
     * 
     * @param ctx EvalMethodSinContext
     * @return ColumnNode containing the Column for sin() eval method
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
     * sinh() eval method Returns hyperbolic sine of the field value
     * 
     * @param ctx EvalMethodSinhContext
     * @return ColumnNode containing the Column for the sinh() eval method
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
     * asin() eval method Returns arc sine of the field value
     * 
     * @param ctx EvalMethodAsinContext
     * @return ColumnNode containing the Column for the asin() eval method
     */
    @Override
    public Node visitEvalMethodAsin(DPLParser.EvalMethodAsinContext ctx) {
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
     * asinh() eval method Returns inverse hyperbolic sine of the field value
     * 
     * @param ctx EvalMethodAsinhContext
     * @return ColumnNode containing the Column for the asinh() eval method
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
     * tan() eval method Returns the tangent of the field value
     * 
     * @param ctx EvalMethodTanContext
     * @return ColumnNode containing the Column for the tan() eval method
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
     * tanh() eval method Returns the hyperbolic tangent of the field value
     * 
     * @param ctx EvalMethodTanhContext
     * @return ColumnNode containing the Column for the tanh() eval method
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
     * atan() eval method Returns the arc tangent of the field value
     * 
     * @param ctx EvalMethodAtanContext
     * @return ColumnNode containing the Column for the atan() eval method
     */
    @Override
    public Node visitEvalMethodAtan(DPLParser.EvalMethodAtanContext ctx) {
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
     * atan2() eval method Returns the arc tangent of Y,X
     * 
     * @param ctx EvalMethodAtan2Context
     * @return ColumnNode containg the Column for the atan2() eval method
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
     * atanh() eval method Returns the inverse hyperbolic tangent of the field value
     * 
     * @param ctx EvalMethodAtanhContext
     * @return ColumnNode containing the Column for the atanh() eval method
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
     * avg() eval method Returns the average of all numerical parameters given as an integer. Ignores parameters that
     * can't be converted to a number.
     * 
     * @param ctx EvalMethodAvgContext
     * @return ColumnNode containing the Column for the avg() eval method
     */
    @Override
    public Node visitEvalMethodAvg(DPLParser.EvalMethodAvgContext ctx) {
        Column sum = functions.lit(0);
        Column amountSummed = functions.lit(0);

        for (DPLParser.EvalStatementContext eval : ctx.evalStatement()) {
            Column number = ((ColumnNode) visit(eval)).getColumn();
            Column isNumber = number.cast(DataTypes.DoubleType).isNotNull();

            // only sum numerical values, because the value would result to a null otherwise
            sum = functions.when(isNumber, sum.plus(number.cast(DataTypes.DoubleType))).otherwise(sum);

            amountSummed = functions.when(isNumber, amountSummed.plus(1)).otherwise(amountSummed);
        }

        Column average = functions.round(sum.divide(amountSummed)).cast(DataTypes.IntegerType);

        return new ColumnNode(average);
    }

    /**
     * hypot() eval method Returns the hypotenuse, when X and Y are the edges forming the 90 degree angle of a triangle
     * 
     * @param ctx EvalMethodHypotContext
     * @return ColumnNode containing the Column for the hypot() eval method
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
     * pi() eval method Returns constant pi to 11 digits of precision
     * 
     * @param ctx EvalMethodPiContext
     * @return ColumnNode containing the Column for the pi() eval method
     */
    @Override
    public Node visitEvalMethodPi(DPLParser.EvalMethodPiContext ctx) {
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
     * min() eval method Returns the minimum of the given arguments
     * 
     * @param ctx EvalMethodMinContext
     * @return ColumnNode containing the Column for the min() eval method
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
        for (int i = 2; i <= ctx.getChildCount() - 2; i = i + 2) {
            listOfColumns.add(((ColumnNode) visit(ctx.getChild(i))).getColumn());
        }

        Seq<Column> seqOfColumns = JavaConversions.asScalaBuffer(listOfColumns);
        Column arrayOfCols = functions.array(seqOfColumns);

        // Register and call UDF MinMax
        UserDefinedFunction minUDF = functions.udf(new MinMax(), DataTypes.StringType);
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("minUDF", minUDF);

        Column res = functions.callUDF("minUDF", arrayOfCols, functions.lit(true));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * max() eval method Returns the maximum of the given arguments
     * 
     * @param ctx EvalMethodMaxContext
     * @return ColumnNode containing the Column for the max() eval method
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
        for (int i = 2; i <= ctx.getChildCount() - 2; i = i + 2) {
            listOfColumns.add(((ColumnNode) visit(ctx.getChild(i))).getColumn());
        }

        Seq<Column> seqOfColumns = JavaConversions.asScalaBuffer(listOfColumns);
        Column arrayOfCols = functions.array(seqOfColumns);

        // Register and call UDF MinMax
        UserDefinedFunction maxUDF = functions.udf(new MinMax(), DataTypes.StringType);
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("maxUDF", maxUDF);

        Column res = functions.callUDF("maxUDF", arrayOfCols, functions.lit(false));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * random() eval method Returns a pseudo-random integer from range 0 to 2^31 - 1
     * 
     * @param ctx EvalMethodRandomContext
     * @return ColumnNode containing the Column for the random() eval method
     */
    @Override
    public Node visitEvalMethodRandom(DPLParser.EvalMethodRandomContext ctx) {
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
     * sqrt() eval method Returns the square root of the field value
     * 
     * @param ctx EvalMethodSqrtContext
     * @return ColumnNode containing the Column for the sqrt() eval method
     */
    @Override
    public Node visitEvalMethodSqrt(DPLParser.EvalMethodSqrtContext ctx) {
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
     * sum() eval method Returns the sum of the given numerical values/fields
     * 
     * @param ctx EvalmethodSumContext
     * @return ColumnNode containing the Column for the sum() eval method
     */
    @Override
    public Node visitEvalMethodSum(DPLParser.EvalMethodSumContext ctx) {
        Column sum = functions.lit(0);
        for (DPLParser.EvalStatementContext eval : ctx.evalStatement()) {
            Column number = ((ColumnNode) visit(eval)).getColumn();
            Column isDouble = number.cast(DataTypes.DoubleType).isNotNull();

            // only sum numerical values, because the value would result to a null otherwise
            sum = functions.when(isDouble, sum.plus(number)).otherwise(sum);
        }
        return new ColumnNode(sum);
    }

    /**
     * round() eval method Returns x rounded to y decimal places, or integer if y missing
     * 
     * @param ctx EvalMethodRoundContext
     * @return ColumnNode containing the Column for the round() eval method
     */
    @Override
    public Node visitEvalMethodRound(DPLParser.EvalMethodRoundContext ctx) {
        Node rv = evalMethodRoundEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodRoundEmitCatalyst(DPLParser.EvalMethodRoundContext ctx) {
        Node rv = null;

        // round ( x , y )
        Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        int decimalPlaces = 0;

        if (ctx.getChildCount() > 4) {
            decimalPlaces = Integer.parseInt(ctx.getChild(4).getText());
        }

        Column res = functions.round(xCol, decimalPlaces);
        rv = new ColumnNode(res);

        return rv;
    }

    /**
     * sigfig() eval method Returns the field value reduced to the significant figures
     * 
     * @param ctx EvalMethodSigfigContext
     * @return ColumnNode containing the Column for the sigfig() eval method
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
     * case() eval method Alternating conditions and values, returns the first value where condition is true
     * 
     * @param ctx EvalMethodCaseContext
     * @return ColumnNode containing the Column for the case() eval method
     */
    public Node visitEvalMethodCase(DPLParser.EvalMethodCaseContext ctx) {
        Node rv = evalMethodCaseEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodCaseEmitCatalyst(DPLParser.EvalMethodCaseContext ctx) {
        Node rv = null;

        // case ( x , y , x2 , y2 , x3, y3 , ... )

        if (ctx.getChildCount() % 2 != 0) {
            throw new UnsupportedOperationException(
                    "The amount of arguments given was invalid. Make sure each condition has a matching value given as an argument."
            );
        }

        Column condition = null;
        Column value = null;
        Column res = null;

        for (int i = 0; i < ctx.getChildCount(); ++i) {
            // Skip TerminalNode (keyword case, parenthesis, comma)
            if (ctx.getChild(i) instanceof TerminalNode)
                continue;

            condition = ((ColumnNode) visit(ctx.getChild(i))).getColumn();
            value = ((ColumnNode) visit(ctx.getChild(i + 2))).getColumn();
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
     * validate() eval method Opposite of 'case(x,y)', returns the first y where x=false
     * 
     * @param ctx EvalMethodValidateContext
     * @return ColumnNode containing the Column for the validate() eval method
     */
    @Override
    public Node visitEvalMethodValidate(DPLParser.EvalMethodValidateContext ctx) {
        Node rv = evalMethodValidateEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodValidateEmitCatalyst(DPLParser.EvalMethodValidateContext ctx) {
        Node rv = null;

        // validate ( x , y , x2 , y2 , x3, y3 , ... )

        if (ctx.getChildCount() % 2 != 0) {
            throw new UnsupportedOperationException(
                    "The amount of arguments given was invalid. Make sure each condition has a matching value given as an argument."
            );
        }

        Column condition = null;
        Column value = null;
        Column res = null;

        for (int i = 0; i < ctx.getChildCount(); ++i) {
            // Skip TerminalNode (keyword validate, parenthesis, comma)
            if (ctx.getChild(i) instanceof TerminalNode)
                continue;

            condition = ((ColumnNode) visit(ctx.getChild(i))).getColumn();
            value = ((ColumnNode) visit(ctx.getChild(i + 2))).getColumn();
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
     * cidrmatch() eval method x= cidr subnet, y= ip address to match with the subnet x
     * 
     * @param ctx EvalMethodCidrmatchContext
     * @return ColumnNode containing the Column for the cidrmatch() eval method
     */
    @Override
    public Node visitEvalMethodCidrmatch(DPLParser.EvalMethodCidrmatchContext ctx) {
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
     * coalesce() eval method Returns the first non-null argument
     * 
     * @param ctx EvalMethodCoalesceContext
     * @return ColumnNode containing the Column for the coalesce() eval method
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
        for (int i = 2; i <= ctx.getChildCount() - 2; i = i + 2) {
            ColumnNode currentItemNode = ((ColumnNode) visit(ctx.getChild(i)));
            Column currentItem = currentItemNode.getColumn();

            columnList.add(currentItem);
        }

        res = functions.coalesce(JavaConversions.asScalaBuffer(columnList));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * in() eval method Returns if the first column's value is any of the other arguments
     * 
     * @param ctx EvalMethodInContext
     * @return ColumnNode containing the Column for the in() eval method
     */
    @Override
    public Node visitEvalMethodIn(DPLParser.EvalMethodInContext ctx) {
        Node rv = evalMethodInEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodInEmitCatalyst(DPLParser.EvalMethodInContext ctx) {
        Node rv = null;

        // Get field as column
        Column field = ((ColumnNode) visit(ctx.getChild(2))).getColumn();

        // Rest of the arguments are values, and are processed as strings
        List<String> valueList = new ArrayList<>();
        for (int i = 4; i < ctx.getChildCount() - 1; i = i + 2) {
            String value = ctx.getChild(i).getText();
            valueList.add(new UnquotedText(new TextString(value)).read());
        }

        field = field.isInCollection(valueList);

        rv = new ColumnNode(field);

        return rv;
    }

    /**
     * like() eval method Returns TRUE if field is like pattern Pattern supports wildcards % (multi char) and _ (single
     * char)
     * 
     * @param ctx EvalMethodLikeContext
     * @return ColumnNode containing the Column for the like() eval method
     */
    @Override
    public Node visitEvalMethodLike(DPLParser.EvalMethodLikeContext ctx) {
        Node rv = evalMethodLikeEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodLikeEmitCatalyst(DPLParser.EvalMethodLikeContext ctx) {
        Node rv = null;

        // like ( text , pattern )
        Column textCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        String pattern = ctx.getChild(4).getText();

        rv = new ColumnNode(textCol.like(new UnquotedText(new TextString(pattern)).read()));

        return rv;
    }

    /**
     * match() eval method Returns true if regex matches the subject
     * 
     * @param ctx EvalMethodMatchContext
     * @return ColumnNode containing the Column for the match() eval method
     */
    @Override
    public Node visitEvalMethodMatch(DPLParser.EvalMethodMatchContext ctx) {
        Node rv = evalMethodMatchEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodMatchEmitCatalyst(DPLParser.EvalMethodMatchContext ctx) {
        Node rv = null;

        // match ( subject , regex )
        Column subjectCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column regexCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();

        this.booleanExpFieldName = ctx.getChild(2).getText(); // TODO for mvfilter()

        // Register and use UDF regexMatch
        UserDefinedFunction regexMatch = functions.udf(new RegexMatch(catCtx.nullValue), DataTypes.BooleanType);
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("regexMatch", regexMatch);

        Column col = functions.callUDF("regexMatch", subjectCol, regexCol);
        rv = new ColumnNode(col);

        return rv;
    }

    /**
     * tostring() eval method Returns different types of strings based on given second argument
     * 
     * @param ctx EvalMethodTostringContext
     * @return ColumnNode containing the Column for the tostring() eval method
     */
    @Override
    public Node visitEvalMethodTostring(DPLParser.EvalMethodTostringContext ctx) {
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
        if (ctx.getChildCount() > 4)
            options = new UnquotedText(new TextString(ctx.getChild(4).getText())).read();

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
                    throw new UnsupportedOperationException(
                            "Unsupported optional argument supplied: '" + options
                                    + "'.The argument must be 'hex', 'commas' or 'duration' instead."
                    );
            }
        }

        rv = new ColumnNode(col.cast("string"));
        return rv;
    }

    /**
     * tonumber() eval method Returns number string converted to given base, defaults to base-10
     * 
     * @param ctx EvalMethodTonumberContext
     * @return ColumnNode containing the Column for the tonumber() eval method
     */
    @Override
    public Node visitEvalMethodTonumber(DPLParser.EvalMethodTonumberContext ctx) {
        Node rv = evalMethodTonumberEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodTonumberEmitCatalyst(DPLParser.EvalMethodTonumberContext ctx) {
        Node rv = null;

        // tonumber ( numstr , base )
        // if base is not given, defaults to base-10
        Column numstrCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column baseCol = functions.lit(10);

        if (ctx.getChildCount() > 4) {
            baseCol = ((ColumnNode) visit(ctx.getChild(4))).getColumn();
        }

        // Register and use UDF toNumber
        UserDefinedFunction toNumber = functions.udf(new Tonumber(catCtx.nullValue), DataTypes.LongType);
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("toNumber", toNumber);

        Column col = functions.callUDF("toNumber", numstrCol, baseCol);
        rv = new ColumnNode(col);

        return rv;
    }

    /**
     * md5() eval method Returns the md5 checksum of given field
     * 
     * @param ctx EvalMethodMd5Context
     * @return ColumnNode containing the Column for the md5() eval method
     */
    @Override
    public Node visitEvalMethodMd5(DPLParser.EvalMethodMd5Context ctx) {
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
     * sha1() eval method Returns the sha1 checksum of given field
     * 
     * @param ctx EvalMethodSha1Context
     * @return ColumnNode containing the Column for the sha1() eval method
     */
    @Override
    public Node visitEvalMethodSha1(DPLParser.EvalMethodSha1Context ctx) {
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
     * sha256() eval method Returns the sha256 checksum of given field
     * 
     * @param ctx EvalMethodSha256Context
     * @return ColumnNode containing the Column for the sha256() eval method
     */
    @Override
    public Node visitEvalMethodSha256(DPLParser.EvalMethodSha256Context ctx) {
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
     * sha512() eval method Returns the sha512 checksum of given field
     * 
     * @param ctx EvalMethodSha512Context
     * @return ColumnNode containing the column for the sha512() eval method
     */
    @Override
    public Node visitEvalMethodSha512(DPLParser.EvalMethodSha512Context ctx) {
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
     * isbool() eval method Returns whether or not the field value is a boolean
     * 
     * @param ctx EvalMethodIsboolContext
     * @return ColumnNode containing the Column for the isbool() eval method
     */
    @Override
    public Node visitEvalMethodIsbool(DPLParser.EvalMethodIsboolContext ctx) {
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
     * isint() eval method Returns whether or not the field value is an integer
     * 
     * @param ctx EvalMethodIsintContext
     * @return ColumnNode containing the Column for the isint() eval method
     */
    @Override
    public Node visitEvalMethodIsint(DPLParser.EvalMethodIsintContext ctx) {
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
     * isnum() eval method Returns whether or not the field value is a numeric
     * 
     * @param ctx EvalMethodIsnumContext
     * @return ColumnNode containing the Column for the isnum() eval method
     */
    @Override
    public Node visitEvalMethodIsnum(DPLParser.EvalMethodIsnumContext ctx) {
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
     * isstr() eval method Returns whether or not the field value is a string
     * 
     * @param ctx EvalMethodIsstrContext
     * @return ColumnNode containing the Column for the isstr() eval method
     */
    @Override
    public Node visitEvalMethodIsstr(DPLParser.EvalMethodIsstrContext ctx) {
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
     * typeof() eval method Returns the type of the field
     * 
     * @param ctx EvalMethodTypeofContext
     * @return ColumnNode containing the Column for the typeof() eval method
     */
    @Override
    public Node visitEvalMethodTypeof(DPLParser.EvalMethodTypeofContext ctx) {
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
     * isnull() eval method Returns whether or not the field value is a null
     * 
     * @param ctx EvalMethodIsnullContext
     * @return ColumnNode containing the Column for the isnull() eval method
     */
    @Override
    public Node visitEvalMethodIsnull(DPLParser.EvalMethodIsnullContext ctx) {
        Node rv = evalMethodIsnullEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodIsnullEmitCatalyst(DPLParser.EvalMethodIsnullContext ctx) {
        Node rv = null;

        Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column res = xCol.eqNullSafe(functions.lit(catCtx.nullValue.value()).cast(DataTypes.StringType));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * isnotnull() eval method Returns whether or not the field value is a non-null
     * 
     * @param ctx EvalMethodIsnotnullContext
     * @return ColumnNode containing the Column for the isnotnull() eval method
     */
    @Override
    public Node visitEvalMethodIsnotnull(DPLParser.EvalMethodIsnotnullContext ctx) {
        Node rv = evalMethodIsnotnullEmitCatalyst(ctx);
        return rv;
    }

    private Node evalMethodIsnotnullEmitCatalyst(DPLParser.EvalMethodIsnotnullContext ctx) {
        Node rv = null;

        Column xCol = ((ColumnNode) visit(ctx.getChild(2))).getColumn();
        Column res = functions.not(xCol.eqNullSafe(functions.lit(catCtx.nullValue.value()).cast(DataTypes.StringType)));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * commands() eval method Returns the commands used in given search string
     * 
     * @param ctx EvalMethodCommandsContext
     * @return ColumnNode containing the Column for the commands() eval method
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
        UserDefinedFunction CommandsUDF = functions
                .udf(new Commands(), DataTypes.createArrayType(DataTypes.StringType, false));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("CommandsUDF", CommandsUDF);

        Column res = functions.callUDF("CommandsUDF", searchString);
        rv = new ColumnNode(res);

        return rv;
    }

    /**
     * mvappend() eval method Returns a multivalue field with all arguments as values
     * 
     * @param ctx EvalMethodMvappendContext
     * @return ColumnNode containing the Column for the mvappend() eval method
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
            if (field != null)
                listOfFields.add(field);
        }

        Column res = functions.array(JavaConversions.asScalaBuffer(listOfFields));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * mvcount() eval method Returns the amount of items in the multivalue field
     * 
     * @param ctx EvalMethodMvcountContext
     * @return ColumnNode containing the Column for the mvcount() eval method
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
        Column res = functions
                .when(sizeCol.notEqual(functions.lit(0)), sizeCol)
                .otherwise(functions.lit(catCtx.nullValue.value()).cast(DataTypes.StringType));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * mvdedup() eval method Returns the given multivalue field with deduplicated values
     * 
     * @param ctx EvalMethodMvdedupContext
     * @return ColumnNode containing the column for the mvdedup() eval method
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
        UserDefinedFunction mvDedupUDF = functions
                .udf(new Mvdedup(), DataTypes.createArrayType(DataTypes.StringType, false));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("mvDedupUDF", mvDedupUDF);

        Column res = functions.callUDF("mvDedupUDF", mvfield);

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * mvfilter() eval method Returns the values in a multivalue field that pass the given regex filter TODO Implement,
     * requires? work on the parser side
     * 
     * @param ctx EvalMethodMvfilterContext
     * @return ColumnNode containing Column for the mvfilter() eval method
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
     * mvfind() eval method Returns the values that match the given regex in the multivalue field provided
     * 
     * @param ctx EvalMethodMvfindContext
     * @return ColumnNode containing the Column for the mvfind() eval method
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
        UserDefinedFunction regexMatch = functions
                .udf(new RegexMatch(isMultiValue, catCtx.nullValue), DataTypes.IntegerType);
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("regexMatch", regexMatch);

        Column col = functions.callUDF("regexMatch", mvfield, regex);
        rv = new ColumnNode(col);

        return rv;
    }

    /**
     * mvindex() eval method Returns the values of the multivalue field between the given indices
     * 
     * @param ctx EvalMethodMvindexContext
     * @return ColumnNode containing Column for the mvindex() eval method
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
        UserDefinedFunction mvIndexUDF = functions
                .udf(new Mvindex(), DataTypes.createArrayType(DataTypes.StringType, false));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("mvIndexUDF", mvIndexUDF);

        Column res = functions
                .callUDF("mvIndexUDF", mvField, startIndex, endIndex == null ? functions.lit(-1) : endIndex, functions.lit(endIndex != null));

        rv = new ColumnNode(res);
        return rv;
    }

    /**
     * mvjoin() eval method Returns the multivalue field's items concatenated with given delimiter in between each value
     * 
     * @param ctx EvalMethodMvjoinContext
     * @return ColumnNode containing Column for the mvjoin() eval method
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
     * mvrange() eval method Returns a multivalue field with numbers from start to end with step.
     * 
     * @param ctx EvalMethodMvrangeContext
     * @return ColumnNode containing Column for mvrange() eval method
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
        UserDefinedFunction MvrangeUDF = functions
                .udf(new Mvrange(), DataTypes.createArrayType(DataTypes.StringType, false));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("MvrangeUDF", MvrangeUDF);

        Column res = functions.callUDF("MvrangeUDF", start, end, step);
        rv = new ColumnNode(res);

        return rv;
    }

    /**
     * mvsort() eval method Returns the given multivalue field sorted
     * 
     * @param ctx EvalMethodMvsortContext
     * @return ColumnNode containing Column for mvsort() eval method
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
     * mvzip() eval method Returns the two multivalue field's values "zipped" together, optionally with a delimiter
     * 
     * @param ctx EvalMethodMvzipContext
     * @return ColumnNode containing Column for mvzip() eval method
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
        UserDefinedFunction MvzipUDF = functions
                .udf(new Mvzip(), DataTypes.createArrayType(DataTypes.StringType, false));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("MvzipUDF", MvzipUDF);

        Column res = functions
                .callUDF("MvzipUDF", mvfield1, mvfield2, delimiter != null ? delimiter : functions.lit(","));
        rv = new ColumnNode(res);

        return rv;
    }

    /**
     * JSONValid() eval method Returns whether or not the given field contains valid json
     * 
     * @param ctx EvalMethodJSONValidContext
     * @return ColumnNode containing Column for JSONValid() eval method
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
     * spath() eval method Processes the spath/xpath expression and returns the results
     * 
     * @param ctx EvalMethodSpathContext
     * @return ColumnNode containing Column for spath() eval method
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
        UserDefinedFunction SpathUDF = functions
                .udf(new Spath(catCtx.nullValue), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("SpathUDF", SpathUDF);

        Column res = functions.callUDF("SpathUDF", input, spathExpr, functions.lit(""), functions.lit(""));
        // wrap expression in backticks to escape dots
        rv = new ColumnNode(res.getItem(new QuotedText(new TextString(spathExpr.toString()), "`").read()));

        return rv;
    }

    /**
     * time() eval method Returns the current time in seconds
     * 
     * @param ctx EvalMethodTimeContext
     * @return ColumnNode containing Column for time() eval method
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
        double currentTimeInSecs = ((double) now.getEpochSecond())
                + ((((double) now.getNano() / 1000d) / 1000d) / 1000d);

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
        LOGGER.debug("Visit Eval Field type");

        Column col = new Column(new UnquotedText(new TextString(ctx.getChild(0).getText())).read());
        rv = new ColumnNode(col);

        return rv;
    }

    public Node visitEvalIntegerType(DPLParser.EvalIntegerTypeContext ctx) {
        Node rv = null;
        LOGGER.debug("Visit Eval Integer type");

        Column col = functions.lit(Integer.parseInt(ctx.getChild(0).getText())); //new Column(ctx.getChild(0).getText());
        rv = new ColumnNode(col);

        return rv;
    }

    public Node visitEvalNumberType(DPLParser.EvalNumberTypeContext ctx) {
        Node rv = null;
        LOGGER.debug("Visit Eval Number type");

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
        LOGGER.debug("Visit Eval String type");

        String text = ctx.getChild(0).getText();
        text = new UnquotedText(new TextString(text)).read();
        // unescape string literals inside a string value
        text = text.replace("\\\"", "\"");
        Column col = functions.lit(text);
        rv = new ColumnNode(col);

        return rv;
    }

    @Override
    public Node visitL_evalStatement_evalCalculateStatement_multipliers(
            DPLParser.L_evalStatement_evalCalculateStatement_multipliersContext ctx
    ) {
        return evalCalculateStatementEmitCatalyst(ctx);
    }

    @Override
    public Node visitL_evalStatement_evalCalculateStatement_minus_plus(
            DPLParser.L_evalStatement_evalCalculateStatement_minus_plusContext ctx
    ) {
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
    public Node visitL_evalStatement_evalConcatenateStatement(
            DPLParser.L_evalStatement_evalConcatenateStatementContext ctx
    ) {
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
        StringNode sNode = new StringNode(new Token(Type.STRING, sql));
        return sNode;
        }
        */
    }

}
