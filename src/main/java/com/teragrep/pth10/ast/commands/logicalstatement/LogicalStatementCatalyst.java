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
package com.teragrep.pth10.ast.commands.logicalstatement;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.jue_01.GlobToRegEx;
import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.evalstatement.EvalStatement;
import com.teragrep.pth10.ast.commands.logicalstatement.UDFs.SearchComparison;
import com.teragrep.pth10.ast.commands.transformstatement.ChartTransformation;
import com.teragrep.pth10.ast.commands.transformstatement.FieldsTransformation;
import com.teragrep.pth10.steps.NullStep;
import com.teragrep.pth10.steps.logicalCatalyst.LogicalCatalystStep;
import com.teragrep.pth10.steps.subsearch.SubsearchStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.ParserRuleContext;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <p>
 * Contains the visitor functions for logicalStatement, which is used for the main search function of the language.
 * </p>
 * <p>
 * These functions help to build the necessary archive query and Spark actions.
 * </p>
 * Example: <pre>index=cinnamon earliest=-1y latest=-1d</pre>
 * <p>
 * After the main logicalStatement, multiple
 * {@link com.teragrep.pth10.ast.commands.transformstatement.TransformStatement transformStatements} that contain
 * aggregations and other functions can be chained, or left unused if the user wants to perform a basic search.
 * </p>
 */
public class LogicalStatementCatalyst extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalStatementCatalyst.class);

    private final DPLParserCatalystContext catCtx;
    private DPLParserCatalystVisitor catVisitor;

    // imported transformations
    private EvalStatement evalStatement;
    private FieldsTransformation fieldsTransformation;
    private ChartTransformation chartTransformation = null;

    public LogicalStatementCatalyst(DPLParserCatalystVisitor catVisitor, DPLParserCatalystContext catCtx) {
        this.catVisitor = catVisitor;
        this.catCtx = catCtx;

        chartTransformation = new ChartTransformation(catCtx);
        fieldsTransformation = new FieldsTransformation(catCtx);
        evalStatement = new EvalStatement(catCtx);
    }

    public LogicalStatementCatalyst(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    /**
     * Visits the parse tree for SearchTransformationRoot and returns a LogicalCatalystStep that can be added to
     * Steptree in DPLParserCatalystVisitor.
     * 
     * @param ctx SearchTransformationRootContext
     * @return LogicalCatalystStep
     */
    public AbstractStep visitLogicalStatementCatalyst(DPLParser.SearchTransformationRootContext ctx) {
        if (ctx != null) {
            final Node ret = visitSearchTransformationRoot(ctx);
            if (ret != null) {
                final ColumnNode colNode = (ColumnNode) visitSearchTransformationRoot(ctx);
                if (colNode.getColumn() != null) {
                    final Column filterColumn = colNode.getColumn();
                    return new LogicalCatalystStep(filterColumn);
                }
            }
        }
        return new NullStep();
    }

    /**
     * The main visitor function for searchTransformation, used for the main search function. <pre>
     *     root : searchTransformationRoot transformStatement?
     *     searchTransformationRoot : logicalStatement
     * </pre>
     * 
     * @param ctx SearchTransformationRoot context
     * @return logicalStatement columnNode
     */
    @Override
    public Node visitSearchTransformationRoot(DPLParser.SearchTransformationRootContext ctx) {
        final ColumnNode rv;
        if (LOGGER.isInfoEnabled()) {
            LOGGER
                    .info(
                            "[SearchTransformationRoot CAT] Visiting: <{}> with <{}> children", ctx.getText(),
                            ctx.getChildCount()
                    );
        }

        if (ctx.getChildCount() == 1) {
            // just a single directoryStatement -or- logicalStatement
            rv = (ColumnNode) visit(ctx.getChild(0));
        }
        else {
            final ParseTree secondChild = ctx.getChild(1);

            if (
                secondChild instanceof TerminalNode && ((TerminalNode) secondChild).getSymbol().getType() == DPLLexer.OR
            ) {
                // case: directoryStmt OR logicalStmt
                final ColumnNode dirStatColumnNode = (ColumnNode) visit(ctx.directoryStatement());
                final ColumnNode logiStatColumnNode = (ColumnNode) visit(ctx.logicalStatement(0));
                rv = new ColumnNode(dirStatColumnNode.getColumn().or(logiStatColumnNode.getColumn()));
            }
            else {
                // case: (logicalStmt AND?)*? directoryStmt (AND? logicalStmt)*?
                Column finalColumn = ((ColumnNode) visit(ctx.directoryStatement())).getColumn();

                for (DPLParser.LogicalStatementContext logiStatCtx : ctx.logicalStatement()) {
                    finalColumn = finalColumn.and(((ColumnNode) visit(logiStatCtx)).getColumn());
                }

                rv = new ColumnNode(finalColumn);
            }
        }

        if (rv != null && rv.getColumn() != null) {
            this.catCtx.setSparkQuery(rv.getColumn().toString());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Spark column: <{}>", rv.getColumn().toString());
            }
        }

        return rv;
    }

    /**
     * Prints parse tree string, IF catCtx has been setRuleNames(parser.getRuleNames())
     * 
     * @param ctx current context
     * @return parse tree as string
     */
    private String getParseTreeString(ParserRuleContext ctx) {
        if (catCtx.getRuleNames() != null) {
            ParserRuleContext curr = ctx;

            //  Search root;
            while (curr.getParent() != null) {
                curr = curr.getParent();
            }
            PrettyTree prettyTree = new PrettyTree(curr.getChild(0), Arrays.asList(catCtx.getRuleNames()));
            return (prettyTree.getTree());
        }
        return "<<Rulenames not provided to the context, cannot print parse tree>>";
    }

    @Override
    public Node visitDirectoryStatement(DPLParser.DirectoryStatementContext ctx) {
        Node rv = null;
        Element el = null;
        List<Node> listOfElements = new ArrayList<>();
        boolean orMode = false;

        if (ctx.getChildCount() == 1) {
            // subindexStatement / indexStatement
            LOGGER.debug("[DirStmt] Only one child. Visiting class= <{}>", ctx.getChild(0).getClass());
            rv = visit(ctx.getChild(0));
        }
        else {
            LOGGER.debug("[DirStmt] Multiple children detected: <{}> children", ctx.getChildCount());
            // directoryStmt OR directoryStmt
            // directoryStmt (AND*? logicalStmt)+
            // (logicalStmt AND? )+ directoryStmt

            ParseTree secondChild = ctx.getChild(1);

            // check if directoryStmt OR directoryStmt
            if (
                secondChild instanceof TerminalNode && ((TerminalNode) secondChild).getSymbol().getType() == DPLLexer.OR
            ) {
                LOGGER.debug("[DirStmt] OR detected");
                orMode = true;
            }

            for (ParseTree child : ctx.children) {
                // Don't visit OR and AND operators
                if (!(child instanceof TerminalNode)) {
                    listOfElements.add(visit(child));
                    LOGGER.debug("[DirStmt] Added class=<{}> to listOfElements.", child.getClass());
                }
            }
        }

        // filter nulls out
        listOfElements = listOfElements.stream().filter(Objects::nonNull).collect(Collectors.toList());

        if (ctx.getChildCount() > 1) {
            Column ret = null;
            Dataset<Row> subSearchDs = null;
            for (Node n : listOfElements) {
                Column c = null;

                if (n instanceof SubSearchNode) {
                    c = ((SubSearchNode) n).getColumn();
                }
                else if (n instanceof ColumnNode) {
                    c = ((ColumnNode) n).getColumn();
                }
                else if (n instanceof CatalystNode) {
                    LOGGER.debug("Got dataset from sub search");
                    subSearchDs = ((CatalystNode) n).getDataset();
                    continue;
                }
                else {
                    // null?
                    continue;
                }

                if (ret == null) {
                    ret = c;
                }
                else {
                    if (orMode) {
                        ret = ret.or(c);
                    }
                    else {
                        ret = ret.and(c);
                    }
                }
            }

            rv = new ColumnNode(ret);
        }

        return rv;
    }

    /**
     * <pre>
     * logicalStatement :
     *   macroStatement
     * | subsearchStatement
     * | sublogicalStatement
     * | timeStatement
     * | searchQualifier
     * | NOT logicalStatement
     * | indexStatement
     * | comparisonStatement
     * | logicalStatement OR logicalStatement
     * | logicalStatement AND? logicalStatement
     *  ;
     * </pre>
     */
    @Override
    public Node visitLogicalStatement(DPLParser.LogicalStatementContext ctx) {
        LOGGER.info("logicalStatement (Catalyst) incoming: <{}>", ctx.getText());
        Node rv = null;
        Node left = null;
        TerminalNode leftIsTerminal = null;

        if (!(ctx.getChild(0) instanceof TerminalNode)) {
            left = visit(ctx.getChild(0));
        }
        else {
            leftIsTerminal = (TerminalNode) ctx.getChild(0);
            if (leftIsTerminal.getSymbol().getType() != DPLLexer.NOT) {
                throw new RuntimeException("Unsupported unary logical operation: " + ctx.getText());
            }
        }

        if (ctx.getChildCount() == 1) {
            // leaf
            rv = left;
        }
        else if (ctx.getChildCount() == 2) {
            Node right = visit(ctx.getChild(1));
            if (leftIsTerminal != null) {
                Column r = ((ColumnNode) right).getColumn();
                // Use unary operation, currently only NOT is supported
                rv = new ColumnNode(functions.not(r));
            }
            else {
                if (left instanceof ColumnNode && right instanceof ColumnNode) {
                    Column l = ((ColumnNode) left).getColumn();
                    Column r = ((ColumnNode) right).getColumn();
                    // Add missing AND between elements
                    rv = new ColumnNode(l.and(r));
                }
            }

        }
        else if (ctx.getChildCount() == 3) {
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Node right = visit(ctx.getChild(2));
            if (left instanceof ColumnNode && right instanceof ColumnNode) {
                Column l = ((ColumnNode) left).getColumn();
                Column r = ((ColumnNode) right).getColumn();
                // resolve operation
                if (DPLLexer.AND == operation.getSymbol().getType()) {
                    rv = new ColumnNode(l.and(r));
                }
                else if (DPLLexer.OR == operation.getSymbol().getType())
                    rv = new ColumnNode(l.or(r));
                else {
                    throw new RuntimeException("Unsupported logical operation:" + operation);
                }
            }
        }

        if (rv instanceof SubSearchNode) {
            LOGGER.info("[CAT] [LogiStat] Return value was SubsearchNode. Converting to ColumnNode!");
            rv = new ColumnNode(((SubSearchNode) rv).getColumn());
        }

        LOGGER.debug("visitLogicalStatement outgoing: <{}>", rv);
        return rv;
    }

    @Override
    public Node visitSearchIndexStatement(DPLParser.SearchIndexStatementContext ctx) {
        Column rv = new Column("_raw");
        if (ctx.stringType() != null) {
            String statement = ctx.stringType().getText();
            LOGGER.debug("Got a statement : <[{}]>", statement);

            statement = StringEscapeUtils.unescapeJava(statement); // unescape any escaped chars
            LOGGER.debug("Unescaped : <{}>", statement);

            statement = new UnquotedText(new TextString(statement)).read(); // strip outer quotes
            LOGGER.info("Stripped quotes : <{}>", statement);

            // remove leading and trailing wildcards, if present
            if (statement.startsWith("*")) {
                statement = statement.substring(1);
            }

            if (statement.endsWith("*")) {
                statement = statement.substring(0, statement.length() - 1);
            }

            statement = "(?i)^.*".concat(Pattern.quote(statement)).concat(".*"); // prefix with case insensitivity
            LOGGER.debug("Final : <{}>", statement);

            rv = rv.rlike(statement);
        }

        LOGGER.info("[CAT] SearchIndexStatement return: <{}>", rv.toString());
        return new ColumnNode(rv);
    }

    /**
     * sublogicalStatement : PARENTHESIS_L logicalStatement PARENTHESIS_R ;
     */
    public Node visitSublogicalStatement(DPLParser.SublogicalStatementContext ctx) {
        // Consume parenthesis
        Node n = visit(ctx.getChild(1));
        return n;
    }

    /**
     * subindexStatement : PARENTHESIS_L directoryStatement PARENTHESIS_R ;
     */
    @Override
    public Node visitSubindexStatement(DPLParser.SubindexStatementContext ctx) {
        // Consume parenthesis
        return visit(ctx.getChild(1));
    }

    /**
     * searchQualifier : INDEX (EQ|NEQ) stringType WILDCARD? | SOURCETYPE (EQ|NEQ) stringType WILDCARD? | HOST (EQ|NEQ)
     * stringType WILDCARD? | SOURCE (EQ|NEQ) stringType WILDCARD? | SAVEDSEARCH (EQ|NEQ) stringType WILDCARD? |
     * EVENTTYPE (EQ|NEQ) stringType WILDCARD? | EVENTTYPETAG (EQ|NEQ) stringType WILDCARD? | HOSTTAG (EQ|NEQ)
     * stringType WILDCARD? | TAG (EQ|NEQ) stringType WILDCARD? ;
     */
    @Override
    public Node visitSearchQualifier(DPLParser.SearchQualifierContext ctx) {
        Column sQualifier;
        String value;

        TerminalNode left = (TerminalNode) ctx.getChild(0);
        TerminalNode operation = (TerminalNode) ctx.getChild(1);

        // Default clause used in WHERE-part
        final String columnName;
        // HOST and SOURCETYPE qualifier stored as additional list and used for Kafka content filtering
        if (left.getSymbol().getType() == DPLLexer.INDEX_IN) {
            value = "";
            columnName = "index";
        }
        else if (left.getSymbol().getType() == DPLLexer.HOST) {
            value = new UnquotedText(new TextString(ctx.getChild(2).getText().toLowerCase())).read();
            columnName = "host";
        }
        else if (left.getSymbol().getType() == DPLLexer.SOURCETYPE) {
            value = new UnquotedText(new TextString(ctx.getChild(2).getText())).read();// don't force cases .toLowerCase());
            columnName = "sourcetype";
        }
        else {
            // other column=value qualifier
            value = new UnquotedText(new TextString(ctx.getChild(2).getText())).read();
            columnName = ctx.getChild(0).getText();
        }

        Column col = new Column(columnName);

        // check whether operation is '=' or '!='
        if (left.getSymbol().getType() != DPLLexer.INDEX_IN && operation.getSymbol().getType() == DPLLexer.EQ) {
            // Use like instead of '=' so that jokers work
            String rlikeStatement = glob2rlike(value);
            sQualifier = col.rlike(rlikeStatement);

        }
        else if (left.getSymbol().getType() == DPLLexer.INDEX_IN) {
            OrColumn orColumn = new OrColumn(
                    ctx
                            .indexStringType()
                            .stream()
                            .map(st -> col.rlike(glob2rlike(new UnquotedText(new TextString(st.getText().toLowerCase())).read()))).collect(Collectors.toList())
            );

            sQualifier = orColumn.column();
        }
        else if (left.getSymbol().getType() == DPLLexer.SOURCETYPE && operation.getSymbol().getType() == DPLLexer.IN) {
            OrColumn orColumn = new OrColumn(
                    ctx
                            .stringType()
                            .stream()
                            .map(st -> col.rlike(glob2rlike(new UnquotedText(new TextString(st.getText().toLowerCase())).read()))).collect(Collectors.toList())
            );

            sQualifier = orColumn.column();
        }
        else {
            String rlikeStatement = glob2rlike(value);
            sQualifier = functions.not(col.rlike(rlikeStatement));
        }

        return new ColumnNode(sQualifier);
    }

    /**
     * Converts glob-type string to one compatible with spark's rlike function
     *
     * @param glob glob-type string
     * @return rlike statement
     */
    private String glob2rlike(String glob) {
        if (glob != null) {
            return "(?i)^" + glob.replaceAll("\\*", ".*");
        }
        throw new RuntimeException("glob2rlike: Provided glob string was null");
    }

    /**
     * stringType WILDCARD? | WILDCARD | termStatement | caseStatement
     *
     * @return StringNode,
     */
    @Override
    public Node visitIndexStatement(DPLParser.IndexStatementContext ctx) {
        TerminalNode index = (TerminalNode) ctx.getChild(0);
        String value = new UnquotedText(new TextString(ctx.getChild(1).getText())).read();

        Column indexCol = null;
        switch (index.getSymbol().getType()) {
            case DPLLexer.INDEX_EQ:
            case DPLLexer.INDEX_SPACE:
                indexCol = new Column("index").rlike("(?i)".concat(GlobToRegEx.regexify(value)));
                break;
            case DPLLexer.INDEX_NEG:
            case DPLLexer.INDEX_SPACE_NEG:
                indexCol = functions.not(new Column("index").rlike("(?i)".concat(GlobToRegEx.regexify(value))));
                break;
            default:
                throw new UnsupportedOperationException("Index type not supported");
        }

        ColumnNode node = new ColumnNode(indexCol);
        LOGGER.debug("Got index statement: ColumnNode=<{}>", node);
        return node;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns the result of calling {@link #visitChildren} on {@code ctx}.
     * </p>
     */
    @Override
    public Node visitComparisonStatement(DPLParser.ComparisonStatementContext ctx) {
        Column col = null;
        Column rv = null;

        String value = new UnquotedText(new TextString(ctx.getChild(1).getText())).read();
        String field = new UnquotedText(new TextString(ctx.getChild(0).getText())).read();

        LOGGER.info("[CAT] [ComparisonStmt] field <{}> = value <{}>", field, value);

        boolean specialCase = false;
        if (ctx.getChild(0) instanceof TerminalNode) {
            TerminalNode specialLefthandSide = (TerminalNode) ctx.getChild(0);
            switch (specialLefthandSide.getSymbol().getType()) {
                case DPLLexer.INDEX_SPACE:
                case DPLLexer.INDEX_EQ:
                    field = "index";
                    specialCase = true;
                    rv = new Column(field).rlike(GlobToRegEx.regexify(value));
                    break;
                case DPLLexer.INDEX_SPACE_NEG:
                case DPLLexer.INDEX_NEG:
                    field = "index";
                    specialCase = true;
                    rv = functions.not(new Column(field).rlike(GlobToRegEx.regexify(value)));
                    break;
            }
        }

        if (!specialCase) {
            col = new Column(field);
            rv = this
                    .addOperation(col, (TerminalNode) ctx.getChild(1), new UnquotedText(new TextString(ctx.getChild(2).getText())).read());
        }

        return new ColumnNode(rv);
    }

    private Column addOperation(Column source, TerminalNode operation, String value) {
        Column rv = null;

        SparkSession ss = catCtx.getSparkSession();
        ss.udf().register("Comparison", new SearchComparison(), DataTypes.BooleanType);
        rv = functions
                .callUDF("Comparison", source, functions.lit(operation.getSymbol().getType()), functions.lit(value));

        return rv;
    }

    @Override
    public Node visitSubsearchStatement(DPLParser.SubsearchStatementContext ctx) {
        LOGGER.info("visitSubsearchStatement with brackets: <{}>", ctx.getText());
        // Strip brackets around statement
        DPLParserCatalystContext subCtx = null;
        if (catCtx != null) {
            LOGGER.info("Cloning main visitor to subsearch");
            subCtx = catCtx.clone();
        }
        LOGGER.info("(Catalyst) subVisitor init with subCtx= <{}>", subCtx);
        DPLParserCatalystVisitor subVisitor = new DPLParserCatalystVisitor(subCtx);

        // Pass actual subsearch branch
        StepNode subSearchNode = (StepNode) subVisitor.visit(ctx);
        LOGGER.info("SubSearchTransformation (Catalyst) Result: class=<{}>", subSearchNode.getClass().getName());

        SubsearchStep subsearchStep = (SubsearchStep) subSearchNode.get();
        // These have to be set here and not in subVisitor to be the same as in other Steps
        subsearchStep.setListener(this.catCtx.getInternalStreamingQueryListener());
        subsearchStep.setHdfsPath(this.catVisitor.getHdfsPath());

        // add subsearch to stepList
        this.catVisitor.getStepList().add(subsearchStep);

        //Node rv = new CatalystNode(subVisitor.getStack().pop());
        return null;
    }

    /*@Override
    public Node visitSubsearchStatement(DPLParser.SubsearchStatementContext ctx) {
        LOGGER.info("visitSubsearchStatement:" + ctx.getText());
        int count = ctx.getChildCount();
        for (int i = 0; i < count; i++) {
            visit(ctx.getChild(i));
        }
        return visitSearchTransformation(ctx.searchTransformation());
    }*/

    // Time format handling
    @Override
    public Node visitTimeStatement(DPLParser.TimeStatementContext ctx) {
        TimeStatement timeStatement = new TimeStatement(catCtx);

        Node rv = timeStatement.visitTimeStatement(ctx);

        // update min earliest and max latest
        if (timeStatement.getStartTime() != null) {
            LOGGER.info("TimeStatement: Set minimum (earliest) time to: <{}>", timeStatement.getStartTime());
            this.catCtx.setDplMinimumEarliest(timeStatement.getStartTime());
        }

        if (timeStatement.getEndTime() != null) {
            LOGGER.info("TimeStatement: Set maximum (latest) time to: <{}>", timeStatement.getEndTime());
            this.catCtx.setDplMaximumLatest(timeStatement.getEndTime());
        }

        return rv;
    }

    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        return evalStatement.visitL_evalStatement_evalCompareStatement(ctx);
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        String sql = ctx.getChild(0).getText();
        return new StringNode(new Token(Type.STRING, sql));
    }

    /**
     * subEvalLogicalStatement : PARENTHESIS_L subEvalLogicalStatement PARENTHESIS_R ;
     */
    @Override
    public Node visitL_evalStatement_subEvalStatement(DPLParser.L_evalStatement_subEvalStatementContext ctx) {
        return evalStatement.visitL_evalStatement_subEvalStatement(ctx);
    }

    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        return evalStatement.visitAggregateFunction(ctx);
        //return chartTransformation.visitAggregateFunction(ctx);
    }

    @Override
    public Node visitT_chart_by_column_rowOptions(DPLParser.T_chart_by_column_rowOptionsContext ctx) {
        return chartTransformation.visitT_chart_by_column_rowOptions(ctx);
    }

    @Override
    public Node visitT_chart_fieldRenameInstruction(DPLParser.T_chart_fieldRenameInstructionContext ctx) {
        return chartTransformation.visitT_chart_fieldRenameInstruction(ctx);
    }

    @Override
    public Node visitT_eval_evalParameter(DPLParser.T_eval_evalParameterContext ctx) {
        return evalStatement.visitT_eval_evalParameter(ctx);
    }

    public Node visitEvalMethodIf(DPLParser.EvalMethodIfContext ctx) {
        return evalStatement.visitEvalMethodIf(ctx);
    }

    @Override
    public Node visitEvalMethodSubstr(DPLParser.EvalMethodSubstrContext ctx) {
        return evalStatement.visitEvalMethodSubstr(ctx);
    }

    public Node visitEvalMethodTrue(DPLParser.EvalMethodTrueContext ctx) {
        return evalStatement.visitEvalMethodTrue(ctx);
    }

    public Node visitEvalMethodFalse(DPLParser.EvalMethodFalseContext ctx) {
        return evalStatement.visitEvalMethodFalse(ctx);
    }

    public Node visitEvalMethodNull(DPLParser.EvalMethodNullContext ctx) {
        return evalStatement.visitEvalMethodNull(ctx);
    }

    public Node visitEvalMethodNow(DPLParser.EvalMethodNowContext ctx) {
        return evalStatement.visitEvalMethodNow(ctx);
    }

    public Node visitEvalMethodLen(DPLParser.EvalMethodLenContext ctx) {
        return evalStatement.visitEvalMethodLen(ctx);
    }

    public Node visitEvalMethodSplit(DPLParser.EvalMethodSplitContext ctx) {
        return evalStatement.visitEvalMethodSplit(ctx);
    }

    public Node visitEvalMethodStrftime(DPLParser.EvalMethodStrftimeContext ctx) {
        return evalStatement.visitEvalMethodStrftime(ctx);
    }

    public Node visitEvalMethodStrptime(DPLParser.EvalMethodStrptimeContext ctx) {
        return evalStatement.visitEvalMethodStrptime(ctx);
    }

    public Node visitEvalFieldType(DPLParser.EvalFieldTypeContext ctx) {
        return evalStatement.visitEvalFieldType(ctx);
    }

    public Node visitEvalIntegerType(DPLParser.EvalIntegerTypeContext ctx) {
        return evalStatement.visitEvalIntegerType(ctx);
    }

    public Node visitEvalStringType(DPLParser.EvalStringTypeContext ctx) {
        return evalStatement.visitEvalStringType(ctx);
    }

    public Node visitL_evalStatement_evalCalculateStatement_multipliers(
            DPLParser.L_evalStatement_evalCalculateStatement_multipliersContext ctx
    ) {
        return evalStatement.visitL_evalStatement_evalCalculateStatement_multipliers(ctx);
    }

    public Node visitL_evalStatement_evalCalculateStatement_minus_plus(
            DPLParser.L_evalStatement_evalCalculateStatement_minus_plusContext ctx
    ) {
        return evalStatement.visitL_evalStatement_evalCalculateStatement_minus_plus(ctx);
    }

    public Node visitL_evalStatement_evalConcatenateStatement(
            DPLParser.L_evalStatement_evalConcatenateStatementContext ctx
    ) {
        return evalStatement.visitL_evalStatement_evalConcatenateStatement(ctx);
    }

    public Node visitFieldsTransformation(DPLParser.FieldsTransformationContext ctx) {
        return fieldsTransformation.visitFieldsTransformation(ctx);
    }
}
