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

package com.teragrep.pth10.ast.commands.logicalstatement;

import com.teragrep.jue_01.GlobToRegEx;
import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.EmitMode;
import com.teragrep.pth10.ast.commands.evalstatement.EvalStatement;
import com.teragrep.pth10.ast.commands.transformstatement.ChartTransformation;
import com.teragrep.pth10.ast.commands.transformstatement.FieldsTransformation;
import com.teragrep.pth10.datasources.DPLDatasource;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParser.SearchQualifierContext;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.teragrep.pth10.ast.Util.stripQuotes;
import static com.teragrep.pth10.ast.commands.EmitMode.mode.CATALYST;
import static com.teragrep.pth10.ast.commands.EmitMode.mode.XML;

/**
 * <p>Contains the visitor functions for logicalStatement, which is used
 * for the main search function of the language. </p>
 * <p>These functions help to build the necessary archive query and Spark actions.</p>
 * Example:
 * <pre>index=voyager earliest=-1y latest=-1d</pre>
 * <p>After the main logicalStatement, multiple
 * {@link com.teragrep.pth10.ast.commands.transformstatement.TransformStatement transformStatements}
 * that contain aggregations and other functions can be chained, or left unused if the user wants
 * to perform a basic search.</p>
 */
public class LogicalStatement extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalStatement.class);

    private DPLParserCatalystContext catCtx = null;

    private List<String> traceBuffer = null;
    // emit mode. 0=SQL, 1=XML, 2=Catalyst
    private EmitMode.mode mode;

    Document doc = null;
    Dataset<Row> ds = null;

    private boolean isSearchCommand = false; // is used in ' | search' command?

    private boolean aggregatesUsed = false;
    private String aggregateField = null;

    private Stack<String> timeFormatStack = new Stack<>();
    ProcessingStack processingPipe = null;


    // Symbol table for mapping DPL default column names
    private Map<String, Object> symbolTable = new HashMap<>();

    // imported transformations
    private EvalStatement evalStatement;
    private FieldsTransformation fieldsTransformation;
    private ChartTransformation chartTransformation = null;

    // has outside parenthesis global time limits?
    // e.g. (index=a or index=b) latest=-10y
    private boolean hasGlobalEarliest = false;
    private boolean hasGlobalLatest = false;


    public LogicalStatement(Document doc, List<String> buf) {
        this.mode = XML;
        this.doc = doc;
        traceBuffer = buf;
    }

    public LogicalStatement(ProcessingStack processingPipe, DPLParserCatalystContext catCtx, Document doc, List<String> buf) {
        this.mode = XML;
        this.doc = doc;
        traceBuffer = buf;
        this.catCtx = catCtx;
        this.ds = catCtx.getDs();
        this.processingPipe = processingPipe;
    }

    public LogicalStatement(ProcessingStack processingPipe, DPLParserCatalystContext catCtx, List<String> buf) {
        this.mode = CATALYST;
        this.ds = catCtx.getDs();
        this.processingPipe = processingPipe;
        this.catCtx = catCtx;
        traceBuffer = buf;

        chartTransformation = new ChartTransformation(catCtx, processingPipe, buf);
        fieldsTransformation = new FieldsTransformation(catCtx, symbolTable, buf, processingPipe);
        evalStatement = new EvalStatement(catCtx, processingPipe, buf);
        traceBuffer.add("Init logicalStatement(catalyst stack)\n");
    }

    public LogicalStatement(ProcessingStack processingPipe, DPLParserCatalystContext catCtx, List<String> buf, Map<String,Object> symbolTable) {
        this.mode = CATALYST;
        this.ds = catCtx.getDs();
        this.processingPipe = processingPipe;
        this.catCtx = catCtx;
        traceBuffer = buf;
        this.symbolTable = symbolTable;

        chartTransformation = new ChartTransformation(catCtx, processingPipe, buf);
        fieldsTransformation = new FieldsTransformation(catCtx, symbolTable, buf, processingPipe);
        evalStatement = new EvalStatement(catCtx, processingPipe, buf);
        traceBuffer.add("Init logicalStatement(catalyst stack)\n");
    }

    public LogicalStatement(ProcessingStack stack, DPLParserCatalystContext catCtx, boolean isSearchCommand) {
        // Constructor specifically made for '| search' command scenario
        this.mode = CATALYST;
        this.processingPipe = stack;
        this.catCtx = catCtx;
        this.isSearchCommand = isSearchCommand;
        this.traceBuffer = new ArrayList<>(); //TODO remove traceBuffer completely?
    }

    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    public String getAggregateField() {
        return this.aggregateField;
    }


    /**
     * The main visitor function for searchTransformation, used for the main search function.
     * <pre>
     *     root : searchTransformationRoot transformStatement?
     *     searchTransformationRoot : logicalStatement
     * </pre>
     * @param ctx SearchTransformationRoot context
     * @return logicalStatement columnNode
     */
    @Override
    public Node visitSearchTransformationRoot(DPLParser.SearchTransformationRootContext ctx) {
        if (this.mode == XML) {
            // xml only mode
            LOGGER.info("searchTransformationRoot/Subsearch - generating only XML");
            ElementNode archiveQuery = (ElementNode) visitSearchTransformationRootEmitXml(ctx);
            return archiveQuery;
        }

        // xml AND spark column generation:

        // Check for index= / index!= / index IN without right side
        if (ctx.getChildCount() == 1 && ctx.getChild(0) instanceof TerminalNode) {
            TerminalNode term = (TerminalNode) ctx.getChild(0);
            if (term.getSymbol().getType() == DPLLexer.INDEX_EQ || term.getSymbol().getType() == DPLLexer.INDEX_SPACE
            || term.getSymbol().getType() == DPLLexer.INDEX_NEG || term.getSymbol().getType() == DPLLexer.INDEX_SPACE_NEG
                || term.getSymbol().getType() == DPLLexer.INDEX_IN) {
                throw new RuntimeException("The right side of the search qualifier was empty! Check that the index has" +
                        " a valid value, like 'index = voyager'.");
            }
        }

        if (!isSearchCommand) {
            // Perform this only if in main search. Don't perform in '| search' command.
            LOGGER.info("SearchTransformationRoot - xml generation");
            visitSearchTransformationRootEmitXml(ctx); /* ignore return value as it is not needed here */
        }
        else {
            LOGGER.info("SearchTransformationRoot - skipping xml generation");
        }

        LOGGER.info("SearchTransformationRoot - generating spark column");
        LOGGER.info("Within subsearch: " + isWithinASubsearchStatement(ctx));
        ColumnNode sparkColumn = (ColumnNode) visitSearchTransformationRootEmitCatalyst(ctx);
        return sparkColumn;
    }

    private Node visitSearchTransformationRootEmitXml(DPLParser.SearchTransformationRootContext ctx) {
        ElementNode archiveQuery = null;
        //Document doc = null;
        LogicalStatement xmlStatement = null;
        LOGGER.info("[SearchTransformationRoot XML] Visiting: " + ctx.getText() + ", with " + ctx.getChildCount() + " children");
        // create xml document & statement
        try {
            if (this.doc == null) {
                this.doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            }
        }
        catch (ParserConfigurationException pce) {
            throw new RuntimeException(pce);
        }

        xmlStatement = new LogicalStatement(processingPipe, catCtx,
                this.doc, traceBuffer);

        if (ctx.getChildCount() == 1) {
            // just a single directoryStatement -or- logicalStatement
            if (ctx.directoryStatement() != null) {
                DPLParser.DirectoryStatementContext dirStatCtx = ctx.directoryStatement();
                archiveQuery = (ElementNode) xmlStatement.visitDirectoryStatement(dirStatCtx);
            }
            else if (ctx.logicalStatement() != null) { //FIXME subsearch error, can't cast to elemnode->>ssnode
                DPLParser.LogicalStatementContext logiStatCtx = ctx.logicalStatement(0);
                archiveQuery = (ElementNode) xmlStatement.visitLogicalStatement(logiStatCtx);
            }
        }
        else {
            // case: directoryStmt OR logicalStmt
            ParseTree secondChild = ctx.getChild(1);
            if (secondChild instanceof TerminalNode) {
                if (((TerminalNode)secondChild).getSymbol().getType() == DPLLexer.OR) {
                    // directoryStmt OR logicalStmt
                    ElementNode dirStatArchiveQuery = null;
                    ElementNode logiStatArchiveQuery = null;
                    Element orElem = doc.createElement("OR");

                    if (ctx.directoryStatement() != null) {
                        DPLParser.DirectoryStatementContext dirStatCtx = ctx.directoryStatement();
                        dirStatArchiveQuery = (ElementNode) xmlStatement.visitDirectoryStatement(dirStatCtx);
                        orElem.appendChild(dirStatArchiveQuery.getElement());
                    }
                    else {
                        throw new RuntimeException("Invalid directory statement, query: " + ctx.getText());
                    }

                    if (ctx.logicalStatement() != null && ctx.logicalStatement().size() == 1) {
                        DPLParser.LogicalStatementContext logiStatCtx = ctx.logicalStatement(0);
                        logiStatArchiveQuery = (ElementNode) xmlStatement.visitLogicalStatement(logiStatCtx);
                        orElem.appendChild(logiStatArchiveQuery.getElement());
                    }
                    else {
                        throw new RuntimeException("Invalid logical statement, query: " + ctx.getText());
                    }

                    archiveQuery = new ElementNode(orElem);
                }
            }
            else {
                // case: (logicalStmt AND?)*? directoryStmt (AND? logicalStmt)*?
                ElementNode dirStatArchiveQuery = null;
                ElementNode logiStatArchiveQuery = null;
                Element andElem = doc.createElement("AND");

                if (ctx.directoryStatement() != null) {
                    // only one directoryStatement
                    // archive query
                    DPLParser.DirectoryStatementContext dirStatCtx = ctx.directoryStatement();
                    dirStatArchiveQuery = (ElementNode) xmlStatement.visitDirectoryStatement(dirStatCtx);
                    andElem.appendChild(dirStatArchiveQuery.getElement());
                }
                else {
                    throw new RuntimeException("Invalid directory statement, query: " + ctx.getText());
                }

                boolean firstLogicalStmt = true;
                if (ctx.logicalStatement() != null && !ctx.logicalStatement().isEmpty()) {
                    for (DPLParser.LogicalStatementContext logiStatCtx : ctx.logicalStatement()) {
                        // archive query
                        logiStatArchiveQuery = (ElementNode) xmlStatement.visitLogicalStatement(logiStatCtx);
                        if (firstLogicalStmt) {
                            andElem.appendChild(logiStatArchiveQuery.getElement());
                            firstLogicalStmt = false;
                        }
                        else {
                            Element newAndElem = doc.createElement("AND");
                            newAndElem.appendChild(andElem);
                            newAndElem.appendChild(logiStatArchiveQuery.getElement());
                            andElem = newAndElem;
                        }
                    }
                }
                else {
                    throw new RuntimeException("Invalid logical statement, query: " + ctx.getText());
                }

                archiveQuery = new ElementNode(andElem);
            }
        }


        if (archiveQuery != null) this.catCtx.setArchiveQuery(archiveQuery.toString());
        LOGGER.info("XML archive query: " + archiveQuery);

        if (this.catCtx != null && this.catCtx.getConfig() != null) {
            // Perform archive query
            if (archiveQuery != null) {
                LOGGER.info("Constructing data stream based on archive query: " + archiveQuery);
                DPLDatasource datasource = new DPLDatasource(catCtx);
                Dataset<Row> finalDataset = datasource.constructStreams(archiveQuery.toString());
                processingPipe.clear();
                processingPipe.push(finalDataset);
                LOGGER.info("Received dataset with columns: " + Arrays.toString(finalDataset.columns()));
                LOGGER.info("Stack has " + processingPipe.size() + " items after archive query");
            }
            else {
                throw new RuntimeException("Archive query was null!");
            }
        }
       else {
           // Testing mode?
            if (catCtx.getDs() != null) {
               processingPipe.clear();
               Dataset<Row> ds = catCtx.getDs();
               processingPipe.push(ds);
            }
            LOGGER.warn("CatalystContext was NULL");
       }

        return archiveQuery;
    }

    private Node visitSearchTransformationRootEmitCatalyst(DPLParser.SearchTransformationRootContext ctx) {
        ColumnNode rv = null;
        LOGGER.info("[SearchTransformationRoot CAT] Visiting: " + ctx.getText() + ", with " + ctx.getChildCount() + " children");


        if (ctx.getChildCount() == 1) {
            // just a single directoryStatement -or- logicalStatement
            if (ctx.directoryStatement() != null) {
                DPLParser.DirectoryStatementContext dirStatCtx = ctx.directoryStatement();
                rv = (ColumnNode) visit(dirStatCtx);
            }
            else if (ctx.logicalStatement() != null) { //FIXME subsearch error, can't cast to elemnode->>ssnode
                DPLParser.LogicalStatementContext logiStatCtx = ctx.logicalStatement(0);
                rv = (ColumnNode) visit(logiStatCtx);
            }
        }
        else {
            // case: directoryStmt OR logicalStmt
            ParseTree secondChild = ctx.getChild(1);
            if (secondChild instanceof TerminalNode) {
                if (((TerminalNode)secondChild).getSymbol().getType() == DPLLexer.OR) {
                    // directoryStmt OR logicalStmt
                    ColumnNode dirStatColumnNode = null;
                    ColumnNode logiStatColumnNode = null;

                    if (ctx.directoryStatement() != null) {
                        DPLParser.DirectoryStatementContext dirStatCtx = ctx.directoryStatement();
                        dirStatColumnNode = (ColumnNode) visit(dirStatCtx);
                    }
                    else {
                        throw new RuntimeException("Invalid directory statement, query: " + ctx.getText());
                    }

                    if (ctx.logicalStatement() != null && ctx.logicalStatement().size() == 1) {
                        DPLParser.LogicalStatementContext logiStatCtx = ctx.logicalStatement(0);
                        logiStatColumnNode = (ColumnNode) visit(logiStatCtx);
                    }
                    else {
                        throw new RuntimeException("Invalid logical statement, query: " + ctx.getText());
                    }

                    if (dirStatColumnNode != null && logiStatColumnNode != null) {
                        rv = new ColumnNode(dirStatColumnNode.getColumn().or(logiStatColumnNode.getColumn()));
                    }
                    else {
                        throw new RuntimeException("Error generating spark columns from two directory/logicalStatements");
                    }
                }
            }
            else {
                // case: (logicalStmt AND?)*? directoryStmt (AND? logicalStmt)*?
                Column finalColumn = null;

                if (ctx.directoryStatement() != null) {
                    // only one directoryStatement
                    DPLParser.DirectoryStatementContext dirStatCtx = ctx.directoryStatement();
                    // spark column
                    finalColumn = ((ColumnNode) visit(dirStatCtx)).getColumn();
                }
                else {
                    throw new RuntimeException("Invalid directory statement, query: " + ctx.getText());
                }

                if (ctx.logicalStatement() != null && !ctx.logicalStatement().isEmpty()) {
                    for (DPLParser.LogicalStatementContext logiStatCtx : ctx.logicalStatement()) {
                        // spark column
                        finalColumn = finalColumn.and(((ColumnNode)visit(logiStatCtx)).getColumn());
                    }
                }
                else {
                    throw new RuntimeException("Invalid logical statement, query: " + ctx.getText());
                }

                rv = new ColumnNode(finalColumn);
            }
        }

        if (rv == null) {
            throw new RuntimeException("Invalid searchTransformation provided. Please check that the query" +
                    " contains at least one field with a valid value. Example: index=abc. Query given: " +
                    ctx.getText() + "\nParse tree:\n" + getParseTreeString(ctx));
        }


        if (rv.getColumn() != null) this.catCtx.setSparkQuery(rv.getColumn().toString());
        LOGGER.info("Spark column: " + rv.getColumn().toString());

        if (!processingPipe.isEmpty()) {
            Dataset<Row> dataframe = processingPipe.pop();
            Dataset<Row> finalDataframe = dataframe.where((rv).getColumn());
            processingPipe.push(finalDataframe);
        }
        else if (ds != null) {
            Dataset<Row> finalDataframe = ds.where((rv).getColumn());
            processingPipe.push(finalDataframe);
        }
        else {
            throw new RuntimeException("Stack was empty and provided dataset was null.");
        }

        return rv;
    }

    /**
     * Prints parse tree string, IF catCtx has been setRuleNames(parser.getRuleNames())
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
            return (TreeUtils.toPrettyTree(curr.getChild(0), Arrays.asList(catCtx.getRuleNames())));
        }
        return "<<Rulenames not provided to the context, cannot print parse tree>>";
    }

    /**
     * Checks if current ParserRuleContext is a child of SubsearchStatement
     * @param ctx parserRuleContext
     * @return boolean if it is a child or not
     */
    private boolean isWithinASubsearchStatement(ParserRuleContext ctx) {
        ParserRuleContext currentContext = ctx;

        while (currentContext != null) {
            if (currentContext instanceof DPLParser.SubsearchStatementContext) {
                return true;
            }

            currentContext = currentContext.getParent();
        }

        return false;
    }

    @Override
    public Node visitDirectoryStatement(DPLParser.DirectoryStatementContext ctx) {
        Node rv = null;
        Element el = null;
        List<Node> listOfElements = new ArrayList<>();
        boolean orMode = false;

        if (ctx.getChildCount() == 1) {
            // subindexStatement / indexStatement
            if (ctx.indexStatement() != null) {
                LOGGER.info("[DirStmt] Visiting indexStatement: " + ctx.indexStatement().getText());
                DPLParser.IndexStatementContext indStatCtx = ctx.indexStatement();

                rv = visit(indStatCtx);
            }
            else if (ctx.subindexStatement() != null) {
                // ( directoryStatement )
                // skip parenthesis
                LOGGER.info("[SubIndexStmt] Visiting subIndexStatement: " + ctx.subindexStatement().getText());
                DPLParser.DirectoryStatementContext dirStatCtx =
                        (DPLParser.DirectoryStatementContext) ctx.subindexStatement().getChild(1);
                rv = visit(dirStatCtx);

                LOGGER.debug("sub index = " + rv.toString());
            }
        }
        else {
            LOGGER.info("[DirStmt] Multiple children detected: " + ctx.getChildCount() + " children");
            // directoryStmt OR directoryStmt
            // directoryStmt (AND*? logicalStmt)+
            // (logicalStmt AND? )+ directoryStmt

            ParseTree secondChild = ctx.getChild(1);

            if (!(secondChild instanceof DPLParser.LogicalStatementContext ||
            secondChild instanceof DPLParser.DirectoryStatementContext)) {
                if (((TerminalNode) secondChild).getSymbol().getType() == DPLLexer.OR) {
                    // must be dirStmt OR dirStmt
                    LOGGER.info("[DirStmt] OR detected");
                    orMode = true;
                }
            }


            if (ctx.directoryStatement() != null) {
                LOGGER.info("[DirStmt] DirectoryStatement found within: " + ctx.directoryStatement().stream().map(RuleContext::getText).collect(Collectors.joining(",")));
                List<DPLParser.DirectoryStatementContext> listOfDirStmtCtx = ctx.directoryStatement();

                for (DPLParser.DirectoryStatementContext dirStmtCtx : listOfDirStmtCtx) {
                    rv = visit(dirStmtCtx);
                    listOfElements.add(rv);
                }
            }

            if (ctx.logicalStatement() != null) {
                LOGGER.info("[DirStmt] LogicalStatement found within: " + ctx.logicalStatement().stream().map(RuleContext::getText).collect(Collectors.joining(",")));
                List<DPLParser.LogicalStatementContext> listOfLogicalStmtCtx = ctx.logicalStatement();

                for (DPLParser.LogicalStatementContext logicalStmtCtx : listOfLogicalStmtCtx) {
                    rv = visit(logicalStmtCtx);
                    listOfElements.add(rv);
                }
            }
        }

        // filter nulls out
        listOfElements = listOfElements.stream().filter(Objects::nonNull).collect(Collectors.toList());

        // different return types based on if it's XML or CATALYST mode
        // This is required to bundle multiple statements into one
        if (this.mode == XML && ctx.getChildCount() > 1) {
            el = doc.createElement(orMode ? "OR" : "AND");
            for (Node n : listOfElements) {
                if (n == null) {
                    // On null, skip
                    continue;
                }
                if (n instanceof SubSearchNode) {
                    SubSearchNode ssn = (SubSearchNode)n;
                    el.appendChild(ssn.asElement(doc));
                }else {
                    Element e = ((ElementNode)n).getElement();
                    el.appendChild(e);
                }

            }

            if (listOfElements.size() == 1) {
                el = (Element) el.getFirstChild();
            }

            // if no elements, rv will remain null
            if (!listOfElements.isEmpty()) {
                rv = new ElementNode(el);
            }

        }
        else if (this.mode == CATALYST && ctx.getChildCount() > 1) {
            Column ret = null;
            Dataset<Row> subSearchDs = null;
            for (Node n : listOfElements) {
                Column c = null;

                if (n instanceof SubSearchNode) {
                    c = ((SubSearchNode)n).getColumn();
                }
                else if (n instanceof ColumnNode) {
                    c = ((ColumnNode)n).getColumn();
                }
                else if (n instanceof CatalystNode) {
                    LOGGER.info("Got dataset from sub search");
                    subSearchDs = ((CatalystNode)n).getDataset();
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

            // Filter main search's _raw column based on subSearch
            if (subSearchDs != null) {
                if (processingPipe.isEmpty()) {
                    // No main search data, only subsearch
                    throw new IllegalStateException("No main search data in stack after subsearch! This is most likely caused " +
                            "by a faulty implementation of the DPL translation layer.");
                }
                else {
                    // Main search data in stack, subsearch in ds variable
                    LOGGER.info("Main search data in stack, subsearch visit done w/ data");

                    Dataset<Row> mainSearchDs = processingPipe.pop();

                    final String randomID = UUID.randomUUID().toString();
                    final String queryName = "subsearch-" + randomID;
                    final String hdfsPath = processingPipe.getCatVisitor().getHdfsPath();
                    final String cpPath = hdfsPath + "checkpoint/sub/" + randomID;
                    final String path = hdfsPath + "data/sub/" + randomID;
                    DataStreamWriter<Row> subToDiskWriter =
                            subSearchDs
                                    .repartition(1)
                                    .writeStream()
                                    .format("avro")
                                    .trigger(Trigger.ProcessingTime(0))
                                    .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                                    .option("checkpointLocation", cpPath)
                                    .option("path", path)
                                    .outputMode(OutputMode.Append());

                    SparkSession ss = SparkSession.builder().getOrCreate();

                    StreamingQuery subToDiskQuery = this.catCtx.getInternalStreamingQueryListener().registerQuery(queryName, subToDiskWriter);

                try {
                    // await for listener to stop the subToDiskQuery
                    subToDiskQuery.awaitTermination();
                } catch (StreamingQueryException e) {
                    throw new RuntimeException(e);
                }

                  // read subsearch data from disk and collect
                  Dataset<Row> readFromDisk = ss.read().schema(subSearchDs.schema()).format("avro").load(path);
                  List<Row> collected = readFromDisk.collectAsList();

                  // generate filtering column
                  Column filterColumn = null;
                  for (Row collectedRow : collected) {
                      for (int i = 0; i < collectedRow.length(); i++) {
                          String rowContent = collectedRow.get(i).toString();
                          if (filterColumn != null) {
                              filterColumn = filterColumn.or(functions.col("_raw").rlike("(?i)^.*" + Pattern.quote(rowContent) + ".*$"));
                          }
                          else {
                              filterColumn = functions.col("_raw").rlike("(?i)^.*" + Pattern.quote(rowContent) + ".*$");
                          }
                      }
                  }


                    if (filterColumn == null) {
                       throw new IllegalStateException("Generated filter column via subsearch was null!");
                    }
                    else {
                        LOGGER.info("Filter column: '" + filterColumn + "'");
                    }

                    // push filtered main search to stack
                    processingPipe.push(mainSearchDs.where(filterColumn));
                }
            }

            LOGGER.info("Stack size after directoryStatement: " + processingPipe.size());
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
        //LOGGER.info("Logical statement: " + ctx.getText());
        Node rv = null;
        LOGGER.info("logicalStatement incoming:" + ctx.getText() + " Mode:" + mode);
        switch (mode) {
            case XML: { // XML
                rv = logicalStatementEmitXml(ctx);
                break;
            }
            case CATALYST: { // Catalyst
                rv = logicalStatementEmitCatalyst(ctx);
                if (rv instanceof SubSearchNode) {
                    LOGGER.info("visit logicalStatement Return value is subsearch query=:" + ((SubSearchNode) rv).asExpression());
                }
//               traceBuffer.add("visitLogicalStatement(Catalyst) rv:" + rv);
                break;
            }
        }
        LOGGER.info("visitLogicalStatement outgoing:" + rv);
        return rv;
    }

    private Node logicalStatementEmitXml(DPLParser.LogicalStatementContext ctx) {
        Node rv = null;
        Node left = null;
        TerminalNode leftIsTerminal = null;

        //LOGGER.info("LogicalStatement XML emit: " + ctx.getText() + " ;; child count: " + ctx.getChildCount());

        // Visit leftmost child if it is not a terminal node
        if (!(ctx.getChild(0) instanceof TerminalNode)) {
            left = visit(ctx.getChild(0));
        } else {
            // TerminalNode can only be "NOT"
            leftIsTerminal = (TerminalNode) ctx.getChild(0);
            if (leftIsTerminal.getSymbol().getType() != DPLLexer.NOT) {
                throw new RuntimeException("Unsupported unary logical operation: " + ctx.getText());
            }
        }

        // If only one child, return the leftmost child
        if (ctx.getChildCount() == 1) {
            // leaf
            rv = left;
        } else if (ctx.getChildCount() == 2) {
            // Two children, visit rightmost child
            Node right = visit(ctx.getChild(1));
            Element el;

            if (leftIsTerminal != null) {
                // Should be NOT
                el = doc.createElement(leftIsTerminal.getText().toUpperCase());
            } else {
                // Add missing AND between elements
                el = doc.createElement("AND");
            }

            if (left instanceof ElementNode) {
                el.appendChild(((ElementNode) left).getElement());
            }
            if (right instanceof ElementNode) {
                el.appendChild(((ElementNode) right).getElement());
            }

            rv = new ElementNode(el);

        } else if (ctx.getChildCount() == 3) {
            // Three children; logicalStmt AND/OR logicalStmt
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Node right = visit(ctx.getChild(2));

            Element el = doc.createElement(operation.getText().toUpperCase());

            el.appendChild(((ElementNode) left).getElement());
            el.appendChild(((ElementNode) right).getElement());

            rv = new ElementNode(el);
        }
        //LOGGER.info("Resulting element: " + elementAsString(((ElementNode)rv).getElement()));

        if (rv instanceof SubSearchNode) {
            LOGGER.info("[XML] [LogiStat] Return value was SubsearchNode. Converting to ElementNode!");
            return new ElementNode(((SubSearchNode)rv).asElement(doc));
        }
        return rv;
    }

    private Node logicalStatementEmitCatalyst(DPLParser.LogicalStatementContext ctx) {
        Node rv = null;
        Node left = null;
        TerminalNode leftIsTerminal = null;

        if (!(ctx.getChild(0) instanceof TerminalNode)) {
            left = visit(ctx.getChild(0));
        } else {
            leftIsTerminal = (TerminalNode) ctx.getChild(0);
            if (leftIsTerminal.getSymbol().getType() != DPLLexer.NOT) {
                throw new RuntimeException("Unsupported unary logical operation: " + ctx.getText());
            }
        }

        if (ctx.getChildCount() == 1) {
            // leaf
            rv = left;
        } else if (ctx.getChildCount() == 2) {
            Node right = visit(ctx.getChild(1));
            if (leftIsTerminal != null) {
                Column r = ((ColumnNode) right).getColumn();
                // Use unary operation, currently only NOT is supported
                rv = new ColumnNode(functions.not(r));
            } else {
                if (left instanceof ColumnNode && right instanceof ColumnNode) {
                    Column l = ((ColumnNode) left).getColumn();
                    Column r = ((ColumnNode) right).getColumn();
                    // Add missing AND between elements
                    rv = new ColumnNode(l.and(r));
                }
            }

        } else if (ctx.getChildCount() == 3) {
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Node right = visit(ctx.getChild(2));
            if (left instanceof ColumnNode && right instanceof ColumnNode) {
                Column l = ((ColumnNode) left).getColumn();
                Column r = ((ColumnNode) right).getColumn();
                // resolve operation
                if (DPLLexer.AND == operation.getSymbol().getType()) {
                    rv = new ColumnNode(l.and(r));
                } else if (DPLLexer.OR == operation.getSymbol().getType())
                    rv = new ColumnNode(l.or(r));
                else {
                    throw new RuntimeException("Unsupported logical operation:" + operation);
                }
            }
        }

        if (rv instanceof SubSearchNode) {
            LOGGER.info("[CAT] [LogiStat] Return value was SubsearchNode. Converting to ColumnNode!");
            rv = new ColumnNode(((SubSearchNode)rv).getColumn());
        }

        return rv;
    }

    @Override
    public Node visitSearchIndexStatement(DPLParser.SearchIndexStatementContext ctx) {
        Node rv = null;
        switch (this.mode) {
            case XML: {
                rv = searchIndexStatementEmitXml(ctx);
                break;
            }
            case CATALYST: {
                rv = searchIndexStatementEmitCatalyst(ctx);
            }
        }
        return rv;
    }

    private Node searchIndexStatementEmitXml(DPLParser.SearchIndexStatementContext ctx) {
        if (ctx.stringType() != null) {
            String statement = ctx.stringType().getText();
            String operation = new Token(Token.Type.EQUALS).toString();
            Element el = doc.createElement("indexstatement");

            el.setAttribute("operation", operation);
            el.setAttribute("value", stripQuotes(statement));

            LOGGER.info("[XML] SearchIndexStatement return: operation= " + operation + ", value= " + statement);
            return new ElementNode(el);
        }
        throw new RuntimeException("[XML] SearchIndexStatement did not contain a string type");
    }

    private Node searchIndexStatementEmitCatalyst(DPLParser.SearchIndexStatementContext ctx) {
        Column rv = new Column("_raw");
        if (ctx.stringType() != null) {
            String statement = ctx.stringType().getText();
            LOGGER.info("Got a statement : " + statement);

            statement = StringEscapeUtils.unescapeJava(statement); // unescape any escaped chars
            LOGGER.info("Unescaped : " + statement);

            statement = stripQuotes(statement); // strip outer quotes
            LOGGER.info("Stripped quotes : " + statement);

            statement = "(?i)^.*".concat(Pattern.quote(statement)).concat(".*"); // prefix with case insensitivity
            LOGGER.info("Final : " + statement);

            rv = rv.rlike(statement);
        }

        LOGGER.info("[CAT] SearchIndexStatement return: " + rv.toString());
        return new ColumnNode(rv);
    }

    /**
     * sublogicalStatement : PARENTHESIS_L logicalStatement PARENTHESIS_R ;
     */
    @Override
    public Node visitSublogicalStatement(DPLParser.SublogicalStatementContext ctx) {
        Node n = null;
        switch (mode) {
            case XML: { // XML
                n = sublogicalStatementEmitXML(ctx);
                traceBuffer.add("subLogicalStatement return:" + n.toString());
                break;
            }
            case CATALYST: { // Catalyst
                n = sublogicalStatementEmitCatalyst(ctx);
                traceBuffer.add("subLogicalStatement return:" + n.toString());
                break;
            }
        }
        return n;
    }

    private Node sublogicalStatementEmitXML(DPLParser.SublogicalStatementContext ctx) {
        // Consume parenthesis
        return visit(ctx.getChild(1));
    }

    private Node sublogicalStatementEmitCatalyst(DPLParser.SublogicalStatementContext ctx) {
        // Consume parenthesis
        return visit(ctx.getChild(1));
    }

    /**
     * searchQualifier : INDEX (EQ|NEQ) stringType WILDCARD? | SOURCETYPE (EQ|NEQ)
     * stringType WILDCARD? | HOST (EQ|NEQ) stringType WILDCARD? | SOURCE (EQ|NEQ)
     * stringType WILDCARD? | SAVEDSEARCH (EQ|NEQ) stringType WILDCARD? | EVENTTYPE
     * (EQ|NEQ) stringType WILDCARD? | EVENTTYPETAG (EQ|NEQ) stringType WILDCARD? |
     * HOSTTAG (EQ|NEQ) stringType WILDCARD? | TAG (EQ|NEQ) stringType WILDCARD? ;
     */
    //@Override
   public Node visitSearchQualifier(SearchQualifierContext ctx) {
        //LOGGER.info("Search qualifier: " + ctx.getText());
        Node n = null;
        switch (mode) {
            case XML: { // XML
                n = searchQualifierEmitXml(ctx);
                traceBuffer.add("visitSearchQualifier(xml) return:" + n);
                break;
            }
            case CATALYST: { // Catalyst
                n = searchQualifierEmitCatalyst(ctx);
                traceBuffer.add("visitSearchQualifier(catalyst) return:" + n);
                break;
            }
        }
        return n;
    }

  private ElementNode searchQualifierEmitXml(DPLParser.SearchQualifierContext ctx) {
        // LOGGER.info("visitSearchQualifier: ");
        Token comparisonToken;
        Element el = null;
        String value = null;
        List<String> listOfIndices = new ArrayList<>();

        String qualifier;
        TerminalNode left = (TerminalNode) ctx.getChild(0);
        TerminalNode operation = (TerminalNode) ctx.getChild(1);

        // check whether operation is '=' or '!='
        if (operation.getSymbol().getType() == DPLLexer.EQ) {
            comparisonToken = new Token(Type.EQUALS);
        } else {
            comparisonToken = new Token(Type.NOT_EQUALS);
        }

        // Default clause used in WHERE-part
        qualifier = left.getText() + " " + operation.getSymbol().getText() + " ";

        // HOST and SOURCETYPE qualifier stored as additional list and used for Kafka content filtering
        if (left.getSymbol().getType() == DPLLexer.INDEX_IN) {
            value = "";
            ctx.indexStringType().forEach(str -> {
                listOfIndices.add(stripQuotes(str.getText().toLowerCase()));
            });
            comparisonToken = new Token(Type.EQUALS); // INDEX IN is equals
            el = doc.createElement("index");
        }
        else if (left.getSymbol().getType() == DPLLexer.HOST) {
            value = stripQuotes(ctx.getChild(2).getText().toLowerCase());
            el = doc.createElement("host");
        }
        else if (left.getSymbol().getType() == DPLLexer.SOURCETYPE) {
            value = stripQuotes(ctx.getChild(2).getText().toLowerCase());
            el = doc.createElement("sourcetype");
            LOGGER.info("qualifier:" + qualifier + " sourcetype=" + value);
        }
        else {
            // other column=value qualifier
            value = stripQuotes(ctx.getChild(2).getText().toLowerCase());
            el = doc.createElement(ctx.getChild(0).getText().toLowerCase());
            LOGGER.info("custom qualifier: " + ctx.getChild(0).getText() + " = " + ctx.getChild(2).getText());
        }

        if (listOfIndices.isEmpty()) {
            el.setAttribute("operation", comparisonToken.toString());
            el.setAttribute("value", value);
        }
        else {
            // build xml string for index IN ( 1 2 )
            el = doc.createElement("OR");

            for (int i = 0; i < listOfIndices.size(); i++) {
                Element indexElem = doc.createElement("index");
                indexElem.setAttribute("operation", comparisonToken.toString());
                indexElem.setAttribute("value", listOfIndices.get(i));

                if (listOfIndices.size() == 1) {
                    el = indexElem;
                    break;
                }
                else if (i < 2) {
                    el.appendChild(indexElem);
                }
                else {
                    Element outerOR = doc.createElement("OR");
                    outerOR.appendChild(el);
                    outerOR.appendChild(indexElem);
                    el = outerOR;
                }


            }
        }

        return new ElementNode(el);
    }

  private ColumnNode searchQualifierEmitCatalyst(DPLParser.SearchQualifierContext ctx) {
        // LOGGER.info("visitSearchQualifier: ");
        Column sQualifier = null;
        String value = null;

        TerminalNode left = (TerminalNode) ctx.getChild(0);
        TerminalNode operation = (TerminalNode) ctx.getChild(1);

        List<String> listOfIndices = new ArrayList<>();

        // Default clause used in WHERE-part
        String columnName = null;
        // HOST and SOURCETYPE qualifier stored as additional list and used for Kafka content filtering
        if (left.getSymbol().getType() == DPLLexer.INDEX_IN) {
            value = "";
            ctx.indexStringType().forEach(str -> {
                listOfIndices.add(stripQuotes(str.getText().toLowerCase()));
            });
            columnName = "index";
        }
        else if (left.getSymbol().getType() == DPLLexer.HOST) {
            value = stripQuotes(ctx.getChild(2).getText().toLowerCase());
            columnName = "host";
        }
        else if (left.getSymbol().getType() == DPLLexer.SOURCETYPE) {
            value = stripQuotes(ctx.getChild(2).getText());// don't force cases .toLowerCase());
            columnName = "sourcetype";
        }
        else {
            // other column=value qualifier
            value = stripQuotes(ctx.getChild(2).getText());
            columnName = ctx.getChild(0).getText();
        }

        Column col = new Column(columnName);

        // check whether operation is '=' or '!='
        if (left.getSymbol().getType() != DPLLexer.INDEX_IN && operation.getSymbol().getType() == DPLLexer.EQ) {
            // Use like instead of '=' so that jokers work
            String rlikeStatement = glob2rlike(value);
            sQualifier = col.rlike(rlikeStatement);

        } else if (left.getSymbol().getType() == DPLLexer.INDEX_IN) {
            for (String index : listOfIndices) {
                String rlikeStatement = glob2rlike(index);
                if (sQualifier == null) {
                    sQualifier = col.rlike(rlikeStatement);
                }
                else {
                    sQualifier = sQualifier.or(col.rlike(rlikeStatement));
                }

            }
        } else {
            String rlikeStatement = glob2rlike(value);
            sQualifier = functions.not(col.rlike(rlikeStatement));
        }

        traceBuffer.add("SearchQualifier(catalyst):" + sQualifier);
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
        //LOGGER.info("Index statement: " + ctx.getText());
        Node rv = null;
        switch (mode) {
            case XML: { // XML
                rv = indexStatementEmitXml(ctx);
                traceBuffer.add("visitIndexStatement(xml) return:" + rv);
                break;
            }
            case CATALYST: { // Catalyst
                rv = indexStatementEmitCatalyst(ctx);
                traceBuffer.add("visitIndexStatement(catalyst) return:" + rv);
                break;
            }
        }
        LOGGER.debug("Got index statement: " + rv.toString());
        return rv;
    }

    /**
     * stringType WILDCARD? | WILDCARD | termStatement | caseStatement
     *
     * @return StringNode,
     */

    private ElementNode indexStatementEmitXml(DPLParser.IndexStatementContext ctx) {
        TerminalNode index = (TerminalNode) ctx.getChild(0);
        String value = stripQuotes(ctx.getChild(1).getText().toLowerCase());
        Token comparisonToken;
        Element el = null;

        switch (index.getSymbol().getType()) {
            case DPLLexer.INDEX_EQ:
            case DPLLexer.INDEX_SPACE: {
                // index=... OR index = ...
                comparisonToken = new Token(Type.EQUALS);
                break;
            }
            case DPLLexer.INDEX_NEG:
            case DPLLexer.INDEX_SPACE_NEG: {
                // index!=...
                comparisonToken = new Token(Type.NOT_EQUALS);
                break;
            }
            default: {
                throw new UnsupportedOperationException("Invalid index statement: " + ctx.getText());
            }
        }

        // index = ...
        el = doc.createElement("index");
        el.setAttribute("operation", comparisonToken.toString());
        el.setAttribute("value", value);

        return new ElementNode(el);
    }

    private Node indexStatementEmitCatalyst(DPLParser.IndexStatementContext ctx) {
        TerminalNode index = (TerminalNode) ctx.getChild(0);
        String value = stripQuotes(ctx.getChild(1).getText());

        Column indexCol = null;
        switch (index.getSymbol().getType()) {
            case DPLLexer.INDEX_EQ:
            case DPLLexer.INDEX_SPACE:
                indexCol = new Column("index").rlike("(?i)^".concat(value));
                break;
            case DPLLexer.INDEX_NEG:
            case DPLLexer.INDEX_SPACE_NEG:
                indexCol = functions.not(new Column("index").rlike("(?i)^".concat(value)));
                break;
            default:
                throw new UnsupportedOperationException("Index type not supported");
        }

        return new ColumnNode(indexCol);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     */
    @Override
    public Node visitComparisonStatement(DPLParser.ComparisonStatementContext ctx) {
        Node rv = null;
        switch (mode) {
            case XML: { // XML
                rv = comparisonStatementEmitXml(ctx);
                traceBuffer.add("visitIndexStatement(xml) return:" + rv);
                break;
            }
            case CATALYST: { // Catalyst
                rv = comparisonStatementEmitCatalyst(ctx);
                traceBuffer.add("visitIndexStatement(catalyst) return:" + rv);
                break;
            }
        }
        return rv;
    }

    private ElementNode comparisonStatementEmitXml(DPLParser.ComparisonStatementContext ctx) {
        String value = ctx.getChild(1).getText();
        String field = ctx.getChild(0).getText();
        String operation = "";

        boolean specialCase = false;
        if (ctx.getChild(0) instanceof TerminalNode) {
            TerminalNode specialLefthandSide = (TerminalNode) ctx.getChild(0);
            switch (specialLefthandSide.getSymbol().getType()) {
                case DPLLexer.INDEX_EQ:
                    field = "index";
                    operation = "=";
                    specialCase = true;
                    break;
            }
        }

        LOGGER.info("[XML] Comparison statement: " + value + " with " + ctx.getChildCount() + " children.");
        traceBuffer.add("--ComparisonStatementEmit(xml): childCount" + ctx.getChildCount() + " Statement:" + value);
        Element el = doc.createElement("comparisonstatement");
        el.setAttribute("field", field);

        if (specialCase) {
            el.setAttribute("operation", operation);
            el.setAttribute("value", stripQuotes(value));
        }
        else {
            el.setAttribute("operation", ctx.getChild(1).getText());
            el.setAttribute("value", stripQuotes(ctx.getChild(2).getText()));
        }

        return new ElementNode(el);
    }


    private ColumnNode comparisonStatementEmitCatalyst(DPLParser.ComparisonStatementContext ctx) {
        Column col = null;
        Column rv = null;

        String value = Util.stripQuotes(ctx.getChild(1).getText());
        String field = Util.stripQuotes(ctx.getChild(0).getText());

        LOGGER.info("[CAT] [ComparisonStmt] " + field + " = " + value);

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
            rv = this.addOperation(col, (TerminalNode) ctx.getChild(1), Util.stripQuotes(ctx.getChild(2).getText()));
        }

        traceBuffer.add("comparisonStatementEmit(Catalyst) rv:" + rv.expr().sql());
        return new ColumnNode(rv);
    }


    private Column addOperation(Column source, TerminalNode operation, String value) {
        Column rv = null;
        traceBuffer.add("AddOperation:");
        switch (operation.getSymbol().getType()) {
            case DPLLexer.EQ:
            case DPLLexer.EVAL_LANGUAGE_MODE_EQ: {
                //rv = source.equalTo(value);
                rv = source.rlike(GlobToRegEx.regexify(value)); //wildcard support
                break;
            }
            case DPLLexer.NEQ:
            case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                //rv = source.notEqual(value);
                rv = functions.not(source.rlike(GlobToRegEx.regexify(value))); //wildcard support
                break;
            }
            case DPLLexer.GT:
            case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                rv = source.gt(value);
                break;
            }
            case DPLLexer.GTE:
            case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                rv = source.geq(value);
                break;
            }
            case DPLLexer.LT:
            case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                rv = source.lt(value);
                break;
            }
            case DPLLexer.EVAL_LANGUAGE_MODE_LTE:
            case DPLLexer.LTE: {
                rv = source.leq(value);
                break;
            }
            default: {
                traceBuffer.add("Unknown operation:" + operation.getSymbol().getType());
            }
        }
        return rv;
    }

    @Override
    public Node visitSubsearchStatement(DPLParser.SubsearchStatementContext ctx) {
        Node rv = null;
        String key = ctx.getText();
        // Check whether subsearch exist already in symbol-table

        switch (mode) {
            case XML: {
                break;
            }
            case CATALYST: { // Catalyst
                rv = SubsearchStatementEmitCatalyst(ctx);
                traceBuffer.add("visitSubSearchStatement(catalyst) return:" + rv);
                break;
            }
        }
        return rv;
    }

    private Node SubsearchStatementEmitCatalyst(DPLParser.SubsearchStatementContext ctx) {
        LOGGER.info("visitSubsearchStatement with brackets:" + ctx.getText());
        // Strip brackets around statement
        DPLParserCatalystContext subCtx = null;
        if (catCtx != null) {
            LOGGER.info("Cloning main visitor to subsearch");
            subCtx = catCtx.clone();
            if (!processingPipe.isEmpty() && subCtx.getDs() == null) {
                LOGGER.info("Stack in main visitor not empty - settings as dataset in subVisitor");
                Dataset<Row> mainDs = processingPipe.pop();
                subCtx.setDs(mainDs);
                processingPipe.push(mainDs);
            }
            else {
                LOGGER.warn("Stack in main visitor was EMPTY");
            }

        }
        LOGGER.info("subVisitor init with subCtx=" + subCtx + " Mode=" + mode);
        DPLParserCatalystVisitor subVisitor = new DPLParserCatalystVisitor(subCtx);
        LOGGER.info("Subvisitor has stack with " + subVisitor.getStack().size() + " datasets");
        ParseTree tree = ctx.getParent().getChild(0);
        String prettyTree = null;
        if (catCtx.getRuleNames() != null) {
            List<String> ruleNamesList = Arrays.asList(catCtx.getRuleNames());
            prettyTree = TreeUtils.toPrettyTree(tree, ruleNamesList);
        }

        // Pass actual subsearch branch
        LOGGER.info(" ----- call using " + ctx.subsearchTransformStatement().getText());
        Node rv = subVisitor.visit(ctx.subsearchTransformStatement());
        LOGGER.info("SubSearchTransformation Result:" + rv.getClass().getName() + " Mode=" + mode);

        rv = new CatalystNode(subVisitor.getStack().pop());
        return rv;
    }


    @Override
    public Node visitSubsearchTransformStatement(DPLParser.SubsearchTransformStatementContext ctx) {
        LOGGER.info("visitSubsearchTransformStatement:" + ctx.getText());
        int count = ctx.getChildCount();
        for (int i = 0; i < count; i++) {
            visit(ctx.getChild(i));
        }
        return visitSearchTransformation(ctx.searchTransformation());
    }

    // Time format handling
    @Override
    public Node visitTimeStatement(DPLParser.TimeStatementContext ctx) {
        TimeStatement timeStatement = null;
        switch (mode) {
            case XML: {
                traceBuffer.add("visitTimeStatement init(XML)");
                timeStatement = new TimeStatement(doc, timeFormatStack);
                break;
            }
            case CATALYST: {
                traceBuffer.add("visitTimeStatement init(Catalyst)");
                timeStatement = new TimeStatement(timeFormatStack);
                break;
            }
        }

        Node rv = timeStatement.visitTimeStatement(ctx);

        // update min earliest and max latest
        if (timeStatement.getStartTime() != null) {
            LOGGER.info("TimeStatement: Set minimum (earliest) time to: " + timeStatement.getStartTime());
            this.catCtx.setDplMinimumEarliest(timeStatement.getStartTime());
        }

        if (timeStatement.getEndTime() != null) {
            LOGGER.info("TimeStatement: Set maximum (latest) time to: " + timeStatement.getEndTime());
            this.catCtx.setDplMaximumLatest(timeStatement.getEndTime());
        }

        return rv;
    }

    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        return evalStatement.visitL_evalStatement_evalCompareStatement(ctx);
    }

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        traceBuffer.add("Visit fieldtype:" + ctx.getChild(0).getText());
        // Check if symbol-table has it
        String sql = ctx.getChild(0).getText();
        if (symbolTable != null && symbolTable.containsKey(sql)) {
            sql = (String)symbolTable.get(sql);
        }
        traceBuffer.add("return fieldtype:" + sql);
        return new StringNode(new Token(Type.STRING, sql));
    }

    /**
     * subEvalLogicalStatement : PARENTHESIS_L subEvalLogicalStatement PARENTHESIS_R
     * ;
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

    public Node visitL_evalStatement_evalCalculateStatement_multipliers(DPLParser.L_evalStatement_evalCalculateStatement_multipliersContext ctx) {
        return evalStatement.visitL_evalStatement_evalCalculateStatement_multipliers(ctx);
    }

    public Node visitL_evalStatement_evalCalculateStatement_minus_plus(DPLParser.L_evalStatement_evalCalculateStatement_minus_plusContext ctx) {
        return evalStatement.visitL_evalStatement_evalCalculateStatement_minus_plus(ctx);
    }

    public Node visitL_evalStatement_evalConcatenateStatement(DPLParser.L_evalStatement_evalConcatenateStatementContext ctx) {
        return evalStatement.visitL_evalStatement_evalConcatenateStatement(ctx);
    }

    public Node visitFieldsTransformation(DPLParser.FieldsTransformationContext ctx) {
        return fieldsTransformation.visitFieldsTransformation(ctx);
    }
}
