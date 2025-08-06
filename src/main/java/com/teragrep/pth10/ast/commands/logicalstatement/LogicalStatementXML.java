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
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.datasources.ArchiveQuery;
import com.teragrep.pth10.steps.logicalXML.LogicalXMLStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.*;
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
public class LogicalStatementXML extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalStatementXML.class);
    private final DPLParserCatalystContext catCtx;
    private final boolean isMetadataQuery;
    Document doc;

    public LogicalStatementXML(DPLParserCatalystContext catCtx, Document doc) {
        this.doc = doc;
        this.catCtx = catCtx;
        this.isMetadataQuery = false;
    }

    public LogicalStatementXML(DPLParserCatalystContext catCtx, Document doc, boolean isMetadataQuery) {
        this.catCtx = catCtx;
        this.isMetadataQuery = isMetadataQuery;
        this.doc = doc;
    }

    /**
     * Visits the parse tree for SearchTransformationRoot and returns a LogicalXMLStep that can be added to Steptree in
     * DPLParserCatalystVisitor.
     * 
     * @param ctx SearchTransformationRootContext
     * @return LogicalXMLStep
     */
    public AbstractStep visitLogicalStatementXML(DPLParser.SearchTransformationRootContext ctx) {
        if (ctx != null) {
            Node ret = visitSearchTransformationRoot(ctx);
            if (ret != null) {
                return new LogicalXMLStep(new ArchiveQuery(ret.toString()), this.catCtx, isMetadataQuery);
            }
        }
        return new LogicalXMLStep(new ArchiveQuery(), this.catCtx, isMetadataQuery);
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
        ElementNode archiveQuery;
        LOGGER
                .info(
                        "[SearchTransformationRoot XML] Visiting: <{}> with <{}> children", ctx.getText(),
                        ctx.getChildCount()
                );

        if (ctx.getChildCount() == 1) {
            // just a single directoryStatement -or- logicalStatement
            archiveQuery = (ElementNode) visit(ctx.getChild(0));
        }
        else {
            ParseTree secondChild = ctx.getChild(1);
            if (
                secondChild instanceof TerminalNode && ((TerminalNode) secondChild).getSymbol().getType() == DPLLexer.OR
            ) {
                // case: directoryStmt OR logicalStmt
                ElementNode dirStatArchiveQuery = (ElementNode) visit(ctx.directoryStatement());
                ElementNode logiStatArchiveQuery = (ElementNode) visit(ctx.logicalStatement(0));

                Element orElem = doc.createElement("OR");
                orElem.appendChild(dirStatArchiveQuery.getElement());
                orElem.appendChild(logiStatArchiveQuery.getElement());

                archiveQuery = new ElementNode(orElem);
            }
            else {
                // case: (logicalStmt AND?)*? directoryStmt (AND? logicalStmt)*?
                Element andElem = doc.createElement("AND");

                ElementNode dirStatArchiveQuery = (ElementNode) visit(ctx.directoryStatement());
                andElem.appendChild(dirStatArchiveQuery.getElement());

                boolean firstLogicalStmt = true;
                for (DPLParser.LogicalStatementContext logiStatCtx : ctx.logicalStatement()) {
                    ElementNode logiStatArchiveQuery = (ElementNode) visitLogicalStatement(logiStatCtx);
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

                archiveQuery = new ElementNode(andElem);
            }
        }

        if (archiveQuery != null)
            this.catCtx.setArchiveQuery(archiveQuery.toString());
        LOGGER.info("XML archive query: <{}>", archiveQuery);

        return archiveQuery;
    }

    @Override
    public Node visitDirectoryStatement(DPLParser.DirectoryStatementContext ctx) {
        Node rv = null;
        Element el = null;
        List<Node> listOfElements = new ArrayList<>();
        boolean orMode = false;

        if (ctx.getChildCount() == 1) {
            // subindexStatement / indexStatement
            LOGGER.debug("[DirStmt] Only one child. Visiting: <{}>", ctx.getChild(0).getClass());
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
            el = doc.createElement(orMode ? "OR" : "AND");
            for (Node n : listOfElements) {
                if (n == null) {
                    // On null, skip
                    continue;
                }
                if (n instanceof SubSearchNode) {
                    SubSearchNode ssn = (SubSearchNode) n;
                    el.appendChild(ssn.asElement(doc));
                }
                else {
                    Element e = ((ElementNode) n).getElement();
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
        LOGGER.info("logicalStatement (XML) incoming: <{}>", ctx.getText());
        Node rv = new NullNode();
        Node left = new NullNode();
        TerminalNode leftIsTerminal = null;

        // Visit leftmost child if it is not a terminal node
        if (!(ctx.getChild(0) instanceof TerminalNode)) {
            left = visit(ctx.getChild(0));
        }
        else {
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
        }
        else if (ctx.getChildCount() == 2) {
            // Two children, visit rightmost child
            Node right = visit(ctx.getChild(1));
            Element el;

            if (leftIsTerminal != null) {
                // Should be NOT
                el = doc.createElement(leftIsTerminal.getText().toUpperCase());
            }
            else {
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

        }
        else if (ctx.getChildCount() == 3) {
            // Three children; logicalStmt AND/OR logicalStmt
            TerminalNode operation = (TerminalNode) ctx.getChild(1);
            Node right = visit(ctx.getChild(2));

            Element el = doc.createElement(operation.getText().toUpperCase());

            el.appendChild(((ElementNode) left).getElement());
            el.appendChild(((ElementNode) right).getElement());

            rv = new ElementNode(el);
        }

        else {
            throw new IllegalStateException(
                    "Unexpected number of children: " + ctx.getChildCount() + " in query: " + ctx.getText()
            );
        }

        if (rv instanceof SubSearchNode) {
            LOGGER.info("[XML] [LogiStat] Return value was SubsearchNode. Converting to ElementNode!");
            return new ElementNode(((SubSearchNode) rv).asElement(doc));
        }

        LOGGER.debug("visitLogicalStatement outgoing: <{}>", rv);
        return rv;
    }

    @Override
    public Node visitSearchIndexStatement(DPLParser.SearchIndexStatementContext ctx) {
        if (ctx.stringType() != null) {
            String statement = new UnquotedText(new TextString(ctx.stringType().getText())).read();
            String operation = new Token(Token.Type.EQUALS).toString();

            // check for wildcard and turn off bloom if present
            boolean wildcardFound = statement.trim().startsWith("*") || statement.trim().endsWith("*");
            this.catCtx.setWildcardSearchUsed(wildcardFound);

            Element el = doc.createElement("indexstatement");
            el.setAttribute("operation", operation);
            el.setAttribute("value", new UnquotedText(new TextString(statement)).read());

            LOGGER.debug("[XML] SearchIndexStatement return: operation= <{}>, value= <{}>", operation, statement);
            return new ElementNode(el);
        }
        throw new RuntimeException("[XML] SearchIndexStatement did not contain a string type");
    }

    /**
     * sublogicalStatement : PARENTHESIS_L logicalStatement PARENTHESIS_R ;
     */
    @Override
    public Node visitSublogicalStatement(DPLParser.SublogicalStatementContext ctx) {
        // Consume parenthesis
        Node n = visit(ctx.getChild(1));
        return n;
    }

    /**
     * searchQualifier : INDEX (EQ|NEQ) stringType WILDCARD? | SOURCETYPE (EQ|NEQ) stringType WILDCARD? | HOST (EQ|NEQ)
     * stringType WILDCARD? | SOURCE (EQ|NEQ) stringType WILDCARD? | SAVEDSEARCH (EQ|NEQ) stringType WILDCARD? |
     * EVENTTYPE (EQ|NEQ) stringType WILDCARD? | EVENTTYPETAG (EQ|NEQ) stringType WILDCARD? | HOSTTAG (EQ|NEQ)
     * stringType WILDCARD? | TAG (EQ|NEQ) stringType WILDCARD? ;
     */
    @Override
    public Node visitSearchQualifier(DPLParser.SearchQualifierContext ctx) {
        Token comparisonToken;
        String field;
        final List<String> values = new ArrayList<>();

        TerminalNode left = (TerminalNode) ctx.getChild(0);
        TerminalNode operation = (TerminalNode) ctx.getChild(1);

        // check whether operation is '=' or '!='
        if (operation.getSymbol().getType() == DPLLexer.EQ) {
            comparisonToken = new Token(Type.EQUALS);
        }
        else {
            comparisonToken = new Token(Type.NOT_EQUALS);
        }

        // Default clause used in WHERE-part
        final String qualifier = left.getText() + " " + operation.getSymbol().getText() + " ";

        // HOST and SOURCETYPE qualifier stored as additional list and used for Kafka content filtering
        if (left.getSymbol().getType() == DPLLexer.INDEX_IN) {
            ctx.indexStringType().forEach(str -> {
                values.add(new UnquotedText(new TextString(str.getText().toLowerCase())).read());
            });
            comparisonToken = new Token(Type.EQUALS); // INDEX IN is equals
            field = "index";
        }
        else if (left.getSymbol().getType() == DPLLexer.HOST) {
            values.add(new UnquotedText(new TextString(ctx.getChild(2).getText().toLowerCase())).read());
            field = "host";
        }
        else if (left.getSymbol().getType() == DPLLexer.SOURCETYPE && operation.getSymbol().getType() == DPLLexer.IN) {
            ctx.stringType().forEach(str -> {
                values.add(new UnquotedText(new TextString(str.getText().toLowerCase())).read());
            });
            comparisonToken = new Token(Type.EQUALS); // SOURCETYPE IN is equals
            field = "sourcetype";
        }
        else if (left.getSymbol().getType() == DPLLexer.SOURCETYPE) {
            values.add(new UnquotedText(new TextString(ctx.getChild(2).getText().toLowerCase())).read());
            field = "sourcetype";
            LOGGER.debug("qualifier=<{}> sourcetype=<{}>", qualifier, values.get(0));
        }
        else {
            // other column=value qualifier
            values.add(new UnquotedText(new TextString(ctx.getChild(2).getText().toLowerCase())).read());
            field = ctx.getChild(0).getText().toLowerCase();
            LOGGER
                    .debug("custom qualifier: field=<{}> = value=<{}>", ctx.getChild(0).getText(), ctx.getChild(2).getText());
        }

        Element el;
        if (values.size() < 2) {
            el = doc.createElement(field);
            el.setAttribute("operation", comparisonToken.toString());
            el.setAttribute("value", values.get(0));
        }
        else {
            // build xml string for index IN ( 1 2 )
            el = doc.createElement("OR");

            for (int i = 0; i < values.size(); i++) {
                Element indexElem = doc.createElement(field);
                indexElem.setAttribute("operation", comparisonToken.toString());
                indexElem.setAttribute("value", values.get(i));

                if (i < 2) {
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

    /**
     * stringType WILDCARD? | WILDCARD | termStatement | caseStatement
     *
     * @return StringNode,
     */
    @Override
    public Node visitIndexStatement(DPLParser.IndexStatementContext ctx) {
        TerminalNode index = (TerminalNode) ctx.getChild(0);
        String value = new UnquotedText(new TextString(ctx.getChild(1).getText().toLowerCase())).read();
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

        ElementNode node = new ElementNode(el);
        LOGGER.debug("Got index statement: <{}>", node);
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

        LOGGER.info("[XML] Comparison statement: <{}> with <{}> children.", value, ctx.getChildCount());
        Element el = doc.createElement("comparisonstatement");
        el.setAttribute("field", field);

        if (specialCase) {
            el.setAttribute("operation", operation);
            el.setAttribute("value", new UnquotedText(new TextString(value)).read());
        }
        else {
            el.setAttribute("operation", ctx.getChild(1).getText());
            el.setAttribute("value", new UnquotedText(new TextString(ctx.getChild(2).getText())).read());
        }

        ElementNode node = new ElementNode(el);
        return node;
    }

    /**
     * subindexStatement : PARENTHESIS_L directoryStatement PARENTHESIS_R ;
     */
    @Override
    public Node visitSubindexStatement(DPLParser.SubindexStatementContext ctx) {
        // Consume parenthesis
        return visit(ctx.getChild(1));
    }

    @Override
    public Node visitSubsearchStatement(DPLParser.SubsearchStatementContext ctx) {
        // Don't visit further when using XML visitor
        return null;
    }

    // Time format handling
    @Override
    public Node visitTimeStatement(DPLParser.TimeStatementContext ctx) {
        TimeStatement timeStatement = new TimeStatement(catCtx, doc);

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

    public Node visitFieldType(DPLParser.FieldTypeContext ctx) {
        String sql = ctx.getChild(0).getText();
        return new StringNode(new Token(Type.STRING, sql));
    }
}
