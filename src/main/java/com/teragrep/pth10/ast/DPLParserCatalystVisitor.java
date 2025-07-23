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
package com.teragrep.pth10.ast;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.logicalstatement.LogicalStatementCatalyst;
import com.teragrep.pth10.ast.commands.logicalstatement.LogicalStatementXML;
import com.teragrep.pth10.ast.commands.logicalstatement.TimeStatement;
import com.teragrep.pth10.ast.commands.transformstatement.TransformStatement;
import com.teragrep.pth10.steps.EmptyDataframeStep;
import com.teragrep.pth10.steps.logicalCatalyst.LogicalCatalystStep;
import com.teragrep.pth10.steps.subsearch.SubsearchStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Visitor used for Catalyst emit mode (main emit mode, XML emit mode only used for archive query)
 */
public class DPLParserCatalystVisitor extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLParserCatalystVisitor.class);

    Node logicalPart = null;
    Node transformPart = null;

    // imported implementations
    LogicalStatementCatalyst logicalCatalyst;
    TransformStatement transformStatement = null;

    private final DPLParserCatalystContext catCtx;

    // hdfs path - used for join command's subsearch save
    private String hdfsPath = null;

    // iplocation mmdb database path
    // primarily set through zeppelin config, this is only used
    // when such config is not found (~testing)
    private String iplocationMmdbPath = null;

    // Message handler for StreamingQueryListener
    private Consumer<Map<String, Map<String, String>>> messageHandler = null;

    // Step "tree" or list
    private final StepList stepList;

    public DPLParserCatalystVisitor(DPLParserCatalystContext ctx) {
        this.catCtx = ctx;
        this.logicalCatalyst = new LogicalStatementCatalyst(this, this.catCtx);
        this.stepList = new StepList(this);
        this.catCtx.setStepList(this.stepList);
    }

    public StepList getStepList() {
        return stepList;
    }

    /**
     * returns "trace buffer", which is an empty list for compatibility reasons. getTracebuffer is used in some older
     * tests, which should be removed
     * 
     * @return empty list
     */
    @Deprecated
    public List<String> getTraceBuffer() {
        return new ArrayList<>();
    }

    @Deprecated
    public Consumer<Dataset<Row>> getConsumer() {
        return this.stepList.getBatchHandler();
    }

    /**
     * Sets the consumer to handle the results of each of the batches
     * 
     * @param consumer Consumer with type Dataset to be implemented in pth_07
     */
    public void setConsumer(Consumer<Dataset<Row>> consumer) {
        this.stepList.setBatchHandler(consumer);
    }

    public void setMessageHandler(Consumer<Map<String, Map<String, String>>> messageHandler) {
        this.messageHandler = messageHandler;
        // register messageHandler to DPLInternalStreamingQueryListener
        if (this.catCtx != null && this.messageHandler != null) {
            this.catCtx.getInternalStreamingQueryListener().registerHandler(this.messageHandler);
        }
        else {
            LOGGER.error("Unable to set message handler successfully.");
        }
    }

    /**
     * Sets the maximum results for batchCollect
     * 
     * @param val int value
     */
    public void setDPLRecallSize(Integer val) {
        this.getCatalystContext().setDplRecallSize(val);
    }

    /**
     * Gets the dpl recall size (max results from batchCollect)
     * 
     * @return max results as int
     */
    public Integer getDPLRecallSize() {
        return this.getCatalystContext().getDplRecallSize();
    }

    /**
     * HDFS path used for join subsearch save
     * 
     * @param path path as string
     */
    public void setHdfsPath(String path) {
        this.hdfsPath = path;
    }

    /**
     * HDFS path used for join/eventstats/subsearch save <br>
     * Generate a random path if none was set via setHdfsPath()
     * 
     * @return path as string
     */
    public String getHdfsPath() {
        if (
            this.hdfsPath == null && this.catCtx != null && this.catCtx.getSparkSession() != null
                    && this.catCtx.getParagraphUrl() != null
        ) {
            final String appId = this.catCtx.getSparkSession().sparkContext().applicationId();
            final String paragraphId = this.catCtx.getParagraphUrl();
            final String path = String.format("/tmp/%s/%s/%s/", appId, paragraphId, UUID.randomUUID());

            // /applicationId/paragraphId/randomUUID/
            LOGGER.debug("No HDFS path specified in visitor, set path to: <[{}]>", path);
            return path;
        }
        else if (this.hdfsPath == null) {
            LOGGER.debug("Random UUID-based HDFS path generation");
            return "/tmp/" + UUID.randomUUID() + "/";
        }
        else {
            LOGGER.debug("Get the specified HDFS path specified in the visitor's field");
            return this.hdfsPath;
        }
    }

    /**
     * Sets the backup mmdb database path used by iplocation command Only used, if the zeppelin config item is not
     * found.
     * 
     * @param iplocationMmdbPath new mmdb file path as string
     */
    public void setIplocationMmdbPath(String iplocationMmdbPath) {
        this.iplocationMmdbPath = iplocationMmdbPath;
    }

    /**
     * Gets the backup mmdb database path used by iplocation command Only used, if the zeppelin config item is not
     * found.
     * 
     * @return mmdb file path as string
     */
    public String getIplocationMmdbPath() {
        return iplocationMmdbPath;
    }

    public boolean getAggregatesUsed() {
        return this.getStepList().getAggregateCount() > 0;
    }

    @Deprecated
    public String getLogicalPart() {
        String rv = null;
        if (logicalPart != null) {
            ColumnNode cn = (ColumnNode) logicalPart;
            LOGGER.debug("\ngetLogicalPart incoming=<{}>", logicalPart);
            rv = cn.asExpression().sql();
        }
        return rv;
    }

    @Deprecated
    public Column getLogicalPartAsColumn() {
        Column rv = null;
        if (logicalPart != null) {
            LOGGER.debug("\ngetLogicalPart incoming=<{}>", logicalPart);
            rv = ((ColumnNode) logicalPart).getColumn();
        }
        return rv;
    }

    /**
     * Get current DPLCatalystContext containing for instance audit information
     * 
     * @return DPLCatalystContext
     */
    public DPLParserCatalystContext getCatalystContext() {
        return catCtx;
    }

    /**
     * Visit the root rule - translate the DPL query to Spark dataframe actions and transformations.
     */
    @Override
    public Node visitRoot(DPLParser.RootContext ctx) {
        LOGGER.info("CatalystVisitor Root incoming: <{}>", ctx.getText());
        // Set DPL query into CatalystContext: used if original query string is needed for something
        catCtx.setDplQuery(ctx.getText());

        // Current version has always 2 nodes at the root level.
        if (ctx.getChildCount() < 1) {
            throw new IllegalStateException("Missing logicalStatement and/or transformStatement: " + ctx.getText());
        }

        // Logical part
        if (ctx.searchTransformationRoot() != null) {
            LOGGER.info("visitRoot Handle logical part: <{}>", ctx.getChild(0).getText());
            logicalPart = visitSearchTransformationRoot(ctx.searchTransformationRoot());
        }
        else {
            // no logical part, e.g. makeresults or similar command in use without main search
            this.getStepList().add(new EmptyDataframeStep(catCtx));
        }

        // Just transform part
        if (ctx.transformStatement() != null) {
            transformPart = visitTransformStatement(ctx.transformStatement());
        }

        LOGGER.info("visitRoot complete.");
        return new TranslationResultNode(stepList);
    }

    @Override
    public Node visitSearchTransformationRoot(DPLParser.SearchTransformationRootContext ctx) {
        LOGGER.info("CatalystVisitor visitSearchTransformationRoot: <{}>", ctx.getText());

        // Check for index= / index!= / index IN without right side
        if (ctx.getChildCount() == 1 && ctx.getChild(0) instanceof TerminalNode) {
            TerminalNode term = (TerminalNode) ctx.getChild(0);
            if (
                term.getSymbol().getType() == DPLLexer.INDEX_EQ || term.getSymbol().getType() == DPLLexer.INDEX_SPACE
                        || term.getSymbol().getType() == DPLLexer.INDEX_NEG
                        || term.getSymbol().getType() == DPLLexer.INDEX_SPACE_NEG
                        || term.getSymbol().getType() == DPLLexer.INDEX_IN
            ) {
                throw new RuntimeException(
                        "The right side of the search qualifier was empty! Check that the index has"
                                + " a valid value, like 'index = cinnamon'."
                );
            }
        }

        Document doc;
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        }
        catch (ParserConfigurationException pce) {
            throw new RuntimeException(pce);
        }

        LogicalStatementXML logicalXML = new LogicalStatementXML(this.catCtx, doc);

        LOGGER.debug("SearchTransformationRoot - generating XML query");
        AbstractStep XMLStep = logicalXML.visitLogicalStatementXML(ctx);
        LOGGER.debug("SearchTransformationRoot - adding XMLStep to step tree");
        this.stepList.add(XMLStep);

        LOGGER.debug("SearchTransformationRoot - generating spark column");
        AbstractStep catalystStep = this.logicalCatalyst.visitLogicalStatementCatalyst(ctx);
        LOGGER.debug("SearchTransformationRoot - adding CatalystStep to step tree");
        this.stepList.add(catalystStep);

        if (catalystStep instanceof LogicalCatalystStep) {
            return new ColumnNode(((LogicalCatalystStep) catalystStep).getFilterColumn());
        }
        return new NullNode();
    }

    /**
     * Used to visit subsearches. Builds a StepList for subsearch and returns SubsearchStep.
     *
     * @param ctx SubsearchTransformStatementContext
     * @return StepNode that has subsearchStep inside
     */
    @Override
    public Node visitSubsearchStatement(DPLParser.SubsearchStatementContext ctx) {
        DPLParser.SearchTransformationContext searchCtx = null;
        if (ctx.transformStatement() != null) {
            searchCtx = ctx.transformStatement().searchTransformation();
        }

        if (searchCtx != null) {
            LOGGER.debug("subsearch has searchTransformation");
            // also generate XML archive query; as subsearch needs to be able to get
            // data from other indices compared to main query
            Document xmlDoc;
            try {
                xmlDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            }
            catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            }
            LogicalStatementXML logicalXml = new LogicalStatementXML(catCtx, xmlDoc);

            AbstractStep xmlStep = logicalXml.visitLogicalStatementXML(searchCtx.searchTransformationRoot());
            AbstractStep catalystStep = this.logicalCatalyst
                    .visitLogicalStatementCatalyst(searchCtx.searchTransformationRoot());

            this.stepList.add(xmlStep);
            this.stepList.add(catalystStep);

            if (ctx.transformStatement() != null && ctx.transformStatement().transformStatement() != null) {
                transformStatement = new TransformStatement(catCtx, this);
                // Adding transformation steps to stepList is done in TransformStatement
                transformStatement.visit(ctx.transformStatement().transformStatement());
            }

        }
        else { // no main search, check first transformStatement
            if (ctx.transformStatement() != null) {
                transformStatement = new TransformStatement(catCtx, this);
                // Adding transformation steps to stepList is done in TransformStatement
                transformStatement.visit(ctx.transformStatement());
            }
        }

        AbstractStep subsearchStep = new SubsearchStep(this.stepList);

        return new StepNode(subsearchStep);
    }

    /**
     * logicalStatement : macroStatement | subsearchStatement | sublogicalStatement | timeStatement | searchQualifier |
     * Not logicalStatement | indexStatement | comparisonStatement | logicalStatement Or logicalStatement |
     * logicalStatement And? logicalStatement ;
     */
    @Override
    public Node visitLogicalStatement(DPLParser.LogicalStatementContext ctx) {
        return this.logicalCatalyst.visitLogicalStatement(ctx);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns the result of calling {@link #visitChildren} on {@code ctx}.
     * </p>
     */
    @Override
    public Node visitComparisonStatement(DPLParser.ComparisonStatementContext ctx) {
        String value = ctx.getChild(0).getText() + ctx.getChild(1).getText() + ctx.getChild(2).getText();
        return new StringNode(new Token(Type.STRING, value));
    }

    /**
     * Time format handling timeStatement : timeFormatQualifier? timeQualifier ;
     */
    @Override
    public Node visitTimeStatement(DPLParser.TimeStatementContext ctx) {
        TimeStatement timeStatement = new TimeStatement(catCtx);
        return timeStatement.visitTimeStatement(ctx);
    }

    @Override
    public Node visitTransformStatement(DPLParser.TransformStatementContext ctx) {
        transformStatement = new TransformStatement(catCtx, this);
        return transformStatement.visit(ctx);
    }

    @Override
    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        return this.logicalCatalyst.visitL_evalStatement_evalCompareStatement(ctx);
    }

    @Override
    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        return visitChildren(ctx);
    }
}
