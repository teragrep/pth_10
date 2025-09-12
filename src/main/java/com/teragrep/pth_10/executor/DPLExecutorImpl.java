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
package com.teragrep.pth_10.executor;

import com.teragrep.functions.dpf_02.BatchCollect;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_10.ast.DPLAuditInformation;
import com.teragrep.pth_10.ast.DPLParserCatalystContext;
import com.teragrep.pth_10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth_10.ast.bo.TranslationResultNode;
import com.teragrep.pth_15.DPLExecutor;
import com.teragrep.pth_15.DPLExecutorResult;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public final class DPLExecutorImpl implements DPLExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLExecutorImpl.class);
    private final Config config;
    private final BatchCollect batchCollect;

    // Active query
    StreamingQuery streamingQuery = null;

    public DPLExecutorImpl(Config config) {
        LOGGER.info("DPLExecutor() was initialized");
        this.config = config;
        this.batchCollect = new BatchCollect("_time", this.config.getInt("dpl.recall-size"));
    }

    private DPLAuditInformation setupAuditInformation(String query) {
        LOGGER.debug("Setting audit information");
        DPLAuditInformation auditInformation = new DPLAuditInformation();
        auditInformation.setQuery(query);
        auditInformation.setReason(""); // TODO new UI element for this
        auditInformation.setUser(System.getProperty("user.name"));
        auditInformation.setTeragrepAuditPluginClassName("com.teragrep.rad_01.DefaultAuditPlugin");
        return auditInformation;
    }

    @Override
    public DPLExecutorResult interpret(
            BiConsumer<Dataset<Row>, Boolean> batchHandler,
            SparkSession sparkSession,
            String queryId,
            String noteId,
            String paragraphId,
            String lines
    ) throws TimeoutException {
        LOGGER.debug("Running in interpret()");
        batchCollect.clear(); // do not store old values // TODO remove from NotebookDatasetStore too

        LOGGER.info("DPL-interpreter initialized sparkInterpreter incoming query:<{}>", lines);
        DPLParserCatalystContext catalystContext = new DPLParserCatalystContext(sparkSession, config);

        LOGGER.debug("Adding audit information");
        catalystContext.setAuditInformation(setupAuditInformation(lines));

        LOGGER.debug("Setting baseurl");
        catalystContext.setBaseUrl(config.getString("dpl.web.url"));
        LOGGER.debug("Setting notebook url");
        catalystContext.setNotebookUrl(noteId);
        LOGGER.debug("Setting paragraph url");
        catalystContext.setParagraphUrl(paragraphId);

        LOGGER.debug("Creating lexer");
        DPLLexer lexer = new DPLLexer(CharStreams.fromString(lines));
        // Catch also lexer-errors i.e. missing '"'-chars and so on. 
        lexer.addErrorListener(new DPLErrorListenerImpl("Lexer"));

        LOGGER.debug("Creating parser");
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        LOGGER.debug("Setting earliest");
        catalystContext.setEarliest("-1Y"); // TODO take from TimeSet
        LOGGER.debug("Creating visitor");
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(catalystContext);

        // Get syntax errors and throw then to zeppelin before executing stream handling
        LOGGER.debug("Added error listener");
        parser.addErrorListener(new DPLErrorListenerImpl("Parser"));

        ParseTree tree;
        try {
            LOGGER.debug("Running parser tree root");
            tree = parser.root();
        }
        catch (IllegalStateException e) {
            return new DPLExecutorResultImpl(DPLExecutorResult.Code.ERROR, e.toString());
        }
        catch (StringIndexOutOfBoundsException e) {
            final String msg = "Parsing error: String index out of bounds. Check for unbalanced quotes - "
                    + "make sure each quote (\") has a pair!";
            return new DPLExecutorResultImpl(DPLExecutorResult.Code.ERROR, msg);
        }

        // set output consumer
        LOGGER.debug("Creating consumer");
        visitor.setConsumer(batchHandler);

        // set BatchCollect size
        LOGGER.debug("Setting recall size");
        visitor.setDPLRecallSize(config.getInt("dpl.recall-size"));

        LOGGER.debug("Creating translationResultNode");
        TranslationResultNode n = (TranslationResultNode) visitor.visit(tree);
        DataStreamWriter<Row> dsw;
        if (n == null) {
            return new DPLExecutorResultImpl(
                    DPLExecutorResult.Code.ERROR,
                    "parser can't construct processing pipeline"
            );
        }
        // execute steplist
        try {
            dsw = n.stepList.execute();
        }
        catch (Exception e) {
            // This will also catch AnalysisExceptions, however Spark does not differentiate between
            // different types, they're all Exceptions.
            // log initial exception
            LOGGER.error("Got exception: <{}>:", e.getMessage(), e);

            // get root cause of the exception
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            return new DPLExecutorResultImpl(DPLExecutorResult.Code.ERROR, exception.getMessage());
        }

        LOGGER.debug("Checking if aggregates are used");
        boolean aggregatesUsed = visitor.getAggregatesUsed();
        LOGGER.info("-------DPLExecutor aggregatesUsed: {} visitor: {}", aggregatesUsed, visitor.getClass().getName());

        LOGGER.debug("Running startQuery");
        streamingQuery = startQuery(dsw, queryId);
        LOGGER.debug("Query started");

        //outQ.explain(); // debug output

        // attach listener for query termination
        LOGGER.debug("Adding the listener");
        sparkSession.streams().addListener(new DPLStreamingQueryListener(streamingQuery, config, catalystContext));

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Streaming query is active: {}", streamingQuery.isActive());
                LOGGER.debug("Streaming query status: {}", streamingQuery.status().toString());
                LOGGER.debug("Awaiting streamingQuery termination for <[{}]>", getQueryTimeout());
            }
            streamingQuery.awaitTermination(getQueryTimeout());

            if (streamingQuery.isActive()) {
                LOGGER.debug("Forcing streamingQuery termination");
                streamingQuery.stop();
            }
            LOGGER.debug("Streaming query terminated");
        }
        catch (StreamingQueryException e) {
            // log initial exception
            LOGGER.error("Got exception: <{}>:", e.getMessage(), e);

            // get root cause of the exception
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            return new DPLExecutorResultImpl(DPLExecutorResult.Code.ERROR, exception.getMessage());
        }

        LOGGER.debug("Returning from interpret()");
        return new DPLExecutorResultImpl(DPLExecutorResult.Code.SUCCESS, "");
    }

    private StreamingQuery startQuery(DataStreamWriter<Row> rowDataset, String queryId) throws TimeoutException {
        LOGGER.debug("Running startQuery");
        StreamingQuery outQ;

        long processingTimeMillis = 0;
        if (config.hasPath("dpl.pth_07.trigger.processingTime")) {
            LOGGER.debug("Got processingTime trigger");
            processingTimeMillis = config.getLong("dpl.pth_07.trigger.processingTime");
        }

        LOGGER.debug("Running rowDataset trigger");
        outQ = rowDataset.trigger(Trigger.ProcessingTime(processingTimeMillis)).queryName(queryId).start();
        LOGGER.debug("Trigger done");

        return outQ;
    }

    private long getQueryTimeout() {
        final long rv;
        if (config.hasPath("dpl.pth_07.query.timeout")) {
            long configuredValue = config.getLong("dpl.pth_07.query.timeout");
            if (configuredValue < 0) {
                rv = Long.MAX_VALUE;
            }
            else {
                rv = configuredValue;
            }
        }
        else {
            rv = Long.MAX_VALUE;
        }
        return rv;
    }

    @Override
    public void stop() throws TimeoutException {
        LOGGER.debug("Request to stop streaming query");
        if (
            streamingQuery != null && !streamingQuery.sparkSession().sparkContext().isStopped()
                    && streamingQuery.isActive()
        ) {
            LOGGER.info("Stopping streaming query");
            streamingQuery.stop();
        }
    }
}
