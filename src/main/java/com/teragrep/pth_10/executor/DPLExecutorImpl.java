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
            String queryName,
            String noteId,
            String paragraphId,
            String lines
    ) throws TimeoutException {
        LOGGER.debug("Running in interpret()");
        batchCollect.clear(); // do not store old values // TODO remove from NotebookDatasetStore too

        LOGGER.info("queryId <{}> DPL-interpreter initialized sparkInterpreter incoming query :<{}>", queryName, lines);
        DPLParserCatalystContext catalystContext = new DPLParserCatalystContext(sparkSession, config, queryName);

        LOGGER.debug("queryId <{}> Adding audit information", queryName);
        catalystContext.setAuditInformation(setupAuditInformation(lines));

        LOGGER.debug("queryId <{}> Setting baseurl", queryName);
        catalystContext.setBaseUrl(config.getString("dpl.web.url"));
        LOGGER.debug("queryId <{}> Setting notebook url", queryName);
        catalystContext.setNotebookUrl(noteId);
        LOGGER.debug("queryId <{}> Setting paragraph url", queryName);
        catalystContext.setParagraphUrl(paragraphId);

        LOGGER.debug("queryId <{}> Creating lexer", queryName);
        DPLLexer lexer = new DPLLexer(CharStreams.fromString(lines));
        // Catch also lexer-errors i.e. missing '"'-chars and so on.
        lexer.addErrorListener(new DPLErrorListenerImpl("Lexer", queryName));

        LOGGER.debug("queryId <{}> Creating parser", queryName);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        LOGGER.debug("queryId <{}> Setting earliest", queryName);
        catalystContext.setEarliest("-1Y"); // TODO take from TimeSet
        LOGGER.debug("queryId <{}> Creating visitor", queryName);
        DPLParserCatalystVisitor visitor = new DPLParserCatalystVisitor(catalystContext);

        // Get syntax errors and throw then to zeppelin before executing stream handling
        LOGGER.debug("queryId <{}> Added error listener", queryName);
        parser.addErrorListener(new DPLErrorListenerImpl("Parser", queryName));

        ParseTree tree;
        try {
            LOGGER.debug("queryId <{}> Running parser tree root", queryName);
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
        LOGGER.debug("queryId <{}> Creating consumer", queryName);
        visitor.setConsumer(batchHandler);

        // set BatchCollect size
        LOGGER.debug("queryId <{}> Setting recall size", queryName);
        visitor.setDPLRecallSize(config.getInt("dpl.recall-size"));

        LOGGER.debug("queryId <{}> Creating translationResultNode", queryName);
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
            LOGGER.error("queryId <{}>  got exception: <{}>:", queryName, e.getMessage(), e);

            // get root cause of the exception
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            return new DPLExecutorResultImpl(DPLExecutorResult.Code.ERROR, exception.getMessage());
        }

        LOGGER.debug("queryId <{}> Checking if aggregates are used", queryName);
        boolean aggregatesUsed = visitor.getAggregatesUsed();
        LOGGER
                .info(
                        "queryId <{}> -------DPLExecutor aggregatesUsed: <{}> visitor: <{}>", queryName, aggregatesUsed,
                        visitor.getClass().getName()
                );

        LOGGER.debug("queryId <{}> Running startQuery", queryName);
        streamingQuery = startQuery(dsw, queryName);
        LOGGER.debug("queryId <{}> started", streamingQuery.name());

        //outQ.explain(); // debug output

        // attach listener for query termination
        LOGGER.debug("queryId <{}> Adding the listener", streamingQuery.name());
        sparkSession.streams().addListener(new DPLStreamingQueryListener(streamingQuery, config, catalystContext));

        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug(
                                "queryId <{}> Streaming query is active: <{}>", streamingQuery.name(),
                                streamingQuery.isActive()
                        );
                LOGGER
                        .debug(
                                "queryId <{}> Streaming query has status: <{}>", streamingQuery.name(),
                                streamingQuery.status().toString()
                        );
                LOGGER
                        .debug(
                                "queryId <{}> Awaiting streamingQuery termination for <[{}]>", streamingQuery.name(),
                                getQueryTimeout()
                        );
            }
            streamingQuery.awaitTermination(getQueryTimeout());

            if (streamingQuery.isActive()) {
                LOGGER.debug("queryId <{}> Forcing streamingQuery termination", streamingQuery.name());
                streamingQuery.stop();
            }
            LOGGER.debug("queryId <{}> Streaming query terminated", streamingQuery.name());
        }
        catch (StreamingQueryException e) {
            // log initial exception
            LOGGER.error("queryId <{}> Query got exception: <{}>:", queryName, e.getMessage(), e);

            // get root cause of the exception
            Throwable exception = e;
            while (exception.getCause() != null) {
                exception = exception.getCause();
            }
            return new DPLExecutorResultImpl(DPLExecutorResult.Code.ERROR, exception.getMessage());
        }

        LOGGER.debug("queryId <{}> Returning from interpret()", queryName);
        return new DPLExecutorResultImpl(DPLExecutorResult.Code.SUCCESS, "");
    }

    private StreamingQuery startQuery(DataStreamWriter<Row> rowDataset, String queryName) throws TimeoutException {
        LOGGER.debug("queryId <{}> Running startQuery", queryName);
        StreamingQuery outQ;

        long processingTimeMillis = 0;
        if (config.hasPath("dpl.pth_07.trigger.processingTime")) {
            LOGGER.debug("queryId <{}> Got processingTime trigger", queryName);
            processingTimeMillis = config.getLong("dpl.pth_07.trigger.processingTime");
        }

        LOGGER.debug("queryId <{}> Running rowDataset trigger", queryName);
        outQ = rowDataset.trigger(Trigger.ProcessingTime(processingTimeMillis)).queryName(queryName).start();
        LOGGER.debug("queryId <{}> Trigger done", outQ.name());

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
        LOGGER.debug("queryId <{}> Request to stop streaming query", streamingQuery.name());
        if (
            streamingQuery != null && !streamingQuery.sparkSession().sparkContext().isStopped()
                    && streamingQuery.isActive()
        ) {
            LOGGER.info("queryId <{}> Stopping streaming query", streamingQuery.name());
            streamingQuery.stop();
        }
    }
}
