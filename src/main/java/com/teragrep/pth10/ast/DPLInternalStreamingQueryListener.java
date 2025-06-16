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

import com.teragrep.pth_06.ArchiveMicroStreamReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * StreamingQueryListener used to handle stopping all the streaming queries used internally in the DPL translation layer
 * (pth_10).
 */
public class DPLInternalStreamingQueryListener extends StreamingQueryListener implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLInternalStreamingQueryListener.class);

    /**
     * Map containing all the queryName-DPLInternalStreamingQuery k,v pairs
     */
    private final Map<String, DPLInternalStreamingQuery> _queries = new HashMap<>();

    /**
     * Used to send messages about the current streaming queries to the UI Map [K: String, V: Map[K:String, V:String]]
     */
    private Consumer<Map<String, Map<String, String>>> _msgHandler;

    /**
     * Contains information of all the queries Key = query id Value = map of query info key-value
     */
    private final Map<String, Map<String, String>> _queryInfoMap = new HashMap<>();

    public DPLInternalStreamingQueryListener() {
        super();
    }

    /**
     * Initializes this listener in the specified sparkSession.
     * 
     * @param sparkSession sparkSession to initialize the listener in
     */
    public void init(SparkSession sparkSession) {
        if (sparkSession != null) {
            LOGGER.info("Registering DPLInternalStreamingQueryListener to SparkSession");
            sparkSession.streams().addListener(this);
        }
        else {
            LOGGER.error("Could not register DPLInternalStreamingQueryListener to SparkSession - it was null");
        }

    }

    /**
     * Add message handler to this listener
     * 
     * @param handler Consumer of type String
     */
    public void registerHandler(Consumer<Map<String, Map<String, String>>> handler) {
        LOGGER.info("Registering handler to DPLInternalStreamingQueryListener");
        this._msgHandler = handler;
    }

    /**
     * Remove message handler from this listener
     */
    public void unregisterHandler() {
        LOGGER.info("Unregistering handler from DPLInternalStreamingQueryListener");
        this._msgHandler = null;
    }

    /**
     * Emit on query start
     * 
     * @param queryStartedEvent Spark calls on query start
     */
    @Override
    public void onQueryStarted(QueryStartedEvent queryStartedEvent) {
        final Map<String, String> internalKeyValueMap = new HashMap<>();

        internalKeyValueMap.put("name", queryStartedEvent.name());
        internalKeyValueMap.put("status", "started");

        // update main map and send event
        this._queryInfoMap.put(queryStartedEvent.id().toString(), internalKeyValueMap);
        sendMessageEvent(this._queryInfoMap);
    }

    /**
     * Emit on query progress
     * 
     * @param queryProgressEvent Spark calls on query progress
     */
    @Override
    public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
        final String nameOfQuery = queryProgressEvent.progress().name();
        final Map<String, String> internalKeyValueMap = new HashMap<>();

        internalKeyValueMap.put("name", nameOfQuery);
        internalKeyValueMap.put("status", "processing");

        internalKeyValueMap
                .put("processedRowsPerSecond", String.valueOf(queryProgressEvent.progress().processedRowsPerSecond()));
        internalKeyValueMap.put("batchId", String.valueOf(queryProgressEvent.progress().batchId()));
        internalKeyValueMap
                .put("inputRowsPerSecond", String.valueOf(queryProgressEvent.progress().inputRowsPerSecond()));
        internalKeyValueMap.put("id", String.valueOf(queryProgressEvent.progress().id()));
        internalKeyValueMap.put("runId", String.valueOf(queryProgressEvent.progress().runId()));
        internalKeyValueMap.put("timestamp", queryProgressEvent.progress().timestamp());

        internalKeyValueMap.put("sinkDescription", queryProgressEvent.progress().sink().description());

        // source info
        if (queryProgressEvent.progress().sources().length != 0) {
            internalKeyValueMap.put("sourceDescription", queryProgressEvent.progress().sources()[0].description());
            internalKeyValueMap.put("sourceStartOffset", queryProgressEvent.progress().sources()[0].startOffset());
            internalKeyValueMap.put("sourceEndOffset", queryProgressEvent.progress().sources()[0].endOffset());
            internalKeyValueMap
                    .put(
                            "sourceInputRowsPerSecond",
                            String.valueOf(queryProgressEvent.progress().sources()[0].inputRowsPerSecond())
                    );
            internalKeyValueMap
                    .put(
                            "sourceProcessedRowsPerSecond",
                            String.valueOf(queryProgressEvent.progress().sources()[0].processedRowsPerSecond())
                    );
            internalKeyValueMap
                    .put("sourceNumInputRows", String.valueOf(queryProgressEvent.progress().sources()[0].numInputRows()));
        }

        // completion checking
        if (this.isRegisteredQuery(nameOfQuery)) {
            DPLInternalStreamingQuery sq = this.getQuery(nameOfQuery);
            if (this.checkCompletion(sq)) {
                this.stopQuery(nameOfQuery);
                boolean wasRemoved = this.removeQuery(nameOfQuery);

                // status -> complete
                internalKeyValueMap.put("status", "complete");

                if (!wasRemoved) {
                    LOGGER
                            .error(
                                    "Removing the query <{}> from the internal DPLStreamingQuery listener was unsuccessful!",
                                    nameOfQuery
                            );
                }
            }
        }
        else {
            internalKeyValueMap.put("status", "notRegistered");
        }

        // update main map and send event
        this._queryInfoMap.put(queryProgressEvent.progress().id().toString(), internalKeyValueMap);
        sendMessageEvent(this._queryInfoMap);
    }

    /**
     * Emit on query termination
     * 
     * @param queryTerminatedEvent Spark calls on query termination
     */
    @Override
    public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
        final Map<String, String> internalKeyValueMap = new HashMap<>();
        internalKeyValueMap.put("status", "terminated");

        // get name for query, QueryTerminatedEvent does not include the human readable name
        if (this._queryInfoMap.containsKey(queryTerminatedEvent.id().toString())) {
            final String name = this._queryInfoMap.get(queryTerminatedEvent.id().toString()).get("name");
            internalKeyValueMap.put("name", name);
        }

        // put to main map and send event
        this._queryInfoMap.put(queryTerminatedEvent.id().toString(), internalKeyValueMap);
        sendMessageEvent(this._queryInfoMap);
    }

    /**
     * Starts a streaming query with the given name and registers it to the listener
     * 
     * @param name Name of the query, must be unique
     * @param dsw  DataStreamWriter to start
     * @return StreamingQuery; use its awaitTermination to block
     */
    public StreamingQuery registerQuery(final String name, DataStreamWriter<Row> dsw) {
        if (this.isRegisteredQuery(name)) {
            throw new RuntimeException("A query was already registered with the given name: " + name);
        }
        else {
            try {
                this._queries.put(name, new DPLInternalStreamingQuery(dsw.queryName(name).start()));
            }
            catch (TimeoutException e) {
                LOGGER.error("Exception occurred on query start <{}>", e.getMessage(), e);
                throw new RuntimeException("Could not register query: " + e.getMessage());
            }
        }

        return this._queries.get(name).getQuery();
    }

    /**
     * Remove query from the listener
     * 
     * @param name queryName
     * @return was removal successful (bool)
     */
    public boolean removeQuery(String name) {
        Object v = this._queries.remove(name);

        return v != null;
    }

    /**
     * Stop query
     * 
     * @param name Name of the query to stop
     */
    public void stopQuery(String name) {
        try {
            this._queries.get(name).getQuery().stop();
        }
        catch (TimeoutException e) {
            LOGGER.error("Exception occurred on query stop <{}>", e.getMessage(), e);
            throw new RuntimeException("Exception occurred on query stop: " + e.getMessage());
        }
    }

    /**
     * Does the internal map contain the specified query
     * 
     * @param name queryName
     * @return bool
     */
    public boolean isRegisteredQuery(String name) {
        return this._queries.containsKey(name);
    }

    /**
     * Returns the internal DPLInternalStreamingQuery object based on queryName
     * 
     * @param name queryName
     * @return DPLInternalStreamingQuery object
     */
    private DPLInternalStreamingQuery getQuery(String name) {
        return this._queries.get(name);
    }

    /**
     * Send message to messageHandler if it was registered
     * 
     * @param s message string
     */
    private void sendMessageEvent(Map<String, Map<String, String>> s) {
        if (this._msgHandler != null) {
            LOGGER.debug("Sending message event to registered messageHandler");
            this._msgHandler.accept(s);
        }
        else {
            LOGGER.debug("Message event cannot be sent because a MessageHandler has not been registered.");
        }
    }

    /**
     * Check if the stream has provided all the data or if it is still in progress
     * 
     * @return is the stream complete
     */
    private boolean checkCompletion(DPLInternalStreamingQuery sq) {
        if (sq.getQuery().lastProgress() == null || sq.getQuery().status().message().equals("Initializing sources")) {
            // Query has not started yet
            return false;
        }

        boolean shouldStop = false;

        if (sq.getQuery().lastProgress().batchId() != sq.getLastBatchId()) {
            if (sq.getQuery().lastProgress().sources().length != 0) {
                shouldStop = isMemoryStreamDone(sq.getQuery()) && isArchiveDone(sq.getQuery());
            }
        }
        sq.setLastBatchId(sq.getQuery().lastProgress().batchId());

        return shouldStop;
    }

    /**
     * check if archive stream is done
     * 
     * @param sq StreamingQuery object
     * @return done?
     */
    private boolean isArchiveDone(StreamingQuery sq) {
        boolean isArchiveDone = true;
        for (int i = 0; i < sq.lastProgress().sources().length; i++) {
            SourceProgress progress = sq.lastProgress().sources()[i];

            if (
                progress.description() != null
                        && !progress.description().startsWith(ArchiveMicroStreamReader.class.getName().concat("@"))
            ) {
                // ignore others than archive
                continue;
            }

            if (progress.startOffset() != null) {
                if (!progress.startOffset().equalsIgnoreCase(progress.endOffset())) {
                    isArchiveDone = false;
                }
            }
            else {
                isArchiveDone = false;
            }
        }

        return isArchiveDone;
    }

    /**
     * check if memory stream is done
     * 
     * @param sq StreamingQuery object
     * @return done?
     */
    private boolean isMemoryStreamDone(StreamingQuery sq) {
        boolean isMemoryStreamDone = true;
        for (int i = 0; i < sq.lastProgress().sources().length; i++) {
            SourceProgress progress = sq.lastProgress().sources()[i];

            if (progress.description() != null && !progress.description().startsWith("MemoryStream[")) {
                // ignore others than MemoryStream
                continue;
            }

            if (progress.startOffset() != null) {
                if (!progress.startOffset().equalsIgnoreCase(progress.endOffset())) {
                    isMemoryStreamDone = false;
                }
            }
            else {
                isMemoryStreamDone = false;
            }
        }

        return isMemoryStreamDone;
    }
}
