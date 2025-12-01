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
package com.teragrep.pth_10.ast;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * StreamingQueryListener used to handle stopping all the streaming queries used internally in the DPL translation layer
 * (pth_10).
 */
public class DPLInternalStreamingQueryListener extends StreamingQueryListener implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DPLInternalStreamingQueryListener.class);

    /**
     * Map containing all the queryName-DPLInternalStreamingQuery k,v pairs
     */
    private final Map<UUID, DPLInternalStreamingQuery> _queries = new HashMap<>();

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
     * Emit on query start
     * 
     * @param queryStartedEvent Spark calls on query start
     */
    @Override
    public void onQueryStarted(QueryStartedEvent queryStartedEvent) {
        //no-op
    }

    /**
     * Emit on query progress
     * 
     * @param queryProgressEvent Spark calls on query progress
     */
    @Override
    public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
        //no-op
    }

    @Override
    public void onQueryIdle(final QueryIdleEvent queryIdleEvent) {
        final UUID queryId = queryIdleEvent.id();

        // completion checking
        if (this.isRegisteredQuery(queryId)) {
            this.stopQuery(queryId);
            boolean wasRemoved = this.removeQuery(queryId);

            if (!wasRemoved) {
                LOGGER
                        .error(
                                "Removing the query <{}> from the internal DPLStreamingQuery listener was unsuccessful!",
                                queryId
                        );
            }
        }
    }

    /**
     * Emit on query termination
     * 
     * @param queryTerminatedEvent Spark calls on query termination
     */
    @Override
    public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
        // no-op
    }

    /**
     * Starts a streaming query with the given name and registers it to the listener
     * 
     * @param name Name of the query, must be unique
     * @param dsw  DataStreamWriter to start
     * @return StreamingQuery; use its awaitTermination to block
     */
    public StreamingQuery registerQuery(final String name, DataStreamWriter<Row> dsw) {
        final StreamingQuery rv;
        try {
            rv = dsw.queryName(name).start();
            this._queries.put(rv.id(), new DPLInternalStreamingQuery(rv));
        }
        catch (final TimeoutException e) {
            LOGGER.error("Exception occurred on query start <{}>", e.getMessage(), e);
            throw new RuntimeException("Could not register query: " + e.getMessage());
        }

        return rv;
    }

    /**
     * Remove query from the listener
     * 
     * @param queryId id of the query
     * @return was removal successful (bool)
     */
    private boolean removeQuery(final UUID queryId) {
        final Object v = this._queries.remove(queryId);

        return v != null;
    }

    /**
     * Stop query
     * 
     * @param queryId ID of the query to stop
     */
    private void stopQuery(final UUID queryId) {
        try {
            this._queries.get(queryId).getQuery().stop();
        }
        catch (final TimeoutException e) {
            LOGGER.error("Exception occurred on query stop <{}>", e.getMessage(), e);
            throw new RuntimeException("Exception occurred on query stop: " + e.getMessage());
        }
    }

    /**
     * Does the internal map contain the specified query
     * 
     * @param queryId id of the query
     * @return bool
     */
    private boolean isRegisteredQuery(final UUID queryId) {
        return this._queries.containsKey(queryId);
    }
}
