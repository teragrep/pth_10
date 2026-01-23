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
package com.teragrep.pth_10.steps.teragrep.migrate;

import com.teragrep.pth_10.steps.teragrep.bloomfilter.LazyConnection;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;

final class EpochMigrationForeachPartitionFunction implements ForeachPartitionFunction<Row> {

    private final static Logger LOGGER = LoggerFactory.getLogger(EpochMigrationForeachPartitionFunction.class);

    private final LazyConnection lazyConnection;
    private final String journalDBName;
    private final long batchSize;
    private final Settings settings;

    EpochMigrationForeachPartitionFunction(final Config config, final String journalDBName) {
        this(config, journalDBName, new Settings());
    }

    EpochMigrationForeachPartitionFunction(final Config config, final String journalDBName, final Settings settings) {
        this(new LazyConnection(config), journalDBName, 500L, settings);
    }

    EpochMigrationForeachPartitionFunction(
            final LazyConnection lazyConnection,
            final String journalDBName,
            final long batchSize,
            final Settings settings
    ) {
        this.lazyConnection = lazyConnection;
        this.journalDBName = journalDBName;
        this.batchSize = batchSize;
        this.settings = settings;
    }

    @Override
    public void call(final Iterator<Row> iter) {
        final Connection conn = lazyConnection.get();
        try {
            conn.setAutoCommit(false);
            final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL, settings);
            EpochMigrationBatchState batch = new EpochMigrationBatchState(ctx.batch(baseQuery(ctx)), batchSize);
            while (iter.hasNext()) {
                batch = batch.accept(iter.next());
                if (batch.shouldFlushRows()) {
                    executeBatch(batch, conn);
                    batch = batch.reset(ctx.batch(baseQuery(ctx)));
                }
            }
            if (batch.hasPendingRows()) {
                executeBatch(batch, conn);
            }
            final long totalRows = batch.totalAccepted();
            LOGGER.info("epoch migration for each partition function finished total rows=<{}>", totalRows);

        }
        catch (final SQLException e) {
            throw new RuntimeException("Exception during epoch migration", e);
        }
        finally {
            resetAutoCommit(conn);
        }
    }

    private void executeBatch(final EpochMigrationBatchState batch, final Connection conn) throws SQLException {
        try {
            batch.batch().execute();
            conn.commit();
        }
        catch (final Exception e) {
            conn.rollback();
            throw new SQLException(e);
        }
    }

    private Query baseQuery(final DSLContext ctx) {
        final Field<Long> epochField = DSL.field(DSL.name("epoch_hour"), Long.class);
        final Field<Long> idField = DSL.field(DSL.name("id"), Long.class);
        final Param<Long> epochParam = DSL.param("epoch_hour", Long.class);
        final Param<Long> idParam = DSL.param("id", Long.class);
        final Query baseQuery = ctx
                .update(DSL.table(DSL.name(journalDBName, "logfile")))
                .set(epochField, epochParam)
                .where(idField.eq(idParam).and(epochField.isNull()));
        LOGGER.trace("epoch migration for each partition function: batch query <{}>", baseQuery);
        return baseQuery;
    }

    private void resetAutoCommit(final Connection conn) {
        try {
            if (!conn.isClosed()) {
                conn.setAutoCommit(true);
            }
        }
        catch (final SQLException e) {
            LOGGER.error("Error turning auto commit true");
        }
    }
}
