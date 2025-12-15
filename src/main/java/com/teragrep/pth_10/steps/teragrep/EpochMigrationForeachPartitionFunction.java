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
package com.teragrep.pth_10.steps.teragrep;

import com.teragrep.pth_10.steps.teragrep.bloomfilter.LazyConnection;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.jooq.BatchBindStep;
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
import java.util.concurrent.TimeUnit;

final class EpochMigrationForeachPartitionFunction implements ForeachPartitionFunction<Row> {

    private final static Logger LOGGER = LoggerFactory.getLogger(EpochMigrationForeachPartitionFunction.class);

    private final LazyConnection lazyConnection;
    private final String tableName;
    private final long batchSize;
    private final Settings settings;

    EpochMigrationForeachPartitionFunction(final Config config, final String tableName) {
        this(config, tableName, new Settings());
    }

    EpochMigrationForeachPartitionFunction(final Config config, final String tableName, final Settings settings) {
        this(new LazyConnection(config), tableName, 500L, settings);
    }

    EpochMigrationForeachPartitionFunction(
            final LazyConnection lazyConnection,
            final String tableName,
            final long batchSize,
            final Settings settings
    ) {
        this.lazyConnection = lazyConnection;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.settings = settings;
    }

    @Override
    public void call(final Iterator<Row> iter) {
        final long start = System.nanoTime();

        try (final Connection conn = lazyConnection.get()) {
            conn.setAutoCommit(false);
            final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL, settings);
            BatchBindStep currentBatch = ctx.batch(baseQuery(ctx));
            int batchCount = 0;
            int totalRows = 0;
            while (iter.hasNext()) {
                final Row row = iter.next();
                final long epochHour = Long.parseLong(row.getAs("_time"));
                final long id = Long.parseLong(row.getAs("partition"));
                currentBatch.bind(epochHour, id);
                batchCount++;
                if (batchCount >= batchSize) {
                    if (LOGGER.isDebugEnabled()) {
                        final long batchDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                        LOGGER
                                .debug(
                                        "executing batch update: table=<{}> batchSize=<{}> duration=<{}>", tableName,
                                        batchSize, batchDuration
                                );
                    }
                    currentBatch = executeAndReset(currentBatch, ctx, conn);
                    totalRows += batchCount;
                    batchCount = 0;
                }
            }
            if (batchCount > 0) {
                LOGGER.debug("executing batch update flush for remaining rows=<{}>", batchCount);
                executeAndReset(currentBatch, ctx, conn);
            }
            final long end = System.nanoTime();
            final long duration = TimeUnit.NANOSECONDS.toMillis(end - start);
            LOGGER
                    .info(
                            "epoch migration for each function finished. Updated total rows=<{}> duration=<{}>ms",
                            totalRows, duration
                    );

        }
        catch (SQLException e) {
            throw new RuntimeException(
                    "Exception running epoch migration for each partition function: " + e.getMessage()
            );
        }
    }

    private BatchBindStep executeAndReset(final BatchBindStep batch, final DSLContext ctx, final Connection conn)
            throws SQLException {
        try {
            batch.execute();
            conn.commit();
        }
        catch (Exception e) {
            conn.rollback();
            throw new SQLException(e);
        }
        return ctx.batch(baseQuery(ctx));
    }

    private Query baseQuery(final DSLContext ctx) {
        final Field<Long> epochField = DSL.field(DSL.name("epoch_hour"), Long.class);
        final Field<Long> idField = DSL.field(DSL.name("id"), Long.class);
        final Param<Long> epochParam = DSL.param("epoch_hour", Long.class);
        final Param<Long> idParam = DSL.param("id", Long.class);
        final Query baseQuery = ctx
                .update(DSL.table(DSL.name(tableName)))
                .set(epochField, epochParam)
                .where(idField.eq(idParam).and(epochField.isNull()));
        LOGGER.trace("epoch migration for each partition function: batch query <{}>", baseQuery);
        return baseQuery;
    }
}
