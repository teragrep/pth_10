/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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

import org.apache.spark.sql.Row;
import org.jooq.BatchBindStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

final class EpochMigrationBatchState {

    private final Logger LOGGER = LoggerFactory.getLogger(EpochMigrationBatchState.class);
    private final BatchBindStep batch;
    private final long batchSize;
    private final long batchCount;
    private final long acceptedRows;

    EpochMigrationBatchState(final BatchBindStep batch, final long batchSize) {
        this(batch, batchSize, 0, 0);
    }

    private EpochMigrationBatchState(
            final BatchBindStep batch,
            final long batchSize,
            final long batchCount,
            final long acceptedRows
    ) {
        this.batch = batch;
        this.batchSize = batchSize;
        this.batchCount = batchCount;
        this.acceptedRows = acceptedRows;
    }

    EpochMigrationBatchState accept(final Row row) {
        final String rawString = row.getString(row.fieldIndex("_raw"));
        final EventMetadata metadata = new EventMetadataFromString(rawString);
        final long epoch = row.getTimestamp(row.fieldIndex("_time")).toInstant().getEpochSecond();
        final String partitionString = row.getString(row.fieldIndex("partition"));
        final long id = Long.parseLong(partitionString);

        if (!metadata.isSyslog() && LOGGER.isInfoEnabled()) {
            LOGGER
                    .info(
                            "Encountered non-syslog row id=<{}> using path extracted time value <{}> with precision of <{}> resulting epoch <{}>",
                            id, metadata.pathExtracted(), metadata.pathExtractedPrecision(), epoch
                    );
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Non-syslog row metadata: <{}>", metadata);
            }
        }
        else if (LOGGER.isDebugEnabled()) {
            LOGGER.trace("Updating partition id <{}> with epoch value <{}>", id, epoch);
        }

        return new EpochMigrationBatchState(batch.bind(epoch, id), batchSize, batchCount + 1, acceptedRows + 1);
    }

    boolean isFull() {
        return batchCount >= batchSize;
    }

    boolean hasPendingRows() {
        return batchCount > 0;
    }

    BatchBindStep batch() {
        return batch;
    }

    long totalAccepted() {
        return acceptedRows;
    }

    EpochMigrationBatchState reset(final BatchBindStep newBatch) {
        return new EpochMigrationBatchState(newBatch, batchSize, 0, acceptedRows);
    }

    @Override
    public boolean equals(final Object o) {
        final boolean rv;
        if (o == null) {
            rv = false;
        }
        else if (getClass() != o.getClass()) {
            rv = false;
        }
        else {
            final EpochMigrationBatchState that = (EpochMigrationBatchState) o;
            rv = batchSize == that.batchSize && batchCount == that.batchCount && acceptedRows == that.acceptedRows
                    && Objects.equals(LOGGER, that.LOGGER) && Objects.equals(batch, that.batch);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hash(LOGGER, batch, batchSize, batchCount, acceptedRows);
    }
}
