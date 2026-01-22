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

import org.apache.spark.sql.Row;
import org.jooq.BatchBindStep;

import java.util.Objects;

public final class EpochMigrationBatch {

    private final BatchBindStep batch;
    private final long batchSize;
    private final int batchCount;
    private final int acceptedRows;

    public EpochMigrationBatch(final BatchBindStep batch, final long batchSize) {
        this(batch, batchSize, 0, 0);
    }

    public EpochMigrationBatch(
            final BatchBindStep batch,
            final long batchSize,
            final int batchCount,
            final int acceptedRows
    ) {
        this.batch = batch;
        this.batchSize = batchSize;
        this.batchCount = batchCount;
        this.acceptedRows = acceptedRows;
    }

    public EpochMigrationBatch accept(final Row row) {
        final EventMetadata metadata = new EventMetadataFromString(row.getString(row.fieldIndex("_raw")));
        if (!metadata.isSyslog()) {
            return this;
        }
        final long epoch = row.getTimestamp(row.fieldIndex("_time")).toInstant().getEpochSecond();
        final long id = Long.parseLong(row.getString(row.fieldIndex("partition")));
        return new EpochMigrationBatch(batch.bind(epoch, id), batchSize, batchCount + 1, acceptedRows + 1);
    }

    public boolean shouldFlushRows() {
        return batchCount >= batchSize;
    }

    public boolean hasPendingRows() {
        return batchCount > 0;
    }

    public BatchBindStep batch() {
        return batch;
    }

    public int acceptedRows() {
        return acceptedRows;
    }

    public EpochMigrationBatch reset(final BatchBindStep newBatch) {
        return new EpochMigrationBatch(newBatch, batchSize, 0, acceptedRows);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        final EpochMigrationBatch that = (EpochMigrationBatch) object;
        return batchSize == that.batchSize && batchCount == that.batchCount && acceptedRows == that.acceptedRows
                && Objects.equals(batch, that.batch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batch, batchSize, batchCount, acceptedRows);
    }
}
