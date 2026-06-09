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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Decorator to map spark Rows from origin Iterator from EpochMigrationForEachBatchFunction into Object arrays that are
 * ready to be loaded into a jooq Loader API.
 * <p>
 * Schema: Object[]{ long id, long epoch, String format }
 */
final class EpochMigrationIterator implements Iterator<Object[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EpochMigrationIterator.class);
    private final Iterator<Row> origin;

    EpochMigrationIterator(final Iterator<Row> origin) {
        this.origin = origin;
    }

    @Override
    public boolean hasNext() {
        return origin.hasNext();
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Row row = origin.next();
        final Timestamp ts = row.getTimestamp(row.fieldIndex("_time"));
        if (ts == null) {
            throw new RuntimeException("Column '_time' was null, cannot convert to epoch seconds");
        }
        final long epoch = ts.toInstant().getEpochSecond();
        final long epochHour = epoch - (epoch % 3600);
        final String rawString = row.getString(row.fieldIndex("_raw"));
        final ResolvedFormat metadata = new ArchiveObjectMetadataWithFormat(rawString).toResolved();
        final String partitionString = row.getString(row.fieldIndex("partition"));
        final long id = Long.parseLong(partitionString);

        return new Object[] {
                id, epochHour, metadata.format()
        };

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
            final EpochMigrationIterator that = (EpochMigrationIterator) o;
            rv = Objects.equals(origin, that.origin);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(origin);
    }
}
