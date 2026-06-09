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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class EpochMigrationIteratorTest {

    private final StructType simplifiedSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build())
    });

    @Test
    public void testMapping() {
        final Instant instant = Instant.parse("2023-09-05T09:00:00Z");
        final Timestamp timestamp = Timestamp.from(instant);
        final String raw = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"path/to/file.gz\",\"partition\":\"part1\"},\"timestamp\":{\"rfc5424timestamp\":\"2023-09-05T09:00:00Z\",\"epoch\":1693904400,\"path-extracted\":\"2023-09-05T09:00:00Z\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
        final Row row = row(timestamp, raw, "12345");
        final List<Row> rowList = Collections.singletonList(row);
        final EpochMigrationIterator epochMigrationIterator = new EpochMigrationIterator(rowList.iterator());
        Assertions.assertTrue(epochMigrationIterator.hasNext());
        final Object[] result = Assertions.assertDoesNotThrow(epochMigrationIterator::next);
        Assertions.assertEquals(12345L, result[0]);
        Assertions.assertEquals(instant.getEpochSecond(), result[1]);
        Assertions.assertEquals("rfc5424", result[2]);
        Assertions.assertFalse(epochMigrationIterator.hasNext());
    }

    @Test
    public void testNoSuchElementException() {
        final Iterator<Row> emptyIterator = Collections.emptyIterator();
        final EpochMigrationIterator epochMigrationIterator = new EpochMigrationIterator(emptyIterator);
        Assertions.assertFalse(epochMigrationIterator.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, epochMigrationIterator::next);
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EpochMigrationIterator.class).verify();
    }

    private Row row(final Timestamp time, final String raw, final String partition) {
        return new GenericRowWithSchema(new Object[] {
                time, raw, partition
        }, simplifiedSchema);
    }
}
