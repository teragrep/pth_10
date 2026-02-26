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
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;

public final class EpochMigrationBatchStateTest {

    private final DSLContext ctx = DSL.using(SQLDialect.MYSQL);
    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });

    @Test
    public void testAcceptsValidRow() {
        final EpochMigrationBatchState batchState = new EpochMigrationBatchState(baseBatch(ctx), 1);
        final EpochMigrationBatchState nextBatchState = Assertions
                .assertDoesNotThrow(() -> batchState.accept(genericResultRow()));
        Assertions.assertTrue(nextBatchState.isFull());
        Assertions.assertTrue(nextBatchState.hasPendingRows());
        Assertions.assertEquals(1, nextBatchState.totalAccepted());
    }

    @Test
    public void testBatchBoundValues() {
        final EpochMigrationBatchState batchState = new EpochMigrationBatchState(
                new RecordingBatchBindStep(baseBatch(ctx)),
                1
        );
        Assertions.assertEquals(0, batchState.batch().size());
        final EpochMigrationBatchState nextBatchState = Assertions
                .assertDoesNotThrow(() -> batchState.accept(genericResultRow()));
        RecordingBatchBindStep step = (RecordingBatchBindStep) nextBatchState.batch();
        Assertions.assertIterableEquals(Arrays.asList(1580551994L, 1L), step.boundValuesList());
    }

    @Test
    public void testTotalAcceptedRowsIsIncremented() {
        final EpochMigrationBatchState startState = new EpochMigrationBatchState(baseBatch(ctx), 1);
        Assertions.assertEquals(0, startState.totalAccepted());
        final EpochMigrationBatchState firstState = Assertions
                .assertDoesNotThrow(() -> startState.accept(genericResultRow()));
        Assertions.assertEquals(1, firstState.totalAccepted());
        final EpochMigrationBatchState secondState = Assertions
                .assertDoesNotThrow(() -> firstState.accept(genericResultRow()));
        Assertions.assertEquals(2, secondState.totalAccepted());
    }

    @Test
    public void testBatchIsFull() {
        final EpochMigrationBatchState startState = new EpochMigrationBatchState(baseBatch(ctx), 2);
        final EpochMigrationBatchState firstState = Assertions
                .assertDoesNotThrow(() -> startState.accept(genericResultRow()));
        Assertions.assertFalse(firstState.isFull());
        final EpochMigrationBatchState secondState = Assertions
                .assertDoesNotThrow(() -> firstState.accept(genericResultRow()));
        Assertions.assertTrue(secondState.isFull());
    }

    @Test
    public void testResetDoesNotAffectTotalAccepted() {
        final BatchBindStep baseBatch = baseBatch(ctx);
        final EpochMigrationBatchState startState = new EpochMigrationBatchState(baseBatch, 1);
        final EpochMigrationBatchState firstState = Assertions
                .assertDoesNotThrow(() -> startState.accept(genericResultRow()));
        final EpochMigrationBatchState resetBatch = firstState.reset(baseBatch);
        Assertions.assertNotEquals(startState.hasPendingRows(), firstState.hasPendingRows());
        Assertions.assertNotEquals(startState.isFull(), firstState.isFull());
        Assertions.assertEquals(startState.hasPendingRows(), resetBatch.hasPendingRows());
        Assertions.assertEquals(startState.isFull(), resetBatch.isFull());
        Assertions.assertEquals(1, resetBatch.totalAccepted());
    }

    private BatchBindStep baseBatch(final DSLContext ctx) {
        final Field<Long> epochField = DSL.field(DSL.name("epoch_hour"), Long.class);
        final Field<Long> idField = DSL.field(DSL.name("id"), Long.class);
        final Param<Long> epochParam = DSL.param("epoch_hour", Long.class);
        final Param<Long> idParam = DSL.param("id", Long.class);
        final Query baseQuery = ctx
                .update(DSL.table(DSL.name("journaldb", "logfile")))
                .set(epochField, epochParam)
                .where(idField.eq(idParam).and(epochField.isNull()));
        return ctx.batch(baseQuery);
    }

    private Row genericResultRow() {
        final String syslogJsonResult = "{\"epochMigration\":true,\"format\":\"rfc5424\",\"object\":{\"bucket\":\"bucket\",\"path\":\"2007/10-08/epoch/migration/test.logGLOB-2007100814.log.gz\",\"partition\":\"id\"},\"timestamp\":{\"rfc5242timestamp\":\"2014-06-20T09:14:07.12345+00:00\",\"epoch\":1403255647123450,\"path-extracted\":\"2007-10-08T14:00+03:00[Europe/Helsinki]\",\"path-extracted-precision\":\"hourly\",\"source\":\"syslog\"}}";
        return new GenericRowWithSchema(new Object[] {
                Timestamp.valueOf("2020-02-01 12:13:14"),
                syslogJsonResult,
                "index",
                "sourcetype",
                "host",
                "source",
                "1",
                1L
        }, testSchema);
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EpochMigrationBatchState.class).verify();
    }
}
