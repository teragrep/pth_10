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
package com.teragrep.pth_10;

import com.teragrep.pth_10.steps.teragrep.migrate.EpochMigrationBatch;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.jetbrains.annotations.NotNull;
import org.jooq.BatchBindStep;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public final class EpochMigrationBatchTest {

    @Test
    public void testNewBatchHasNoAcceptedRows() {
        final EpochMigrationBatch batch = new EpochMigrationBatch(dummyBatch(), 5);
        Assertions.assertFalse(batch.hasPendingRows());
        Assertions.assertEquals(0, batch.acceptedRows());
        Assertions.assertFalse(batch.shouldFlushRows());
    }

    @Test
    public void testShouldFlush() {
        final EpochMigrationBatch batch = new EpochMigrationBatch(dummyBatch(), 2, 2, 0);
        Assertions.assertTrue(batch.shouldFlushRows());
    }

    @Test
    public void testHasPendingRows() {
        final EpochMigrationBatch batch = new EpochMigrationBatch(dummyBatch(), 10, 1, 0);
        Assertions.assertTrue(batch.hasPendingRows());
    }

    @Test
    public void testResetClearsBatchCountButKeepsAcceptedRows() {
        final EpochMigrationBatch batch = new EpochMigrationBatch(dummyBatch(), 10, 3, 7);
        final EpochMigrationBatch reset = batch.reset(dummyBatch());
        Assertions.assertFalse(reset.hasPendingRows());
        Assertions.assertEquals(7, reset.acceptedRows());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(EpochMigrationBatch.class).verify();
    }

    // never accessed
    private BatchBindStep dummyBatch() {
        return new BatchBindStep() {

            @Override
            public @NotNull BatchBindStep bind(Object ... objects) {
                throw new UnsupportedOperationException();
            }

            @Override
            public @NotNull BatchBindStep bind(Object[] ... objects) {
                throw new UnsupportedOperationException();
            }

            @Override
            public @NotNull BatchBindStep bind(Map<String, Object> map) {
                throw new UnsupportedOperationException();
            }

            @SafeVarargs
            @Override
            public final @NotNull BatchBindStep bind(Map<String, Object> ... maps) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int @NotNull [] execute() {
                throw new UnsupportedOperationException();
            }

            @Override
            public @NotNull CompletionStage<int[]> executeAsync() {
                throw new UnsupportedOperationException();
            }

            @Override
            public @NotNull CompletionStage<int[]> executeAsync(Executor executor) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int size() {
                return 0;
            }
        };
    }
}
