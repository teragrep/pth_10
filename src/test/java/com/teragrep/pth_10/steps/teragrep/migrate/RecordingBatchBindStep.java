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

import org.jetbrains.annotations.NotNull;
import org.jooq.BatchBindStep;
import org.jooq.exception.DataAccessException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

final class RecordingBatchBindStep implements BatchBindStep {

    private final BatchBindStep origin;
    private final List<Object> bindValuesList;

    RecordingBatchBindStep(final BatchBindStep origin) {
        this(origin, new ArrayList<>());
    }

    RecordingBatchBindStep(final BatchBindStep origin, final List<Object> bindValuesList) {
        this.origin = origin;
        this.bindValuesList = bindValuesList;
    }

    List<Object> boundValuesList() {
        return bindValuesList;
    }

    @Override
    public @NotNull BatchBindStep bind(Object ... bindValues) {
        bindValuesList.addAll(List.of(bindValues));
        BatchBindStep next = origin.bind(bindValues);
        return new RecordingBatchBindStep(next, bindValuesList);
    }

    @Override
    public @NotNull BatchBindStep bind(Object[] ... bindValues) {
        bindValuesList.addAll(List.of(bindValues));
        BatchBindStep next = origin.bind(bindValues);
        return new RecordingBatchBindStep(next, bindValuesList);
    }

    @Override
    public @NotNull BatchBindStep bind(Map<String, Object> namedBindValues) {
        bindValuesList.addAll(List.of(namedBindValues));
        BatchBindStep next = origin.bind(namedBindValues);
        return new RecordingBatchBindStep(next, bindValuesList);
    }

    @Override
    public @NotNull BatchBindStep bind(Map<String, Object> ... namedBindValues) {
        bindValuesList.addAll(List.of(namedBindValues));
        BatchBindStep next = origin.bind(namedBindValues);
        return new RecordingBatchBindStep(next, bindValuesList);
    }

    @Override
    public @NotNull int[] execute() throws DataAccessException {
        return new RecordingBatchBindStep(origin, bindValuesList).execute();
    }

    @Override
    public @NotNull CompletionStage<int[]> executeAsync() {
        return new RecordingBatchBindStep(origin, bindValuesList).executeAsync();
    }

    @Override
    public @NotNull CompletionStage<int[]> executeAsync(Executor executor) {
        return new RecordingBatchBindStep(origin, bindValuesList).executeAsync(executor);
    }

    @Override
    public int size() {
        return origin.size();
    }

    @Override
    public String toString() {
        return "Bound values <" + bindValuesList + ">";
    }
}
