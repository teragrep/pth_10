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

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class ResolvedObjectFormats {

    private final DSLContext ctx;
    private final String journalDBName;
    // cache to hold the ID values for formats that can be then used again after fetching them from database
    private final Map<String, Long> resolvedObjectFormatsCache;

    ResolvedObjectFormats(final DSLContext ctx) {
        this(ctx, "journaldb", new ConcurrentHashMap<>());
    }

    ResolvedObjectFormats(final DSLContext ctx, final String journalDBName) {
        this(ctx, journalDBName, new ConcurrentHashMap<>());
    }

    ResolvedObjectFormats(
            final DSLContext ctx,
            final String journalDBName,
            final Map<String, Long> resolvedObjectFormatsCache
    ) {
        this.ctx = ctx;
        this.journalDBName = journalDBName;
        this.resolvedObjectFormatsCache = resolvedObjectFormatsCache;
    }

    long resolve(final String objectFormat) {
        return resolvedObjectFormatsCache.computeIfAbsent(objectFormat, this::resolveFromDatabase);
    }

    private long resolveFromDatabase(final String objectFormat) {
        try {
            // plain SQL since jooq can't insert into a generic table
            ctx
                    .execute(
                            " INSERT INTO " + journalDBName
                                    + ".object_format (name)VALUES (?) ON DUPLICATE KEY UPDATE name = VALUES(name)",
                            DSL.val(objectFormat)
                    );
        }
        catch (final DataAccessException e) {
            // we ignore duplicate key errors here
            // this is a workaround since the .onDuplicateKeyIgnoe() method doesn't work with generic tables
        }

        // get id object format
        final Table<Record> objectFormatTable = DSL.table(DSL.name(journalDBName, "object_format"));
        final Field<String> nameField = DSL
                .field(DSL.name(journalDBName, objectFormatTable.getName(), "name"), String.class);
        final Field<Long> idField = DSL.field(DSL.name(journalDBName, objectFormatTable.getName(), "id"), Long.class);
        return ctx.select(idField).from(objectFormatTable).where(nameField.eq(objectFormat)).fetchOneInto(Long.class);
    }

    @Override
    public boolean equals(final Object o) {
        // mutable internal cache ignored
        final boolean rv;
        if (o == null) {
            rv = false;
        }
        else if (getClass() != o.getClass()) {
            rv = false;
        }
        else {
            final ResolvedObjectFormats that = (ResolvedObjectFormats) o;
            rv = Objects.equals(ctx, that.ctx) && Objects.equals(journalDBName, that.journalDBName);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        // mutable internal cache ignored
        return Objects.hash(ctx, journalDBName);
    }
}
