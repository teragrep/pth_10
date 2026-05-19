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

import org.jooq.BatchBindStep;
import org.jooq.CreateTableColumnStep;
import org.jooq.DSLContext;
import org.jooq.DropTableStep;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Param;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

final class EpochMigrationTempTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EpochMigrationTempTable.class);

    private final DSLContext ctx;
    private final String journalDBName;
    private final String tableName;
    private final Field<Long> logfileIdField;
    private final Field<Long> epochHourField;
    private final Field<String> objectFormatField;

    EpochMigrationTempTable(final DSLContext ctx) {
        this(ctx, "journaldb");
    }

    EpochMigrationTempTable(final DSLContext ctx, final String journalDBName) {
        this(ctx, journalDBName, "epoch_migration_temp_table");
    }

    private EpochMigrationTempTable(final DSLContext ctx, final String journalDBName, final String tableName) {
        this(
                ctx,
                journalDBName,
                tableName,
                DSL.field("logfile_id", SQLDataType.BIGINT.nullable(false)),
                DSL.field("epoch_hour", SQLDataType.BIGINT.nullable(false)),
                DSL.field("object_format", SQLDataType.VARCHAR(255).nullable(false))
        );
    }

    private EpochMigrationTempTable(
            final DSLContext ctx,
            final String journalDBName,
            final String tableName,
            final Field<Long> logfileIdField,
            final Field<Long> epochHourField,
            final Field<String> objectFormatField
    ) {
        this.ctx = ctx;
        this.journalDBName = journalDBName;
        this.tableName = tableName;
        this.logfileIdField = logfileIdField;
        this.epochHourField = epochHourField;
        this.objectFormatField = objectFormatField;
    }

    /**
     * Batch step for inserting values into epoch migration temp table
     * 
     * @return BatchBindStep for inserting logfile_id, epoch_hour and object_format into the epoch_migration_temp_table
     */
    BatchBindStep insertBatch() {
        final Param<Long> idParam = DSL.param("logfile_id", Long.class);
        final Param<Long> epochParam = DSL.param("epoch_hour", Long.class);
        final Field<String> objectFormatParam = DSL.param("object_format", String.class);
        final Query baseQuery = ctx
                .insertInto(DSL.table(DSL.name(journalDBName, tableName)))
                .set(logfileIdField, idParam)
                .set(epochHourField, epochParam)
                .set(objectFormatField, objectFormatParam);
        return ctx.batch(baseQuery);
    }

    void create() {
        final Name qualifiedName = DSL.name(journalDBName, tableName);
        try (final DropTableStep dropTableStep = ctx.dropTemporaryTableIfExists(qualifiedName)) {
            dropTableStep.execute();
        }
        try (
                final CreateTableColumnStep createTempTableStep = ctx.createTemporaryTableIfNotExists(qualifiedName).column(logfileIdField).column(epochHourField).column(objectFormatField)
        ) {
            LOGGER.debug("Creating epoch migration temp table <{}>", createTempTableStep);
            createTempTableStep.execute();
        }
    }

    void callMigrationProcedure() {
        LOGGER.info("Calling epoch migration procedure");
        ctx.execute("CALL epochMigrationProcedure()");
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
            final EpochMigrationTempTable that = (EpochMigrationTempTable) o;
            rv = Objects.equals(ctx, that.ctx) && Objects.equals(journalDBName, that.journalDBName)
                    && Objects.equals(tableName, that.tableName) && Objects.equals(logfileIdField, that.logfileIdField) && Objects.equals(epochHourField, that.epochHourField) && Objects.equals(objectFormatField, that.objectFormatField);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, journalDBName, tableName, logfileIdField, epochHourField, objectFormatField);
    }
}
