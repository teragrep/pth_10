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

import com.teragrep.functions.dpf_02.AbstractStep;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class TeragrepEpochMigrationStep extends AbstractStep {

    private final Logger LOGGER = LoggerFactory.getLogger(TeragrepEpochMigrationStep.class);
    private final Config config;
    private final String journaldbNameConfigItem;

    public TeragrepEpochMigrationStep(final Config config) {
        this.config = config;
        this.journaldbNameConfigItem = "dpl.archive.db.journaldb.name";
        this.properties.add(CommandProperty.SEQUENTIAL_ONLY);
    }

    @Override
    public Dataset<Row> get(final Dataset<Row> dataset) {
        final String journalDBName;
        if (!config.hasPath(journaldbNameConfigItem)) {
            LOGGER.info("Using default journaldb name <{}>", "journaldb");
            journalDBName = "journaldb";
        }
        else {
            journalDBName = config.getString(journaldbNameConfigItem);
            LOGGER
                    .info(
                            "Using journaldb name from config option <{}>, value <{}>", journaldbNameConfigItem,
                            journalDBName
                    );
        }
        EpochMigrationForeachPartitionFunction migrationFunction = new EpochMigrationForeachPartitionFunction(
                config,
                journalDBName
        );
        dataset.foreachPartition(migrationFunction);
        return dataset;

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
            final TeragrepEpochMigrationStep that = (TeragrepEpochMigrationStep) o;
            rv = Objects.equals(config, that.config)
                    && Objects.equals(journaldbNameConfigItem, that.journaldbNameConfigItem);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, journaldbNameConfigItem);
    }
}
