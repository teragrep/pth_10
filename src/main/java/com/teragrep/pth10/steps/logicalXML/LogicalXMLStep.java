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
package com.teragrep.pth10.steps.logicalXML;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.datasources.ArchiveQuery;
import com.teragrep.pth10.datasources.DPLDatasource;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Step for Dataset generation. Utilizes archiveQuery gotten from parse tree to get the initial dataset from Datasource.
 */
public final class LogicalXMLStep extends AbstractStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalXMLStep.class);
    private final ArchiveQuery archiveQuery;
    private final DPLParserCatalystContext catCtx;
    private final boolean isMetadataQuery;

    public LogicalXMLStep(ArchiveQuery archiveQuery, DPLParserCatalystContext catCtx) {
        super();
        this.archiveQuery = archiveQuery;
        this.catCtx = catCtx;
        this.isMetadataQuery = false;
    }

    public LogicalXMLStep(ArchiveQuery archiveQuery, DPLParserCatalystContext catCtx, boolean isMetadataQuery) {
        super();
        this.archiveQuery = archiveQuery;
        this.catCtx = catCtx;
        this.isMetadataQuery = isMetadataQuery;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        Dataset<Row> ds;

        if (this.catCtx != null && this.catCtx.getConfig() != null && !this.catCtx.getTestingMode()) {
            // Perform archive query
            if (!this.archiveQuery.isStub) {
                LOGGER
                        .info(
                                "Constructing data stream with query=<{}> metadata=<{}>", this.archiveQuery,
                                isMetadataQuery
                        );
                DPLDatasource datasource = new DPLDatasource(catCtx);
                ds = datasource.constructStreams(this.archiveQuery, isMetadataQuery);
                LOGGER.info("Received dataset with columns: <{}>", Arrays.toString(ds.columns()));
            }
            else {
                LOGGER.info("Archive query object was a stub! Generating empty streaming dataset.");
                ds = new GeneratedDatasource(catCtx).constructEmptyStream();
            }
        }
        else {
            // Testing mode?
            if (catCtx != null && catCtx.getDs() != null && !archiveQuery.isStub) {
                ds = catCtx.getDs();
            }
            else if (catCtx != null && archiveQuery.isStub) {
                // generate empty dataset even in testing mode if no archive query was generated
                ds = new GeneratedDatasource(catCtx).constructEmptyStream();
            }
            else {
                throw new RuntimeException("CatCtx didn't have a config and it's dataset is null as well!");
            }
        }

        return ds;
    }
}
