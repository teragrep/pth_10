/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TeragrepInfoOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepHdfsOperations.class);

    /**
     * Returns a dataset containing the various version numbers of the components used in Teragrep
     * @return CatalystNode with a dataset containing the version numbers, or incoming dataset if versions could not be found
     */
    static Dataset<Row> getTeragrepVersionAsDataframe(Dataset<Row> ds, DPLParserCatalystContext catCtx) {
        LOGGER.info("Calling getTeragrepVersionAsDataframe function");
        List<String> versions;
        String explainStr;

        // Default mode is brief
        // just physical plan
        explainStr = "teragrep version";
        versions = getComponentVersions();

        if (versions != null) {
            GeneratedDatasource datasource = new GeneratedDatasource(catCtx);
            try {
                ds = datasource.constructStream(versions, explainStr);
            } catch (StreamingQueryException | InterruptedException | UnknownHostException e) {
                e.printStackTrace();
            }

        }
        else {
            // getComponentVersions() requires jar packaging
            LOGGER.error("Teragrep get system version: Versions list was NULL, meaning the version properties could " +
                    "not be fetched. This might be caused by running this command in a development environment.");
        }

        return ds;
    }

    /**
     * Gets the various Teragrep component versions
     * @return component versions as a list
     */
    private static List<String> getComponentVersions() {
        List<String> rv = null;

        LOGGER.info("programmatically resolved package: " + TeragrepInfoOperations.class.getPackage());
        LOGGER.info("programmatically resolved: " + TeragrepInfoOperations.class.getPackage().getImplementationVersion());

        java.io.InputStream is = TeragrepInfoOperations.class.getClassLoader().getResourceAsStream("maven.properties");
        java.util.Properties p = new Properties();
        try {
            p.load(is);
            LOGGER.info("package properties: " + p);

            rv = new ArrayList<>();
            rv.add("pth_10 version: " + (String)p.get("revision"));
            rv.add("pth_03 version: " + (String)p.get("teragrep.pth_03.version"));
            rv.add("jue_01 version: " + (String)p.get("teragrep.jue_01.version"));
            rv.add("rlo_06 version: " + (String)p.get("teragrep.rlo_06.version"));
            rv.add("pth_06 version: " + (String)p.get("teragrep.pth_06.version"));
            rv.add("dpf_02 version: " + (String)p.get("teragrep.dpf_02.version"));
            rv.add("dpf_03 version: " + (String)p.get("teragrep.dpf_03.version"));
            rv.add("jpr_01 version: " + (String)p.get("teragrep.jpr_01.version"));
        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
        }
        return rv;
    }

}
