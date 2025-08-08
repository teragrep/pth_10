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
package com.teragrep.pth10.steps.teragrep;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * teragrep get system version: Returns a dataset containing the various version numbers of the components used in
 * Teragrep
 */
public final class TeragrepSystemStep extends AbstractStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepSystemStep.class);

    private final DPLParserCatalystContext catCtx;

    public TeragrepSystemStep(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        LOGGER.debug("Calling getTeragrepVersionAsDataframe function");
        List<String> versions;
        String explainStr;

        // Default mode is brief
        // just physical plan
        explainStr = "teragrep version";
        versions = getComponentVersions();

        if (versions != null) {
            GeneratedDatasource datasource = new GeneratedDatasource(catCtx);
            try {
                dataset = datasource.constructStream(versions, explainStr);
            }
            catch (InterruptedException | UnknownHostException e) {
                throw new RuntimeException(e);
            }

        }
        else {
            // getComponentVersions() requires jar packaging
            LOGGER
                    .error(
                            "Teragrep get system version: Versions list was NULL, meaning the version properties could "
                                    + "not be fetched. This might be caused by running this command in a development environment."
                    );
        }

        return dataset;
    }

    /**
     * Gets the various Teragrep component versions
     * 
     * @return component versions as a list
     */
    private List<String> getComponentVersions() {
        final List<String> rv = new ArrayList<>();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("programmatically resolved package: <{}>", TeragrepSystemStep.class.getPackage());
            LOGGER
                    .info(
                            "programmatically resolved: <{}>",
                            TeragrepSystemStep.class.getPackage().getImplementationVersion()
                    );
        }

        try (final InputStream is = TeragrepSystemStep.class.getClassLoader().getResourceAsStream("maven.properties")) {
            if (is == null) {
                throw new IllegalStateException("InputStream was Null, Problem fetching package properties");
            }
            final Properties p = new Properties();
            p.load(is);
            LOGGER.debug("package properties: <{}>", p);

            rv.add("pth_10 version: " + p.get("revision"));

            // add all "teragrep" properties to the list
            p.stringPropertyNames().stream().forEach(property -> {
                if (property.contains("teragrep")) {
                    String[] splitProperty = property.split("\\."); // e.g. "teragrep.pth_03.version"
                    rv.add(splitProperty[1] + " version: " + p.getProperty(property));
                }
            });
        }
        catch (IOException e) {
            LOGGER.error("Failed to load InputStream: ", e);
            throw new UncheckedIOException("Failed to load InputStream: ", e);
        }
        return rv;
    }
}
