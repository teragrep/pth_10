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
package com.teragrep.pth_10.steps.teragrep.connection;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides a JVM shared HikariDatasource for a spark executor
 */
public final class ExecutorDataSourceRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorDataSourceRegistry.class);
    private static HikariDataSource dataSource;
    private static Config initializedWith;

    private ExecutorDataSourceRegistry() {
        // blocks initialization
    }

    /**
     * Initializes HikariDatasource if not yet created and provides a connection from the pool
     *
     * @param config config to set HikariCP values, once a connection is build once the config cannot change
     * @return Connection from the pool
     * @throws SQLException thrown if there is a problem getting a connection
     */
    public static synchronized Connection connection(final Config config) throws SQLException {
        LOGGER.debug("thread entered lock block");
        if (dataSource == null) {
            dataSource = new DataSourceFromConfig(config).get();
            initializedWith = config;
        }
        else if (!initializedWith.equals(config)) {
            throw new IllegalStateException("Datasource was already initialized with a different configuration");
        }
        return dataSource.getConnection();
    }

    // only for testing
    static synchronized void resetForTest() {
        LOGGER.warn("resetForTest() called, this should only happen in a test case");
        if (dataSource != null) {
            dataSource.close();
        }
        dataSource = null;
        initializedWith = null;
    }
}
