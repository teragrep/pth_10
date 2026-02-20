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
 * Test object to provide a thread safe, non-static version of a ConnectionSource if required for unit testing, avoids
 * concurrency issues. Can be closed and opened but a new instance is recommended.
 */
public final class TestingConnectionSource implements ConnectionSource {

    private final Logger LOGGER = LoggerFactory.getLogger(TestingConnectionSource.class);
    private HikariDataSource dataSource;
    private final Config config;

    public TestingConnectionSource(final Config config) {
        this.config = config;
    }

    @Override
    public Connection get() {
        LOGGER.debug("connection() called");
        final Connection connection;
        try {
            connection = source().getConnection();
        }
        catch (final SQLException e) {
            throw new RuntimeException("Error getting connection from source: " + e.getMessage(), e);
        }
        return connection;
    }

    public synchronized boolean isInitialized() {
        return dataSource != null;
    }

    private synchronized HikariDataSource source() {
        if (dataSource == null) {
            dataSource = new DataSourceFromConfig(config).get();
            LOGGER.debug("datasource initialized");
        }
        return dataSource;
    }

    @Override
    public synchronized void close() {
        LOGGER.debug("close() called");
        if (dataSource != null) {
            LOGGER.debug("Closing datasource");
            dataSource.close();
            dataSource = null;
        }
    }
}
