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

public final class LazyHikariConnectionSource implements ConnectionSource {

    private final static Logger LOGGER = LoggerFactory.getLogger(LazyHikariConnectionSource.class);
    private static HikariDataSource DATA_SOURCE = null; // null used for lazy init
    private final Config config;

    public LazyHikariConnectionSource(final Config config) {
        this.config = config;
    }

    /**
     * Thread safe initialization of datasource if not initialized yet and subsequent connection
     *
     * @return Connection from the datasource
     * @throws SQLException when cannot get connection from datasource
     */
    @Override
    public synchronized Connection get() throws SQLException {
        if (DATA_SOURCE == null) {
            DATA_SOURCE = new HikariDataSourceFromConfig(config).dataSource();
            LOGGER.info("HikariDataSource initialized");
        }
        LOGGER.debug("getting new connection from datasource");
        return DATA_SOURCE.getConnection();
    }

    /**
     * Executors should only close connections not the executor,
     */
    @Override
    public void close() {
        if (DATA_SOURCE != null) {
            DATA_SOURCE.close();
            DATA_SOURCE = null;
        }
        LOGGER.info("HikariDataSource closed");
    }
}
