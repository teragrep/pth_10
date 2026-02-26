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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides Connection objects from a static HikariCP datasource.
 * <p>
 * Methods connection() and resetForTesting() are thread locked on the class level
 */
public final class ConnectionPoolSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPoolSingleton.class);
    private static DataSourceState state = new StubDataSourceState();

    private ConnectionPoolSingleton() {
        // blocks accidental initialization
    }

    /**
     * Gets a Connection instance using a given config to instantiate a static connection pool.
     *
     * @param config config that is used to configure the connection pool, cannot change after initialization
     * @return Connection instance form the pool
     * @throws SQLException          if there is an exception getting an SQL connection from the pool
     * @throws IllegalStateException if the config is changed after initialization
     */
    public static synchronized Connection connection(final Config config) throws SQLException, IllegalStateException {
        LOGGER.debug("thread entered lock block");
        if (state.isStub()) {
            state = new InitializedDataSourceState(config);
        }
        else if (!state.config().equals(config)) {
            throw new IllegalStateException("Datasource was already initialized with a different config");
        }
        return state.dataSource().getConnection();
    }

    // only for testing
    public static synchronized void resetForTest() {
        LOGGER.warn("resetForTest() called, this should only happen in a test case");
        if (!state.isStub()) {
            state.dataSource().close();
        }
        state = new StubDataSourceState();
    }
}
