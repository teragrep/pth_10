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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

final class DataSourceFromConfig implements Supplier<HikariDataSource> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceFromConfig.class);

    private final String sourceName;
    private final Config config;

    DataSourceFromConfig(final Config config) {
        this("pth_10-pool-" + UUID.randomUUID(), config);
    }

    DataSourceFromConfig(final String sourceName, final Config config) {
        this.sourceName = sourceName;
        this.config = config;
    }

    @Override
    public HikariDataSource get() {
        final HikariConfig hikariConfig = new HikariConfig();
        // credentials
        final String url = connectionURL();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(connectionUsername());
        hikariConfig.setPassword(connectionPassword());
        // pool configuration
        if (url.startsWith("jdbc:mariadb:")) {
            hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        }
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(256); // limit for safety, should not be reached
        hikariConfig.setMinimumIdle(0); // no hanging connections inside executor
        hikariConfig.setAutoCommit(true);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setValidationTimeout(5000);
        hikariConfig.setPoolName(sourceName);
        return new HikariDataSource(hikariConfig);
    }

    private String connectionUsername() {

        final String archiveUsernameConfigurationItem = "dpl.pth_10.archive.db.username";

        final String BLOOMDB_USERNAME_CONFIG_ITEM = "dpl.pth_10.bloom.db.username";

        final String username;
        if (config.hasPath(archiveUsernameConfigurationItem)) {
            username = config.getString(archiveUsernameConfigurationItem);

        }
        else if (config.hasPath(BLOOMDB_USERNAME_CONFIG_ITEM)) {
            LOGGER
                    .warn(
                            "Using deprecated configuration item <{}>, use <{}> instead", BLOOMDB_USERNAME_CONFIG_ITEM,
                            archiveUsernameConfigurationItem
                    );
            username = config.getString(BLOOMDB_USERNAME_CONFIG_ITEM);
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + archiveUsernameConfigurationItem + "'.");
        }

        if (username == null || username.isEmpty()) {
            throw new RuntimeException("Database username not set.");
        }

        return username;
    }

    private String connectionPassword() {

        final String archivePasswordConfigurationItem = "dpl.pth_10.archive.db.password";

        final String BLOOMDB_PASSWORD_CONFIG_ITEM = "dpl.pth_10.bloom.db.password";

        final String password;
        if (config.hasPath(archivePasswordConfigurationItem)) {
            password = config.getString(archivePasswordConfigurationItem);

        }
        else if (config.hasPath(BLOOMDB_PASSWORD_CONFIG_ITEM)) {
            LOGGER
                    .warn(
                            "Using deprecated configuration item <{}>, use <{}> instead", BLOOMDB_PASSWORD_CONFIG_ITEM,
                            archivePasswordConfigurationItem
                    );
            password = config.getString(BLOOMDB_PASSWORD_CONFIG_ITEM);
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + archivePasswordConfigurationItem + "'.");
        }

        if (password == null) {
            throw new RuntimeException("Database password not set.");
        }

        return password;
    }

    private String connectionURL() {

        final String archiveDBConfigurationItem = "dpl.pth_10.archive.db.url";

        final String BLOOMDB_URL_CONFIG_ITEM = "dpl.pth_06.bloom.db.url";

        final String databaseUrl;
        if (config.hasPath(archiveDBConfigurationItem)) {
            databaseUrl = config.getString(archiveDBConfigurationItem);
        }
        else if (config.hasPath(BLOOMDB_URL_CONFIG_ITEM)) {
            LOGGER
                    .warn(
                            "Using deprecated configuration item <{}>, use <{}> instead", BLOOMDB_URL_CONFIG_ITEM,
                            archiveDBConfigurationItem
                    );
            databaseUrl = config.getString(BLOOMDB_URL_CONFIG_ITEM);
        }
        else {
            throw new RuntimeException("Missing configuration item: '" + archiveDBConfigurationItem + "'.");
        }

        if (databaseUrl == null || databaseUrl.isEmpty()) {
            throw new RuntimeException("Database url not set.");
        }

        return databaseUrl;
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
            final DataSourceFromConfig that = (DataSourceFromConfig) o;
            rv = Objects.equals(sourceName, that.sourceName) && Objects.equals(config, that.config);
        }
        return rv;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceName, config);
    }
}
