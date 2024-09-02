/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022, 2023  Suomen Kanuuna Oy
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

package com.teragrep.pth10.steps.teragrep.bloomfilter;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.typesafe.config.Config;


import static com.teragrep.pth10.steps.teragrep.TeragrepBloomStep.BLOOMDB_URL_CONFIG_ITEM;
import static com.teragrep.pth10.steps.teragrep.TeragrepBloomStep.BLOOMDB_USERNAME_CONFIG_ITEM;
import static com.teragrep.pth10.steps.teragrep.TeragrepBloomStep.BLOOMDB_PASSWORD_CONFIG_ITEM;

public class LazyConnection implements Serializable {
    private static Connection connection = null;
    private final Config config;

    public LazyConnection(Config config) {
        this.config = config;
    }

    public synchronized Connection get() {
        if (connection == null) {
            // lazy init
            String connectionURL = connectionURL();
            String username = connectionUsername();
            String password= connectionPassword();

            try {
                connection = DriverManager.getConnection(
                        connectionURL,
                        username,
                        password
                );

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return connection;
    }

    private String connectionUsername() {

        String username;
        if (config.hasPath(BLOOMDB_USERNAME_CONFIG_ITEM)) {
            username = config.getString(BLOOMDB_USERNAME_CONFIG_ITEM);
            if (username == null || username.isEmpty()) {
                throw new RuntimeException("Database username not set.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + BLOOMDB_USERNAME_CONFIG_ITEM + "'.");
        }
        return username;
    }

    private String connectionPassword() {

        String password;
        if (config.hasPath(BLOOMDB_PASSWORD_CONFIG_ITEM)) {
            password = config.getString(BLOOMDB_PASSWORD_CONFIG_ITEM);
            if (password == null) {
                throw new RuntimeException("Database password not set.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + BLOOMDB_PASSWORD_CONFIG_ITEM + "'.");
        }
        return password;
    }

    private String connectionURL() {

        String databaseUrl;
        if (config.hasPath(BLOOMDB_URL_CONFIG_ITEM)) {
            databaseUrl = config.getString(BLOOMDB_URL_CONFIG_ITEM);
            if (databaseUrl == null || databaseUrl.isEmpty()) {
                throw new RuntimeException("Database url not set.");
            }
        } else {
            throw new RuntimeException("Missing configuration item: '" + BLOOMDB_URL_CONFIG_ITEM + "'.");
        }
        return databaseUrl;
    }
}
