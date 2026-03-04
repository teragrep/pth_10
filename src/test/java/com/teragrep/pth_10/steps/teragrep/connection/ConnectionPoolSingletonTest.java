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
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Properties;
import java.util.UUID;

/**
 * Note that this object is static
 */
public final class ConnectionPoolSingletonTest {

    private static final String username = "testuser";
    private static final String password = "testpass";
    private static final String url = "jdbc:h2:mem:test_" + UUID.randomUUID()
            + ";MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";

    @BeforeEach
    public void reset() {
        ConnectionPoolSingleton.resetForTest();
    }

    @Test
    public void testInitializesOnFirstCall() {
        final Config config = defaultConfig();
        final Connection connection = Assertions.assertDoesNotThrow(() -> ConnectionPoolSingleton.connection(config));
        Assertions.assertNotNull(connection);
        Assertions.assertDoesNotThrow(connection::close);
    }

    @Test
    public void testAllowsSameConfigTwice() {
        final Config config = defaultConfig();
        final Connection c1 = Assertions.assertDoesNotThrow(() -> ConnectionPoolSingleton.connection(config));
        final Connection c2 = Assertions.assertDoesNotThrow(() -> ConnectionPoolSingleton.connection(config));
        Assertions.assertNotNull(c1);
        Assertions.assertNotNull(c2);
        Assertions.assertDoesNotThrow(c1::close);
        Assertions.assertDoesNotThrow(c2::close);
    }

    @Test
    public void throwsIfReinitializedWithDifferentConfig() {
        final Config config1 = defaultConfig();
        final Config config2 = ConfigFactory.parseProperties(new Properties());
        Assertions.assertNotEquals(config1, config2);
        Assertions.assertDoesNotThrow(() -> {
            try (final Connection connection = ConnectionPoolSingleton.connection(config1)) {
                Assertions.assertFalse(connection.isClosed());
            }
        });
        final IllegalStateException exception = Assertions
                .assertThrows(IllegalStateException.class, () -> ConnectionPoolSingleton.connection(config2));
        final String expected = "Datasource was already initialized with a different config";
        Assertions.assertEquals(expected, exception.getMessage());
    }

    private static Config defaultConfig() {
        final Properties properties = new Properties();
        properties.put("dpl.pth_06.archive.db.username", username);
        properties.put("dpl.pth_06.archive.db.password", password);
        properties.put("dpl.pth_06.archive.db.url", url);
        return ConfigFactory.parseProperties(properties);
    }
}
