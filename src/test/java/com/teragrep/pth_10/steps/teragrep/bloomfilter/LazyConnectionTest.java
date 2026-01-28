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
package com.teragrep.pth_10.steps.teragrep.bloomfilter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Properties;

public class LazyConnectionTest {

    final String username = "sa";
    final String password = "";
    final String connectionUrl = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";

    @Test
    public void testEqualsVerifier() {
        EqualsVerifier.forClass(LazyConnection.class).withNonnullFields("config").verify();
    }

    @Test
    public void testOnlyOneConnectionInstance() {
        final Config config = ConfigFactory.parseProperties(defaultProperties());
        final LazyConnection lazyConnection = new LazyConnection(config);
        final Connection connection1 = lazyConnection.get();
        final Connection connection2 = lazyConnection.get();
        Assertions.assertSame(connection1, connection2);
    }

    @Test
    public void testValidConnectionWithDefaultProperties() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        final LazyConnection lazyConnection = new LazyConnection(config);
        final Connection connection = lazyConnection.get();
        final boolean isValid = Assertions.assertDoesNotThrow(() -> connection.isValid(10));
        Assertions.assertTrue(isValid);
    }

    @Test
    @Disabled(value = "Test disabled: pth_10 issue #793")
    public void testGetClosedConnection() {
        final Config config = ConfigFactory.parseProperties(defaultProperties());
        final LazyConnection lazyConnection = new LazyConnection(config);
        final Connection conn1 = lazyConnection.get();
        Assertions.assertDoesNotThrow(conn1::close);
        final Connection conn2 = lazyConnection.get();
        final boolean isClosed = Assertions.assertDoesNotThrow(conn2::isClosed);
        Assertions.assertFalse(isClosed);
    }

    private Properties defaultProperties() {
        final Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.02},"
                                + "{expected: 3000, fpp: 0.03}" + "]"
                );
        return properties;
    }
}
