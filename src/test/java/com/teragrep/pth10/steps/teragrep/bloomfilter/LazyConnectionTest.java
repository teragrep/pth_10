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
package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.*;

import java.net.ConnectException;
import java.sql.*;
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
        Config config = ConfigFactory.parseProperties(defaultProperties());
        LazyConnection lazyconnection = new LazyConnection(config);
        Connection connection1 = lazyconnection.get();
        Connection connection2 = lazyconnection.get();
        Assertions.assertDoesNotThrow(connection1::close);
        boolean isClosed = Assertions.assertDoesNotThrow(connection2::isClosed);
        Assertions.assertTrue(isClosed);
    }

    @Test
    public void testValidConnectionWithDefaultProperties() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        LazyConnection lazyconnection = new LazyConnection(config);
        Connection connection = lazyconnection.get();
        boolean isValid = Assertions.assertDoesNotThrow(() -> connection.isValid(10));
        Assertions.assertTrue(isValid);
    }

    private Properties defaultProperties() {
        Properties properties = new Properties();
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
