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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class LazyConnectionSourceTest {

    private final String username = "testuser";
    private final String password = "testpass";
    private final String url = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";

    @Test
    public void testInitialization() {
        final Config config = ConfigFactory.parseProperties(defaultProperties());
        final ConnectionSource source = new LazyConnectionSource(config);
        Assertions.assertDoesNotThrow(() -> {
            try (Connection conn = source.connection()) {
                Assertions.assertDoesNotThrow(conn::close);
                final boolean isConn1Closed = Assertions.assertDoesNotThrow(conn::isClosed);
                Assertions.assertTrue(isConn1Closed);
            }
        });
        Assertions.assertDoesNotThrow(source::closeSource);
    }

    @Test
    public void testReturnsDistinctConnections() {
        final Config config = ConfigFactory.parseProperties(defaultProperties());
        final LazyConnectionSource source = new LazyConnectionSource(config);
        Assertions.assertDoesNotThrow(() -> {
            try (final Connection c1 = source.connection(); final Connection c2 = source.connection()) {
                Assertions.assertNotSame(c1, c2);
                Assertions.assertFalse(c1.isClosed());
                Assertions.assertFalse(c2.isClosed());
            }
        });
        Assertions.assertDoesNotThrow(source::closeSource);
    }

    @Test
    public void testConcurrentConnectionDoesNotFail() {
        final Config config = ConfigFactory.parseProperties(defaultProperties());
        final LazyConnectionSource source = new LazyConnectionSource(config);
        final int threads = 10;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final List<Callable<Connection>> tasks = IntStream
                .range(0, threads)
                .mapToObj(i -> (Callable<Connection>) source::connection)
                .collect(Collectors.toList());
        final List<Future<Connection>> futures = Assertions.assertDoesNotThrow(() -> executor.invokeAll(tasks));
        Assertions.assertDoesNotThrow(() -> {
            for (final Future<Connection> f : futures) {
                try (final Connection c = f.get()) {
                    Assertions.assertFalse(c.isClosed());
                }
            }
        });
        Assertions.assertDoesNotThrow(() -> {
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        });
        Assertions.assertDoesNotThrow(source::closeSource);
    }

    @Test
    public void missingUrlThrows() {
        final Properties props = defaultProperties();
        props.remove("dpl.pth_06.bloom.db.url");
        final Config config = ConfigFactory.parseProperties(props);
        final LazyConnectionSource source = new LazyConnectionSource(config);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, source::connection);
        String expected = "Missing configuration item: 'dpl.pth_06.bloom.db.url'.";
        Assertions.assertEquals(expected, exception.getMessage());
        Assertions.assertDoesNotThrow(source::closeSource);
    }

    @Test
    public void missingUsernameThrows() {
        final Properties props = defaultProperties();
        props.remove("dpl.pth_10.bloom.db.username");
        final Config config = ConfigFactory.parseProperties(props);
        final LazyConnectionSource source = new LazyConnectionSource(config);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, source::connection);
        String expected = "Missing configuration item: 'dpl.pth_10.bloom.db.username'.";
        Assertions.assertEquals(expected, exception.getMessage());
        Assertions.assertDoesNotThrow(source::closeSource);
    }

    @Test
    public void missingPasswordThrows() {
        final Properties props = defaultProperties();
        props.remove("dpl.pth_10.bloom.db.password");
        final Config config = ConfigFactory.parseProperties(props);
        final LazyConnectionSource source = new LazyConnectionSource(config);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, source::connection);
        String expected = "Missing configuration item: 'dpl.pth_10.bloom.db.password'.";
        Assertions.assertEquals(expected, exception.getMessage());
        Assertions.assertDoesNotThrow(source::closeSource);
    }

    private Properties defaultProperties() {
        final Properties properties = new Properties();
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", url);
        return properties;
    }
}
