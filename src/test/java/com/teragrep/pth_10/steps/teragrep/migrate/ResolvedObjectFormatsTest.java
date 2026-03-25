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
package com.teragrep.pth_10.steps.teragrep.migrate;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;

public final class ResolvedObjectFormatsTest {

    private Connection conn;
    private final Table<?> objectFormatTable = DSL.table(DSL.name("journaldb", "object_format"));

    @BeforeEach
    void setUp() {
        conn = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(
                                        "jdbc:h2:mem:test_" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE",
                                        "sa", ""
                                )
                );
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS journaldb").execute();
            conn
                    .prepareStatement(
                            " CREATE TABLE journaldb.object_format ( id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, UNIQUE (name)) "
                    )
                    .execute();
        });
    }

    @AfterEach
    void close() {
        Assertions.assertDoesNotThrow(conn::close);
    }

    @Test
    void shouldInsertNewObjectFormat() {
        final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
        final ResolvedObjectFormats resolved = new ResolvedObjectFormats(ctx);
        final long id = Assertions.assertDoesNotThrow(() -> resolved.resolve("rfc5452"));
        Assertions.assertTrue(id > 0);
        final int count = ctx.fetchCount(objectFormatTable);
        Assertions.assertEquals(1, count);
    }

    @Test
    void testReturnsSameIdForSameObjectFormat() {
        final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
        final ResolvedObjectFormats resolved = new ResolvedObjectFormats(ctx);
        final long id1 = resolved.resolve("rfc5452");
        final long id2 = resolved.resolve("rfc5452");
        Assertions.assertEquals(id1, id2);
        final int count = ctx.fetchCount(objectFormatTable);
        Assertions.assertEquals(1, count);
    }

    @Test
    void testNoDuplicatesProduced() {
        final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
        final ResolvedObjectFormats resolved = new ResolvedObjectFormats(ctx);
        final long id1 = resolved.resolve("rfc5452");
        final long id2 = resolved.resolve("rfc5452");
        Assertions.assertEquals(id1, id2);
        final int count = ctx.fetchCount(objectFormatTable);
        Assertions.assertEquals(1, count);
    }

    @Test
    void testMultipleObjectFormatValues() {
        final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL);
        final ResolvedObjectFormats resolved = new ResolvedObjectFormats(ctx);
        final long id1 = resolved.resolve("rfc5452");
        final long id2 = resolved.resolve("non-rfc5252");
        Assertions.assertNotEquals(id1, id2);
        final int count = ctx.fetchCount(objectFormatTable);
        Assertions.assertEquals(2, count);
    }

    @Test
    public void testContract() {
        // mutable internal cache ignored
        EqualsVerifier.forClass(ResolvedObjectFormats.class).withIgnoredFields("resolvedObjectFormatsCache").verify();
    }
}
