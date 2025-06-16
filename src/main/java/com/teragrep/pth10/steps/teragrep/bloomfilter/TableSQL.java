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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.regex.Pattern;

public final class TableSQL {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSQL.class);
    private final Pattern validPattern;
    private final String name;
    private final String journalDBName;
    private final boolean ignoreConstraints;

    // used in testing
    public TableSQL(String name) {
        this(name, "journaldb");
    }

    // used in testing
    public TableSQL(String name, boolean ignoreConstraints) {
        this(name, "journaldb", ignoreConstraints);
    }

    public TableSQL(String name, String journalDBName) {
        this(name, journalDBName, false);
    }

    public TableSQL(String name, String journalDBName, boolean ignoreConstraints) {
        this(name, journalDBName, ignoreConstraints, Pattern.compile("^[A-Za-z0-9_]+$"));
    }

    public TableSQL(String name, String journalDBName, boolean ignoreConstraints, Pattern validPattern) {
        this.name = name;
        this.journalDBName = journalDBName;
        this.ignoreConstraints = ignoreConstraints;
        this.validPattern = validPattern;
    }

    private void validSQLName(final String sql) {
        if (ignoreConstraints && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Ignore database constraints active this should be only used in testing");
        }
        if (!validPattern.matcher(sql).find()) {
            throw new RuntimeException("malformed SQL input <[" + sql + "]>, only use alphabets, numbers and _");
        }
        if (sql.length() > 100) {
            throw new RuntimeException(
                    "SQL input <[" + sql + "]> was too long, allowed maximum length is 100 characters"
            );
        }
    }

    public String createTableSQL() {
        validSQLName(name);
        final String sql;
        if (ignoreConstraints) {
            sql = "CREATE TABLE IF NOT EXISTS `" + name + "`("
                    + "`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                    + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                    + "`filter` LONGBLOB NOT NULL);";
        }
        else {
            validSQLName(journalDBName);
            sql = "CREATE TABLE IF NOT EXISTS `" + name + "`("
                    + "`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                    + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                    + "`filter` LONGBLOB NOT NULL," + "CONSTRAINT `" + name
                    + "_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)" + "ON DELETE CASCADE,"
                    + "CONSTRAINT `" + name + "_ibfk_2` FOREIGN KEY (partition_id) REFERENCES " + journalDBName
                    + ".logfile (id)" + "ON DELETE CASCADE" + ");";
        }
        return sql;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        final TableSQL cast = (TableSQL) o;
        // custom equals check for Pattern
        boolean arePatternsEqual = validPattern.pattern().equals(cast.validPattern.pattern())
                && validPattern.flags() == cast.validPattern.flags();
        return ignoreConstraints == cast.ignoreConstraints && arePatternsEqual && Objects.equals(name, cast.name)
                && Objects.equals(journalDBName, cast.journalDBName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validPattern, name, journalDBName, ignoreConstraints);
    }
}
