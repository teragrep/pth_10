/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

public final class BloomFilterTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterTable.class);
    private final Config config;
    private final boolean ignoreConstraints;

    public BloomFilterTable(Config config) {
        this(config, false);
    }

    // used in testing
    public BloomFilterTable(Config config, boolean ignoreConstraints) {
        this.config = config;
        this.ignoreConstraints = ignoreConstraints;
    }

    public void create() {
        if (ignoreConstraints && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Ignore database constraints active this should be only used in testing");
        }
        final String name = new FilterTypes(config).tableName();
        final String sql = createTableSQL(name);
        final Connection connection = new LazyConnection(config).get();
        try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.execute();
            connection.commit();
            LOGGER.info("Created a bloom filter table name <[{}]>", name);
            LOGGER.debug("Create table SQL <{}>", sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating bloom filter table: " + e);
        }
    }

    private String createTableSQL(final String name) {
        final Pattern pattern = Pattern.compile("^[A-Za-z0-9_]+$");
        if (!pattern.matcher(name).find()) {
            throw new RuntimeException("dpl.pth_06.bloom.table.name malformed name, only use alphabets, numbers and _");
        }
        if (name.length() > 100) {
            throw new RuntimeException(
                    "dpl.pth_06.bloom.table.name was too long, allowed maximum length is 100 characters"
            );
        }

        if (ignoreConstraints) {
            return "CREATE TABLE IF NOT EXISTS `" + name + "`("
                    + "`id`            BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                    + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                    + "`filter` LONGBLOB NOT NULL);";
        }
        return "CREATE TABLE IF NOT EXISTS `" + name + "`("
                + "`id`            BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                + "`filter` LONGBLOB NOT NULL," + "CONSTRAINT `" + name
                + "_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)" + "ON DELETE CASCADE,"
                + "CONSTRAINT `" + name + "_ibfk_2` FOREIGN KEY (partition_id) REFERENCES journaldb.logfile (id)"
                + "ON DELETE CASCADE" + ");";

    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final BloomFilterTable cast = (BloomFilterTable) object;
        return config.equals(cast.config) && this.ignoreConstraints == cast.ignoreConstraints;
    }
}
