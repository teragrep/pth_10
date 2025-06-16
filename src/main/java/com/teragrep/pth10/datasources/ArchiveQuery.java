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
package com.teragrep.pth10.datasources;

/**
 * Class representing an archive query. The query is in XML format and is used by the PTH-06 Datasource Component. If no
 * query is provided, the constructor will set the isStub property as true, which should be used instead of a null
 * check.
 */
public final class ArchiveQuery {

    public final String queryString;
    public final boolean isStub;

    /**
     * Case: No query given. Traditionally this would be the null case. Check for a "null case" using the isStub field.
     */
    public ArchiveQuery() {
        this.queryString = "";
        this.isStub = true;
    }

    /**
     * Case: A non-empty query given. Traditionally this would be a non-null case. It does not guarantee that the query
     * is 100% valid, only that it is of a non-null value.
     * 
     * @param queryString XML-formatted string for the Archive
     */
    public ArchiveQuery(final String queryString) {
        if (queryString == null || queryString.isEmpty()) {
            throw new IllegalArgumentException("ArchiveQuery object was initialized with a null or empty string.");
        }
        this.queryString = queryString;
        this.isStub = false;
    }

    @Override
    public String toString() {
        return this.queryString;
    }
}
