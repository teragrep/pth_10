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
package com.teragrep.pth_10.ast;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FilteredColumns {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilteredColumns.class);
    private final String[] columns;
    private final String wc;

    /**
     * Checks for wildcards from given array of column names
     *
     * @param wc      wildcard statement
     * @param columns array of column names
     */

    public FilteredColumns(final String wc, final String[] columns) {
        this.columns = columns;
        this.wc = wc;
    }

    /**
     * @return list of column names that match the wildcard statement
     */

    public List<String> filtered() {
        final StringBuilder quotablePartBuilder = new StringBuilder();
        final StringBuilder regexBuilder = new StringBuilder();
        final String regex;

        for (final char c : wc.toCharArray()) {
            if (c == '*') {
                // On wildcard, get preceding content and quote it
                // Also clear quotablePartBuilder and add regex any char wildcard
                if (quotablePartBuilder.length() > 0) {
                    regexBuilder.append(Pattern.quote(quotablePartBuilder.toString()));
                    quotablePartBuilder.setLength(0);
                }
                regexBuilder.append(".*");
            }
            else {
                // On normal characters, add to quotablePartBuilder
                quotablePartBuilder.append(c);
            }
        }

        if (quotablePartBuilder.length() > 0) {
            // if quotablePartBuilder is not empty, quote and add it
            regex = regexBuilder + Pattern.quote(quotablePartBuilder.toString());
        }
        else {
            // if it is empty, the regexBuilder contains the final regex
            regex = regexBuilder.toString();
        }

        final Pattern p = Pattern.compile(regex);
        Matcher m;
        final List<String> matchedFields = new ArrayList<>();

        for (final String column : columns) {
            m = p.matcher(column);
            if (m.matches()) {
                LOGGER.debug("Field <[{}]> matches the wildcard rule: <[{}]>", column, wc);
                matchedFields.add(column);
            }
        }

        return matchedFields;
    }

    @Override
    public boolean equals(Object o) {
        final boolean isEquals;
        if (this == o)
            isEquals = true;
        else if (o == null || getClass() != o.getClass())
            isEquals = false;
        else {
            final FilteredColumns filteredColumns = (FilteredColumns) o;
            isEquals = Objects.equals(wc, filteredColumns.wc) && Arrays.equals(columns, filteredColumns.columns);
        }
        return isEquals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(wc, Arrays.hashCode(columns));
    }
}
