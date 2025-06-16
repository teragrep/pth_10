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
package com.teragrep.pth10.steps.table;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class TableStep extends AbstractTableStep {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableStep.class);

    public TableStep() {
        super();
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) {
        if (dataset == null) {
            return null;
        }

        // check fields for wildcards
        List<String> wildcardedFields = new ArrayList<>();
        for (String field : this.listOfFields) {
            LOGGER.debug("Check field <[{}]> for wildcards", field);
            wildcardedFields.addAll(getWildcardFields(field, dataset.columns()));
        }

        Dataset<Row> dsWithDroppedCols = dataset;
        // drop columns not present in the table command
        for (String column : dataset.columns()) {
            if (!wildcardedFields.contains(column)) {
                LOGGER.debug("Dropped column <[{}}> !", column);
                dsWithDroppedCols = dsWithDroppedCols.drop(column);
            }
        }

        // reorder them to be in the same order as in the table command
        Seq<Column> seqOfCols = JavaConversions
                .asScalaBuffer(wildcardedFields.stream().map(functions::col).collect(Collectors.toList()));

        assert dsWithDroppedCols != null : "Dropped columns dataset was null";

        if (seqOfCols != null && !seqOfCols.isEmpty()) {
            return dsWithDroppedCols.select(seqOfCols);
        }
        else {
            return dsWithDroppedCols;
        }

    }

    /**
     * Gets wildcarded fields from given array of column names
     * 
     * @param wc   wildcard statement
     * @param cols array of column names
     * @return list of column names which match the wildcard statement
     */
    private List<String> getWildcardFields(String wc, String[] cols) {
        StringBuilder quotablePartBuilder = new StringBuilder();
        StringBuilder regexBuilder = new StringBuilder();
        final String regex;

        for (char c : wc.toCharArray()) {
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
            regex = Pattern.quote(quotablePartBuilder.toString());
        }
        else {
            // if it is empty, the regexBuilder contains the final regex
            regex = regexBuilder.toString();
        }

        Pattern p = Pattern.compile(regex);
        Matcher m;
        List<String> matchedFields = new ArrayList<>();

        for (String column : cols) {
            m = p.matcher(column);
            if (m.matches()) {
                LOGGER.debug("Field <[{}]> matches the wildcard rule: <[{}]>", column, wc);
                matchedFields.add(column);
            }
        }

        return matchedFields;
    }
}
