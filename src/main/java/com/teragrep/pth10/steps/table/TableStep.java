/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

public class TableStep extends AbstractTableStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStep.class);

    public TableStep(Dataset<Row> dataset) {
        super(dataset);
    }

    @Override
    public Dataset<Row> get() {
        if (this.dataset == null) {
            return null;
        }

        // check fields for wildcards
        List<String> wildcardedFields = new ArrayList<>();
        for (String field : this.listOfFields) {
            LOGGER.debug("Check field '" + field + "' for wild cards");
            wildcardedFields.addAll(getWildcardFields(field, this.dataset.columns()));
        }

        Dataset<Row> dsWithDroppedCols = this.dataset;
        // drop columns not present in the table command
        for (String column : this.dataset.columns()) {
            if (!wildcardedFields.contains(column)) {
                LOGGER.debug("Dropped column '" + column + "'!");
                dsWithDroppedCols = dsWithDroppedCols.drop(column);
            }
        }

        // reorder them to be in the same order as in the table command
        Seq<Column> seqOfCols = JavaConversions.asScalaBuffer(
                wildcardedFields.stream().map(functions::col).collect(Collectors.toList())
        );

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
     * @param wc wildcard statement
     * @param cols array of column names
     * @return list of column names which match the wildcard statement
     */
    private List<String> getWildcardFields(String wc, String[] cols) {
        Pattern p = Pattern.compile(wc.replaceAll("\\*",".*"));
        Matcher m = null;
        List<String> matchedFields = new ArrayList<>();

        for (String column : cols) {
            m = p.matcher(column);
            if (m.matches()) {
                LOGGER.debug("Field '" + column + "' matches the wild card rule: '" + wc + "'");
                matchedFields.add(column);
            }
        }

        return matchedFields;
    }
}
