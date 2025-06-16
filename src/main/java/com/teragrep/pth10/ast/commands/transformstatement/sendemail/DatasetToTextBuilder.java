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
package com.teragrep.pth10.ast.commands.transformstatement.sendemail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.io.Serializable;
import java.util.List;

/**
 * Converts Dataset to text, such as an HTML table, CSV or raw.
 * 
 * @author eemhu
 */
public class DatasetToTextBuilder implements Serializable {

    private static final long serialVersionUID = 1L;
    private String format = "table";
    private String lineBreak = "\n";

    /**
     * empty constructor will use default 'table' format and '\n' line break
     */
    public DatasetToTextBuilder() {
    }

    /**
     * Constructor that provides format and linebreak character
     * 
     * @param format    <code>html, csv or raw</code>
     * @param lineBreak like <code>\n</code>
     */
    public DatasetToTextBuilder(String format, String lineBreak) {
        this.format = format;
        this.lineBreak = lineBreak;
    }

    /**
     * Collects dataset to driver and builds the text content using the {@link #build(List)} method
     * 
     * @param dset Dataset to build the content from
     * @return text content
     */
    public String build(Dataset<Row> dset) {
        List<Row> listOfRows = dset.collectAsList(); // to driver, don't use for high count datasets
        return this.build(listOfRows);
    }

    /**
     * Build string with given format<br>
     * "table" format can still use hard-coded \n line break since it is just html code
     * 
     * @param listOfRows list of dataset rows
     * @return text content
     */
    public String build(List<Row> listOfRows) {
        if (listOfRows.size() == 0 && format.equals("csv")) {
            return "no_results" + this.lineBreak + "Search results had no rows.";
        }
        else if (listOfRows.size() == 0 && format.equals("table")) {
            return "<tr>\n<td>\nSearch results had no rows.</td>\n</tr>\n";
        }
        else if (listOfRows.size() == 0 && format.equals("raw")) {
            return "Search results had no rows.";
        }

        StructType schema = listOfRows.get(0).schema();
        int numOfCols = schema.length();

        if (format.equals("table")) {
            // includes styling to make every other row grey-ish
            String html = "<style>tr:nth-child(even){background-color:#cdcfd1;}</style>\n<table>\n";

            // column headers
            html = html.concat("<tr>\n");

            Iterator<StructField> it = schema.iterator();
            while (it.hasNext()) {
                StructField col = it.next();
                String colName = col.name();

                html = html.concat("<td>");
                html = html.concat(colName);
                html = html.concat("</td>\n");
            }

            html = html.concat("</tr>\n");

            // actual rows
            for (Row r : listOfRows) {
                html = html.concat("<tr>\n");

                for (int i = 0; i < numOfCols; i++) {
                    html = html.concat("<td>");

                    // check for null
                    Object cell = r.getAs(i);
                    if (cell != null) {
                        html = html.concat(cell.toString());
                    }
                    else {
                        html = html.concat("null");
                    }

                    html = html.concat("</td>\n");
                }

                html = html.concat("</tr>\n");
            }

            html = html.concat("</table>");

            return html;
        }
        else if (format.equals("csv")) {
            String csv = "";
            String cols = "";

            // Go through columns
            Iterator<StructField> it = schema.iterator();
            boolean first = true;
            while (it.hasNext()) {
                StructField col = it.next();
                String colName = col.name();

                if (first) {
                    cols = cols.concat(colName);
                    first = false;
                }
                else {
                    cols = cols.concat(",");
                    cols = cols.concat(colName);
                }
            }

            // add column headers
            csv = csv.concat(cols);
            csv = csv.concat(this.lineBreak);

            // Go through rows
            for (Row r : listOfRows) {
                String rowString = "";
                boolean firstInRow = true;

                // Go through cells in each row
                // Enclose cells in double quotes
                for (int i = 0; i < r.length(); i++) {
                    String cell = r.getAs(i).toString();
                    cell = cell.replaceAll("\"", "\"\""); // in-cell double quotes need to be duplicated

                    if (firstInRow) {
                        cell = "\"" + cell + "\"";
                        firstInRow = false;
                    }
                    else {
                        cell = ",\"" + cell + "\"";
                    }
                    rowString = rowString.concat(cell);
                }
                csv = csv.concat(rowString);
                csv = csv.concat(this.lineBreak);
            }

            return csv;
        }
        else if (format.equals("raw")) {
            String raw = "";
            String cols = "";

            Iterator<StructField> it = schema.iterator();
            boolean first = true;
            while (it.hasNext()) {
                StructField col = it.next();
                String colName = col.name();

                if (first) {
                    cols = cols.concat(colName);
                    first = false;
                }
                else {
                    cols = cols.concat(this.lineBreak);
                    cols = cols.concat(colName);
                }
            }

            // column headers
            raw = raw.concat(cols); // substring to remove [ ] from string
            raw = raw.concat(this.lineBreak);
            // rows
            for (Row r : listOfRows) {
                raw = raw.concat(r.mkString(this.lineBreak));
                raw = raw.concat(this.lineBreak);
            }

            return raw;
        }

        throw new IllegalArgumentException("Invalid inline data format '" + format + "' !");
    }
}
