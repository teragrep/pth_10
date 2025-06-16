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
package com.teragrep.pth10.ast.bo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Node to contain information of subsearches
 */
public class SubSearchNode extends ColumnNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubSearchNode.class);

    private List<String> valList = new ArrayList<>();

    /**
     * Empty constructor, values needs to be filled up.
     */
    public SubSearchNode() {
        super.val = null;
    }

    public SubSearchNode(Token token) {
        super(token);
    }

    public SubSearchNode(Column col) {
        super.val = col;
    }

    public SubSearchNode(String str) {
        valList.add(str);
        val = new Column("_raw").like("%" + str + "%");
    }

    public Column getColumn() {
        return val;
    }

    public String getStrVal() {
        return valList.stream().collect(Collectors.joining(","));
    }

    public String toString() {
        String str = val.expr().sql();
        return str;
    }

    public Expression asExpression() {
        return val.expr();
    }

    public void addValue(String valStr) {
        valList.add(valStr);
        //		valStr="%"+valStr+"%";
        valStr = "(?i)^*" + valStr + "*";
        if (this.val == null) {
            this.val = new Column("_raw").rlike(valStr);
        }
        else {
            this.val = this.val.and(new Column("_raw").rlike(valStr));
        }
        LOGGER.info("subSearchNode current val: <{}>", val.expr().sql());
    }

    public Element asElement(Document d) {
        Element el = d.createElement("indexstatement");
        el.setAttribute("OPERATION", "EQUALS");
        el.setAttribute("value", "%" + valList.get(0) + "%");
        LOGGER.info("Construct archiveQuery: <{}>", ElementNode.toString(el));
        if (valList.size() > 1) {
            for (int i = 1; i < valList.size(); i++) {
                Element e = d.createElement("indexstatement");
                e.setAttribute("OPERATION", "EQUALS");
                e.setAttribute("value", "%" + valList.get(i) + "%");
                LOGGER.info("Construct archiveQuery: <{}>", ElementNode.toString(el));
                Element andE = d.createElement("AND");
                andE.appendChild(el);
                andE.appendChild(e);
                el = andE;
            }
        }
        LOGGER.info("SubNode=<{}>", new ElementNode(el));
        return el;
    }

    public Dataset<Row> asDataset(Dataset<Row> ds) {
        Dataset<Row> rv = null;
        if (ds != null)
            rv = ds.where(val);
        return rv;
    }
}
