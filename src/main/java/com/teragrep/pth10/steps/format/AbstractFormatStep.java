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
package com.teragrep.pth10.steps.format;

import com.teragrep.functions.dpf_02.AbstractStep;

public abstract class AbstractFormatStep extends AbstractStep {

    protected String mvSep = "OR";
    protected int maxResults = 0;
    protected String rowPrefix = "(";
    protected String colPrefix = "(";
    protected String colSep = "AND";
    protected String colSuffix = ")";
    protected String rowSep = "OR";
    protected String rowSuffix = ")";
    protected String emptyStr = "NOT( )";

    public String getMvSep() {
        return mvSep;
    }

    public void setMvSep(String mvSep) {
        this.mvSep = mvSep;
    }

    public int getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
    }

    public String getRowPrefix() {
        return rowPrefix;
    }

    public void setRowPrefix(String rowPrefix) {
        this.rowPrefix = rowPrefix;
    }

    public String getColPrefix() {
        return colPrefix;
    }

    public void setColPrefix(String colPrefix) {
        this.colPrefix = colPrefix;
    }

    public String getColSep() {
        return colSep;
    }

    public void setColSep(String colSep) {
        this.colSep = colSep;
    }

    public String getColSuffix() {
        return colSuffix;
    }

    public void setColSuffix(String colSuffix) {
        this.colSuffix = colSuffix;
    }

    public String getRowSep() {
        return rowSep;
    }

    public void setRowSep(String rowSep) {
        this.rowSep = rowSep;
    }

    public String getRowSuffix() {
        return rowSuffix;
    }

    public void setRowSuffix(String rowSuffix) {
        this.rowSuffix = rowSuffix;
    }

    public String getEmptyStr() {
        return emptyStr;
    }

    public void setEmptyStr(String emptyStr) {
        this.emptyStr = emptyStr;
    }
}
