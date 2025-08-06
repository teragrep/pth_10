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
package com.teragrep.pth10.steps.iplocation;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractIplocationStep extends AbstractStep {

    protected String lang = "en";
    protected String field = null; // required
    protected boolean allFields = false;
    protected String prefix = "";
    protected DPLParserCatalystContext catCtx = null;
    protected String pathToDb = null;
    protected final String internalMapColumnName = "$$dpl_pth10_internal_iplocation_column$$";
    protected final List<String> columnsMinimal = Arrays.asList("country", "lat", "lon", "region", "city");
    protected final List<String> columnsFull = Arrays
            .asList("country", "lat", "lon", "metroCode", "continent", "city", "region");

    protected final List<String> columnsRirData = Arrays.asList("country", "operator");
    protected final List<String> columnsCountryData = Arrays.asList("country", "continent");

    public AbstractIplocationStep() {
        super();
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setAllFields(boolean allFields) {
        this.allFields = allFields;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setCatCtx(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public void setPathToDb(String pathToDb) {
        this.pathToDb = pathToDb;
    }

    public String getField() {
        return field;
    }

    public String getLang() {
        return lang;
    }

    public String getPrefix() {
        return prefix;
    }

    public boolean isAllFields() {
        return allFields;
    }

    public DPLParserCatalystContext getCatCtx() {
        return catCtx;
    }

    public String getPathToDb() {
        return pathToDb;
    }
}
