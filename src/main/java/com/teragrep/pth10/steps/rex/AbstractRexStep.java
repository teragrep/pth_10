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

package com.teragrep.pth10.steps.rex;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.steps.AbstractStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class AbstractRexStep extends AbstractStep {
    protected String regexStr = null;
    protected String field = "_raw";
    protected String offsetField = null;
    protected int maxMatch = -1;
    protected boolean sedMode = false;
    protected DPLParserCatalystContext catCtx = null;

    public AbstractRexStep(Dataset<Row> dataset) {
        super(dataset);
    }

    public void setMaxMatch(int maxMatch) {
        this.maxMatch = maxMatch;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setRegexStr(String regexStr) {
        this.regexStr = regexStr;
    }

    public void setSedMode(boolean sedMode) {
        this.sedMode = sedMode;
    }

    public void setOffsetField(String offsetField) {
        this.offsetField = offsetField;
    }

    public boolean isSedMode() {
        return sedMode;
    }

    public String getField() {
        return field;
    }

    public String getRegexStr() {
        return regexStr;
    }

    public int getMaxMatch() {
        return maxMatch;
    }

    public String getOffsetField() {
        return offsetField;
    }

    public void setCatCtx(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public DPLParserCatalystContext getCatCtx() {
        return catCtx;
    }
}
