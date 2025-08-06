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
package com.teragrep.pth10.steps.join;

import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.steps.subsearch.SubsearchStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public abstract class AbstractJoinStep extends AbstractStep {

    protected String joinMode = null;
    protected Boolean usetime = null;
    protected Boolean earlier = null;
    protected Boolean overwrite = null;
    protected Integer max = null;

    protected Dataset<Row> subSearchDataset = null;
    protected String pathForSubsearchSave = null;
    protected List<String> listOfFields = null;

    protected DPLParserCatalystContext catCtx = null;
    protected SubsearchStep subsearchStep = null;

    public AbstractJoinStep() {
        super();
    }

    public void setEarlier(Boolean earlier) {
        this.earlier = earlier;
    }

    public void setJoinMode(String joinMode) {
        this.joinMode = joinMode;
    }

    public void setMax(Integer max) {
        this.max = max;
    }

    public void setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setUsetime(Boolean usetime) {
        this.usetime = usetime;
    }

    public void setSubSearchDataset(Dataset<Row> subSearchDataset) {
        this.subSearchDataset = subSearchDataset;
    }

    public void setPathForSubsearchSave(String pathForSubsearchSave) {
        this.pathForSubsearchSave = pathForSubsearchSave;
    }

    public void setListOfFields(List<String> listOfFields) {
        this.listOfFields = listOfFields;
    }

    public void setCatCtx(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public void setSubsearchStep(SubsearchStep subsearchStep) {
        this.subsearchStep = subsearchStep;
    }

    public Boolean getEarlier() {
        return earlier;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public Boolean getUsetime() {
        return usetime;
    }

    public Integer getMax() {
        return max;
    }

    public String getJoinMode() {
        return joinMode;
    }

    public Dataset<Row> getSubSearchDataset() {
        return subSearchDataset;
    }

    public String getPathForSubsearchSave() {
        return pathForSubsearchSave;
    }

    public List<String> getListOfFields() {
        return listOfFields;
    }

    public DPLParserCatalystContext getCatCtx() {
        return catCtx;
    }

    public SubsearchStep getSubsearchStep() {
        return subsearchStep;
    }
}
