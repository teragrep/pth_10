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
package com.teragrep.pth10.steps.subsearch;

import com.teragrep.pth10.ast.DPLInternalStreamingQueryListener;
import com.teragrep.pth10.ast.StepList;
import com.teragrep.pth10.steps.AbstractStep;

public abstract class AbstractSubsearchStep extends AbstractStep {

    public enum SubSearchType {
        MAIN_SEARCH_FILTERING, // for use in "LogicalStatement" subsearch (e.g. index=a [ search index=b ])
        JOIN_COMMAND_SUBSEARCH // for use in join command's [ ] brackets (e.g. | join (...) [ search index=b ])
    }

    protected StepList stepList;
    protected DPLInternalStreamingQueryListener listener;
    protected String hdfsPath;
    protected SubSearchType type = SubSearchType.MAIN_SEARCH_FILTERING;

    public void setStepList(StepList stepList) {
        this.stepList = stepList;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public void setListener(DPLInternalStreamingQueryListener listener) {
        this.listener = listener;
    }

    public void setType(SubSearchType type) {
        this.type = type;
    }

    public StepList getStepList() {
        return stepList;
    }

    public DPLInternalStreamingQueryListener getListener() {
        return listener;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public SubSearchType getType() {
        return type;
    }
}
