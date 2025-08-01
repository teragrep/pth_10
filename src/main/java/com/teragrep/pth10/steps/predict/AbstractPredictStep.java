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
package com.teragrep.pth10.steps.predict;

import com.teragrep.functions.dpf_02.AbstractStep;
import org.apache.spark.sql.Column;

import java.util.List;

public abstract class AbstractPredictStep extends AbstractStep {

    public enum Algorithm {
        LL, LLT, LLP, LLP5, LLB, BILL
    }

    protected Algorithm algorithm;
    protected List<Column> listOfColumnsToPredict;
    protected String correlateField;
    protected String suppressField;
    protected int futureTimespan;
    protected int holdback;
    protected int period;
    protected int upper;
    protected int lower;
    protected String upperField;
    protected String lowerField;

    public AbstractPredictStep() {
        super();
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public List<Column> getListOfColumnsToPredict() {
        return listOfColumnsToPredict;
    }

    public void setListOfColumnsToPredict(List<Column> listOfColumnsToPredict) {
        this.listOfColumnsToPredict = listOfColumnsToPredict;
    }

    public String getCorrelateField() {
        return correlateField;
    }

    public void setCorrelateField(String correlateField) {
        this.correlateField = correlateField;
    }

    public String getSuppressField() {
        return suppressField;
    }

    public void setSuppressField(String suppressField) {
        this.suppressField = suppressField;
    }

    public int getFutureTimespan() {
        return futureTimespan;
    }

    public void setFutureTimespan(int futureTimespan) {
        this.futureTimespan = futureTimespan;
    }

    public int getHoldback() {
        return holdback;
    }

    public void setHoldback(int holdback) {
        this.holdback = holdback;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public int getUpper() {
        return upper;
    }

    public void setUpper(int upper) {
        this.upper = upper;
    }

    public int getLower() {
        return lower;
    }

    public void setLower(int lower) {
        this.lower = lower;
    }

    public void setLowerField(String lowerField) {
        this.lowerField = lowerField;
    }

    public String getLowerField() {
        return lowerField;
    }

    public void setUpperField(String upperField) {
        this.upperField = upperField;
    }

    public String getUpperField() {
        return upperField;
    }
}
