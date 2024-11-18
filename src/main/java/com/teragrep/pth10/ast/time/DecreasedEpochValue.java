/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
package com.teragrep.pth10.ast.time;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.w3c.dom.Element;

import java.util.Objects;

/** Decreases the amount from qualifier epoch value */
public final class DecreasedEpochValue implements TimeQualifier {

    private final TimeQualifier origin;
    private final long decreaseAmount;

    public DecreasedEpochValue(final TimeQualifier origin, final long decreaseAmount) {
        this.origin = origin;
        this.decreaseAmount = decreaseAmount;
    }

    @Override
    public Element xmlElement() {
        final long decreasedValue = origin.epoch() - decreaseAmount;
        final Element element = origin.xmlElement();
        // set decreased value
        element.setAttribute("value", Long.toString(decreasedValue));
        return element;
    }

    @Override
    public boolean isStartTime() {
        return origin.isStartTime();
    }

    @Override
    public boolean isEndTime() {
        return origin.isEndTime();
    }

    @Override
    public long epoch() {
        return origin.epoch() - decreaseAmount;
    }

    @Override
    public Column column() {
        final Column timeColumn = new Column("`_time`");
        final Column retrunColumn;
        if (origin.isStartTime()) {
            retrunColumn = timeColumn.geq(functions.from_unixtime(functions.lit(epoch())));
        }
        else if (origin.isEndTime()) {
            retrunColumn = timeColumn.lt(functions.from_unixtime(functions.lit(epoch())));
        }
        else {
            retrunColumn = origin.column();
        }
        return retrunColumn;
    }

    @Override
    public boolean isUnixEpoch() {
        return origin.isUnixEpoch();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        final DecreasedEpochValue cast = (DecreasedEpochValue) o;
        return decreaseAmount == cast.decreaseAmount && Objects.equals(origin, cast.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(origin, decreaseAmount);
    }
}
