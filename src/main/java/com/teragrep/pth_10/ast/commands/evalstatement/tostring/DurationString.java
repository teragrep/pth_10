/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
package com.teragrep.pth_10.ast.commands.evalstatement.tostring;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Objects;

/**
 * Converts input column to the readable time format HH:MM:SS.
 */
final class DurationString implements ToStringFormat {

    private final Column inputColumn;
    private final String option;
    private final FormatResultStub stub;

    DurationString(final Column inputColumn, final String option) {
        this(inputColumn, option, new FormatResultStub());
    }

    private DurationString(final Column inputColumn, final String option, final FormatResultStub stub) {
        this.inputColumn = inputColumn;
        this.option = option;
        this.stub = stub;
    }

    @Override
    public FormatResult result() {
        final int secondsInMinute = 60;
        final int secondsInHour = secondsInMinute * 60;
        final FormatResult result;
        if ("duration".equalsIgnoreCase(option)) {
            final Column totalSeconds = functions.round(inputColumn).cast(DataTypes.LongType);
            final Column hours = totalSeconds.divide(secondsInHour).cast(DataTypes.LongType);
            final Column minutes = totalSeconds.mod(secondsInHour).divide(secondsInMinute).cast(DataTypes.LongType);
            final Column seconds = totalSeconds.mod(secondsInMinute);
            result = new FormatResultImpl(functions.format_string("%02d:%02d:%02d", hours, minutes, seconds));
        }
        else {
            result = stub;
        }
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        final boolean isEqual;
        if (o == null || getClass() != o.getClass()) {
            isEqual = false;
        }
        else {
            final DurationString that = (DurationString) o;
            isEqual = Objects.equals(inputColumn, that.inputColumn) && Objects.equals(option, that.option);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputColumn, option);
    }
}
