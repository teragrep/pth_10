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
package com.teragrep.pth_10.ast.time;

import com.teragrep.pth_10.ast.TextString;
import com.teragrep.pth_10.ast.UnquotedText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Objects;

public final class RelativeTimestamp implements DPLTimestamp {

    private final Logger LOGGER = LoggerFactory.getLogger(RelativeTimestamp.class);

    private final ValidRelativeTimestampText offsetString;
    private final ZonedDateTime baseTime;

    public RelativeTimestamp(final String offsetString, final ZonedDateTime baseTime) {
        this(new ValidRelativeTimestampText(new UnquotedText(new TextString(offsetString))), baseTime);
    }

    public RelativeTimestamp(final ValidRelativeTimestampText offsetString, final ZonedDateTime baseTime) {
        this.offsetString = offsetString;
        this.baseTime = baseTime;
    }

    @Override
    public ZonedDateTime zonedDateTime() {
        if (!isValid()) {
            throw new RuntimeException("Timestamp did not contain a valid relative timestamp information");
        }
        final String validOffset = offsetString.read();
        final DPLTimestamp offsetTimestamp = new OffsetTimestamp(validOffset, baseTime);
        final DPLTimestamp snappedTimestamp = new SnappedTimestamp(validOffset, offsetTimestamp);
        final ZonedDateTime updatedTime;
        if (snappedTimestamp.isValid()) {
            updatedTime = snappedTimestamp.zonedDateTime();
        }
        else {
            updatedTime = offsetTimestamp.zonedDateTime();
        }
        return updatedTime;
    }

    @Override
    public boolean isValid() {
        boolean isValid = true;
        try {
            offsetString.read();
        }
        catch (final IllegalArgumentException e) {
            isValid = false;
        }
        return isValid;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final RelativeTimestamp other = (RelativeTimestamp) o;
        return Objects.equals(offsetString, other.offsetString) && Objects.equals(baseTime, other.baseTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offsetString, baseTime);
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
