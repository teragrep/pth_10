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
package com.teragrep.pth10.ast.time;

import com.teragrep.pth10.ast.Text;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ValidSnapToTimeText implements Text {

    private final Pattern snapPattern;
    private final Text origin;

    public ValidSnapToTimeText(final Text origin) {
        this(origin, Pattern.compile("@((?:w[0-7])|[a-zA-Z]+)(?![a-zA-Z0-9])"));
    }

    public ValidSnapToTimeText(final Text origin, Pattern snapPattern) {
        this.origin = origin;
        this.snapPattern = snapPattern;
    }

    @Override
    public String read() {
        final String timeStampString = origin.read();
        final String snapUnitSubstring;
        final Matcher matcher = snapPattern.matcher(timeStampString);
        if (matcher.find()) {
            snapUnitSubstring = matcher.group(1);
        }
        else {
            throw new IllegalArgumentException("Invalid snap to time text <" + timeStampString + ">");
        }
        return snapUnitSubstring;
    }

    public boolean containsSnapCharacter() {
        final String timeStampString = origin.read();
        return timeStampString.contains("@");
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
        final ValidSnapToTimeText other = (ValidSnapToTimeText) o;
        return Objects.equals(snapPattern, other.snapPattern) && Objects.equals(origin, other.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapPattern, origin);
    }
}
