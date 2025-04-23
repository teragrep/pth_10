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

import com.teragrep.pth10.ast.Text;
import org.apache.hadoop.shaded.com.google.re2j.Matcher;
import org.apache.hadoop.shaded.com.google.re2j.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidTrailingRelativeTimestampText implements Text {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidTrailingRelativeTimestampText.class);
    private final Text origin;
    private final Pattern validPattern = Pattern.compile(".*@([a-zA-Z]+)([+-]?\\d+[a-zA-Z]+)?$");

    public ValidTrailingRelativeTimestampText(final Text origin) {
        this.origin = origin;
    }

    public boolean isStub() {
        boolean isStub = false;
        try {
            read();
        }
        catch (final RuntimeException e) {
            isStub = true;
        }
        return isStub;
    }

    @Override
    public String read() {
        final String originString = origin.read();
        LOGGER.debug("origin string <{}>", originString);
        final String updatedString;
        final Matcher matcher = validPattern.matcher(originString);
        if (matcher.find() && matcher.groupCount() > 1 && !matcher.group(2).isEmpty()) {
            // The second group contains the valid trailing offset (e.g., +3h, -10m)
            updatedString = matcher.group(2);
        }
        else {
            throw new RuntimeException("Could not find a valid trailing offset after '@'");
        }
        LOGGER.debug("trailing timestamp from trail <{}>", updatedString);
        return updatedString;
    }
}
