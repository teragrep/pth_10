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

package com.teragrep.pth10.ast;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Decorator for unquoting text.
 */
public class UnquotedText implements Text {

    private Text origin;

    public UnquotedText(Text origin) {
        this.origin = origin;
    }

    @Override
    public String read() {
        return stripQuotes(this.origin.read());
    }

    /**
     * Strips quotes
     * @return string with stripped quotes
     */
    private String stripQuotes(String quoted) {
        Matcher m = Pattern.compile("^\"(.*)\"$").matcher(quoted);
        Matcher m1 = Pattern.compile("^'(.*)'$").matcher(quoted);

        String strUnquoted = quoted;
        // check "-quotes
        if (m.find()) {
            strUnquoted = m.group(1);
        } else {
            // check '-quotes
            if (m1.find()) {
                strUnquoted = m1.group(1);
            }
        }
        return strUnquoted;
    }
}
