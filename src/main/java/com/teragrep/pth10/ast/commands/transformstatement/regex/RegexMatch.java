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

package com.teragrep.pth10.ast.commands.transformstatement.regex;

import com.teragrep.pth10.ast.Util;
import org.apache.spark.sql.api.java.UDF3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used for regex command (RegexTransformation)
 */
public class RegexMatch implements UDF3<String, String, Boolean, Boolean> {

    /**
     * Filter rows that do not match the regex
     * @param rowString row content
     * @param regexString regex statement
     * @param equals = is true, != is false
     * @return boolean for where function
     * @throws Exception
     */
    @Override
    public Boolean call(String rowString, String regexString, Boolean equals) throws Exception {
        Matcher m = Pattern.compile(Util.stripQuotes(regexString)).matcher(rowString);
        boolean isMatch = m.matches();

        if (isMatch && equals) {
            // Regex matches with = sign
            return true;
        }
        else if (isMatch && !equals) {
            // Regex matches with != sign
            return false;
        }
        else if (!isMatch && equals) {
            // Regex doesn't match with = sign
            return false;
        }
        else if (!isMatch && !equals) {
            // Regex doesn't match with != sign
            return true;
        }

        throw new RuntimeException(String.format("Invalid arguments used with regex command: row: '%s' regex: '%s' equals: '%s", rowString, regexString, equals));
    }
}
