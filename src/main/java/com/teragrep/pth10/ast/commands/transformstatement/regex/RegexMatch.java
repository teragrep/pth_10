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
package com.teragrep.pth10.ast.commands.transformstatement.regex;

import com.teragrep.jpr_01.JavaPcre;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.apache.spark.sql.api.java.UDF3;

/**
 * Used for regex command (RegexTransformation)
 */
public class RegexMatch implements UDF3<String, String, Boolean, Boolean> {

    /**
     * Filter rows that do not match the regex
     * 
     * @param rowString   row content
     * @param regexString regex statement
     * @param equals      = is true, != is false
     * @return boolean for where function
     * @throws Exception invalid args or pcre error
     */
    @Override
    public Boolean call(String rowString, String regexString, Boolean equals) throws Exception {
        // create JavaPCRE instance
        final JavaPcre pcre = new JavaPcre();

        // compile regex matcher for given regex pattern
        pcre.compile_java(new UnquotedText(new TextString(regexString)).read());

        // get matches from beginning of input string
        pcre.singlematch_java(rowString, 0);
        boolean isMatch = pcre.get_matchfound();

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

        throw new RuntimeException(
                String
                        .format(
                                "Invalid arguments used with regex command: row: '%s' regex: '%s' equals: '%s'",
                                rowString, regexString, equals
                        )
        );
    }
}
