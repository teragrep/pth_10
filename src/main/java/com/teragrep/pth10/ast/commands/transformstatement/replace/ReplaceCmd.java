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
package com.teragrep.pth10.ast.commands.transformstatement.replace;

import org.apache.spark.sql.api.java.UDF3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UDF used for the command <code>replace</code>.<br>
 * Spark's built-in regex_replace can be used, but the wildcard replacement does not work using that function. Instead
 * it must be done using this UDF. More information can be found from the 'struck' documentation. <br>
 * <hr>
 * Command examples:<br>
 * Original data: "data"
 * <hr>
 * <code>(1) REPLACE "data" WITH "y" -&gt; result: "y"</code>
 * <hr>
 * <code>(2) REPLACE "dat*" WITH "y" -&gt; result: "y"</code>
 * <hr>
 * <code>(3) REPLACE "dat*" WITH "y*" -&gt; result: "ya"</code>
 * <hr>
 * <code>(4) REPLACE "*at*" WITH "*y*" -&gt; result: "dya"</code>
 */
public class ReplaceCmd implements UDF3<String, String, String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceCmd.class);

    @Override
    public String call(String currentContent, String wildcard, String replaceWith) throws Exception {
        // Wildcard statement (field after REPLACE keyword) converted into regex statement
        String regex = wcfieldToRegex(wildcard);

        // Match converted regex with current content of the row
        Matcher matcher = Pattern.compile(regex).matcher(currentContent);

        LOGGER
                .debug(
                        String
                                .format(
                                        "[ReplaceCmd.call] Wildcard: %s | Regex: %s | CurrentContent: %s | WithClause: %s",
                                        wildcard, regex, currentContent, replaceWith
                                )
                );

        // Is there a match for replacing?
        boolean isMatch = matcher.matches();

        // Match the wildcards (if any) in the WITH clause
        Matcher wildcardMatcher = Pattern.compile("\\*").matcher(replaceWith);
        String subSeq = null;
        boolean isFirst = true;

        if (wildcard.contains("*")) {
            // Split original wildcard statement (field after REPLACE keyword) into parts based on wildcards
            String[] partsOfWildcard = wildcard.split("\\*");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Got split wildcard <{}>", Arrays.toString(partsOfWildcard));
            }

            for (int i = 0; i < partsOfWildcard.length; i++) {
                String contentWithinWildcard;
                if (isFirst && partsOfWildcard.length > 1) {
                    if (partsOfWildcard[i].length() < 1) {
                        // Leading wildcard, without trailing present
                        contentWithinWildcard = currentContent
                                .substring(0, currentContent.indexOf(partsOfWildcard[i + 1]));
                    }
                    else {
                        // Leading wildcard, with trailing present
                        contentWithinWildcard = currentContent.substring(0, currentContent.indexOf(partsOfWildcard[i]));
                    }
                    isFirst = false;
                }
                else {
                    if (subSeq != null) {
                        // Trailing wildcard, with leading wildcard
                        contentWithinWildcard = currentContent
                                .substring(currentContent.indexOf(partsOfWildcard[i]) + partsOfWildcard[i].length());
                    }
                    else {
                        // Trailing wildcard, no leading wildcard
                        contentWithinWildcard = currentContent.substring(partsOfWildcard[i].length());
                    }

                }

                LOGGER.debug("The content within wildcard: <{}> ", contentWithinWildcard);

                if (subSeq == null) {
                    // First wildcard to be processed -> subsequence does not yet exist
                    subSeq = wildcardMatcher.replaceFirst(Matcher.quoteReplacement(contentWithinWildcard));
                }
                else {
                    // Subsequence exists, generate a new matcher for the subsequence and continue building it
                    wildcardMatcher = Pattern.compile("\\*").matcher(subSeq);
                    subSeq = wildcardMatcher.replaceFirst(Matcher.quoteReplacement(contentWithinWildcard));
                }
                LOGGER.debug("Subsequent wildcard: <{}>", subSeq);
            }
        }

        // Return the replacement if no wildcards in WITH clause, otherwise return the subsequence
        // If no regex matches, return the pre-existing content in the input row
        if (isMatch) {
            if (subSeq != null) {
                return subSeq;
            }
            else {
                return replaceWith;
            }

        }
        else {
            return currentContent;
        }

    }

    /**
     * Converts a wildcard statement into a regex statement: all regex-sensitive characters are escaped, and the
     * wildcard (*) gets converted into a regex any character wildcard (.*)
     * 
     * @param wc wildcard statement string
     * @return regex statement string
     */
    private String wcfieldToRegex(String wc) {
        return wc
                .replaceAll("\\\\", "\\\\")
                .replaceAll("\\^", "\\\\^")
                .replaceAll("\\.", "\\\\.")
                .replaceAll("\\|", "\\\\|")
                .replaceAll("\\?", "\\\\?")
                .replaceAll("\\+", "\\\\+")
                .replaceAll("\\{", "\\\\{")
                .replaceAll("\\}", "\\\\}")
                .replaceAll("\\[", "\\\\[")
                .replaceAll("\\]", "\\\\]")
                .replaceAll("\\(", "\\\\(")
                .replaceAll("\\)", "\\\\)")
                .replaceAll("\\$", "\\\\\\$")
                .replaceAll("\\*", ".*");
    }
}
