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
package com.teragrep.pth10.ast.commands.transformstatement.rex;

import com.teragrep.jpr_01.JavaPcre;
import org.apache.spark.sql.api.java.UDF2;

import java.util.ArrayList;
import java.util.List;

public class RexSedModeUDF implements UDF2<String, String, String> {

    @Override
    public String call(String inputStr, String sedStr) throws Exception {
        JavaPcre pcre = new JavaPcre();

        // regex string is now "s/original/replacement/[g|Ng|N]", n>0
        // check for correct form
        CheckedSedString checkedSedString = new CheckedSedString(sedStr);

        // compile match pattern
        pcre.compile_java(checkedSedString.toRegexString());

        List<int[]> offsets;
        if (checkedSedString.globalMode()) {
            // global mode
            offsets = getAllOccurrences(pcre, inputStr, checkedSedString.replaceOccurrencesAmount());
        }
        else {
            // replace nth occurrence mode
            offsets = getUpToNthOccurrence(pcre, inputStr, checkedSedString.replaceOccurrencesAmount());
            // only need the last nth occurrence
            offsets = offsets.subList(offsets.size() - 1, offsets.size());
        }

        StringBuilder resultStrBuilder = new StringBuilder();
        int processedUntilInd = 0; // processed until which index of the input string?
        for (int i = 0; i < offsets.size(); ++i) {
            // get offsets of match and replace with replacement
            final int beginInd = offsets.get(i)[0];
            final int endInd = offsets.get(i)[1];
            //System.out.printf("Processing offset %s,%s with input '%s'\n", beginInd, endInd, inputStr);

            for (int j = processedUntilInd; j < inputStr.length(); ++j) {
                if (j == beginInd) {
                    // add replacement on beginning index
                    resultStrBuilder.append(checkedSedString.toReplacementString());
                }
                else if (j > beginInd && j < endInd) {
                    // part-to-be-replaced, skip chars
                    continue;
                }
                else if (j == endInd && i == offsets.size() - 1) {
                    // final set of offsets, add the remaining bits of the input
                    resultStrBuilder.append(inputStr.charAt(j));
                }
                else if (j == endInd) {
                    // processed until end of replacement, move to next set of offsets
                    processedUntilInd = endInd;
                    break;
                }
                else {
                    // outside the part-to-be-replaced; add original characters
                    resultStrBuilder.append(inputStr.charAt(j));
                }
            }
            //System.out.println("Built string thus far: " + resultStrBuilder.toString());
        }
        return resultStrBuilder.toString();
    }

    private List<int[]> getUpToNthOccurrence(JavaPcre jPcre, String input, int n) {
        int offset = 0;
        int start = 0;

        List<int[]> offsets = new ArrayList<>();

        for (int i = 0; i < n; ++i) {
            jPcre.singlematch_java(input, offset);

            start = jPcre.get_ovector0();
            offset = jPcre.get_ovector1();

            //System.out.printf("Match found (%s, %s): '%s'\n", start, offset, input.substring(start, offset));
            offsets.add(new int[] {
                    start, offset
            });
        }

        return offsets;
    }

    private List<int[]> getAllOccurrences(JavaPcre jPcre, String input, int from) {
        List<int[]> offsets = new ArrayList<>();
        int start = 0;
        int offset = 0;

        jPcre.singlematch_java(input, offset);
        int count = 1;
        while (jPcre.get_matchfound()) {
            start = jPcre.get_ovector0();
            offset = jPcre.get_ovector1();

            if (count >= from) {
                offsets.add(new int[] {
                        start, offset
                });
            }
            //System.out.printf("Match found (%s, %s): '%s'\n", start, offset, input.substring(start, offset));
            jPcre.singlematch_java(input, offset);
            count++;
        }

        return offsets;
    }
}
