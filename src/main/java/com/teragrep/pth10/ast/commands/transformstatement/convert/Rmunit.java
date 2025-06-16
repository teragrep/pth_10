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
package com.teragrep.pth10.ast.commands.transformstatement.convert;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UDF for convert command 'rmunit'<br>
 * Removes trailing non-numeric characters, e.g. 123abc results in 123<br>
 * Exponential numbers are supported, and numbers beginning with +/- signs and dots (.)<br>
 * Also numbers beginning with 0 are supported.<br>
 * Only one decimal place is supported. (e.g. 100.000.000 is not a valid number).
 * 
 * @author eemhu
 */
public class Rmunit implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(Rmunit.class);
    private static final Pattern p = Pattern.compile("^(\\+|-)?[0-9]*((\\.)[0-9]*)?((e|E)[0-9]+)?");

    @Override
    public String call(String value) throws Exception {
        String rv;
        final StringBuilder sb = new StringBuilder();

        // special cases
        boolean firstWasZero = false;
        boolean secondWasZero = false;

        // match against pattern (+|-)? [0-9]* ((.)? [0-9]*)* (e[0-9]+)?
        Matcher m = p.matcher(value);
        if (!m.find()) {
            return ""; // on fail, return empty
        }

        // Go through input, removing trailing characters if needed
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (i == 0) {
                // first one can be ./+/-/digit
                if (c == '0') {
                    firstWasZero = true;
                    sb.append(c);
                    continue;
                }
                else if (c == '+' || c == '-' || c == '.' || Character.isDigit(c)) {
                    sb.append(c);
                    continue;
                }
                else {
                    return "";
                }
            }
            else if (i == 1) {
                //Second one can be ./digit
                if (c == '.') {
                    if (firstWasZero) {
                        // 0. -> .
                        sb.replace(0, 1, ".");
                    }
                    else {
                        sb.append(".");
                    }
                    continue;
                }
                else if (c == '0') {
                    secondWasZero = true;
                    sb.append(c);
                    continue;
                }
                else if (Character.isDigit(c)) {
                    // this is ok
                    sb.append(c);
                    continue;
                }
                else {
                    return "";
                }
            }
            else if (i == 2) {
                if (c == '.' && secondWasZero && !firstWasZero) {
                    // (+/-)0. -> (+/-).
                    sb.replace(1, 2, ".");
                    continue;
                }
            }
            else if (!Character.isDigit(c) && c != '+' && c != '-' && c != '.' && c != 'e' && c != 'E') {
                // break off on non-digit (excluding +/-/./e/E)
                break;
            }

            sb.append(c);
        }

        // check final char (special case: '0.000e'; trailing exponent)
        String cleanedUpString = sb.toString();
        char finalChar = cleanedUpString.charAt(cleanedUpString.length() - 1);
        if (finalChar == 'e' || finalChar == 'E') {
            cleanedUpString = cleanedUpString.substring(0, cleanedUpString.length() - 1);
        }

        // build pattern used by DecimalFormat
        StringBuilder pattern = new StringBuilder();
        boolean previousCharWasExponent = false;
        char exponentSign = ' ';
        for (int i = 0; i < cleanedUpString.length(); i++) {
            char c = cleanedUpString.charAt(i);
            if (i == 0) {
                if (c == '+' || c == '.') { // '-' can't be added, as DecimalFormat automatically adds it
                    pattern.append(c);
                }
                else if (Character.isDigit(c)) {
                    pattern.append('0');
                }
            }
            else if (i == 1) {
                if (c == '.') {
                    pattern.append(c);
                }
                else if (Character.isDigit(c)) {
                    pattern.append('0');
                }
            }
            else {
                if (Character.isDigit(c)) {
                    pattern.append('0');
                }
                else if (c == 'e' || c == 'E') { // DecimalFormat expects capitalized E
                    pattern.append('E');
                    previousCharWasExponent = true;
                }
                else if (previousCharWasExponent) {
                    // don't add +/- after E in pattern since DecimalFormat does not expect them
                    if (c != '+' && c != '-') {
                        sb.append(c);
                    }
                    else {
                        exponentSign = c;
                    }
                    previousCharWasExponent = false;
                }
                else {
                    pattern.append(c);
                }
            }
        }

        LOGGER.debug("rmunit-Convert: <{}>", cleanedUpString);
        LOGGER.debug("rmunit-Pattern: <{}>", pattern);
        try {
            rv = new DecimalFormat(pattern.toString()).format(new BigDecimal(cleanedUpString));

            // Even though the pattern is "0.00E0", DecimalFormat causes "0.45E2" to be converted into ".45E2"
            // which is incorrect. These checks will avoid that.
            if (firstWasZero) {
                if (rv.charAt(0) != '0') {
                    rv = '0' + rv;
                }
            }

            if (secondWasZero) {
                if (rv.charAt(1) != '0') {
                    String leftSide = rv.substring(0, 1);
                    String rightSide = rv.substring(1);
                    rv = leftSide + '0' + rightSide;
                }
            }

            // DecimalFormat ignores E+ like this:
            // E+00 -> E00 so add + back
            // E-00 -> E-00, no need to add
            if (exponentSign == '+') {
                int smallE = rv.indexOf("e");
                int bigE = rv.indexOf("E");

                if (smallE != -1) {
                    String leftSide = rv.substring(0, smallE + 1);
                    String rightSide = rv.substring(smallE + 1);
                    rv = leftSide + exponentSign + rightSide;

                }
                else if (bigE != -1) {
                    String leftSide = rv.substring(0, bigE + 1);
                    String rightSide = rv.substring(bigE + 1);
                    rv = leftSide + exponentSign + rightSide;
                }
            }
        }
        catch (Exception e) {
            // return empty on fail
            LOGGER.error(e.getMessage());
            rv = "";
        }

        return rv;
    }
}
