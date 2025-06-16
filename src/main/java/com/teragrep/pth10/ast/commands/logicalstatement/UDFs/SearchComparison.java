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
package com.teragrep.pth10.ast.commands.logicalstatement.UDFs;

import com.teragrep.jue_01.GlobToRegEx;
import com.teragrep.pth10.steps.ParsedResult;
import com.teragrep.pth10.steps.TypeParser;
import com.teragrep.pth_03.antlr.DPLLexer;
import org.apache.spark.sql.api.java.UDF3;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UDF for comparing a field to a value in a search command.
 */
public class SearchComparison implements UDF3<Object, Integer, Object, Boolean> {

    @Override
    public Boolean call(Object l, Integer operationType, Object r) throws Exception {
        // Parse in case a number has been set to a String
        // (which would lead to a wrong result)
        TypeParser typeParser = new TypeParser();
        ParsedResult left = typeParser.parse(l);
        ParsedResult right = typeParser.parse(r);

        ParsedResult.Type leftType = left.getType();
        ParsedResult.Type rightType = right.getType();

        boolean rv;

        // if left or right is a string, compare lexicographically (as if both were strings)
        if (leftType == ParsedResult.Type.STRING || rightType == ParsedResult.Type.STRING) {
            String leftString;
            String rightString;

            // get left as string
            if (leftType == ParsedResult.Type.STRING) {
                leftString = left.getString();
            }
            else if (leftType == ParsedResult.Type.DOUBLE) {
                leftString = BigDecimal.valueOf(left.getDouble()).toPlainString();
            }
            else {
                leftString = BigDecimal.valueOf(left.getLong()).toPlainString();
            }

            // get right as string
            if (rightType == ParsedResult.Type.STRING) {
                rightString = right.getString();
            }
            else if (rightType == ParsedResult.Type.DOUBLE) {
                // make into a string through BigDecimal to get it exactly as written in the command
                rightString = BigDecimal.valueOf(right.getDouble()).stripTrailingZeros().toPlainString();
            }
            else {
                rightString = BigDecimal.valueOf(right.getLong()).stripTrailingZeros().toPlainString();
            }

            switch (operationType) {
                case DPLLexer.EQ:
                case DPLLexer.EVAL_LANGUAGE_MODE_EQ: {
                    //rv = source.equalTo(value);
                    rightString = GlobToRegEx.regexify(rightString); //wildcard support
                    Pattern p = Pattern.compile(rightString);
                    Matcher m = p.matcher(leftString);
                    rv = m.matches();
                    break;
                }
                case DPLLexer.NEQ:
                case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                    //rv = source.notEqual(value);
                    rightString = GlobToRegEx.regexify(rightString); //wildcard support
                    Pattern p = Pattern.compile(rightString);
                    Matcher m = p.matcher(leftString);
                    rv = !m.matches();
                    break;
                }
                case DPLLexer.GT:
                case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                    rv = leftString.compareTo(rightString) > 0;
                    break;
                }
                case DPLLexer.GTE:
                case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                    rv = leftString.compareTo(rightString) >= 0;
                    break;
                }
                case DPLLexer.LT:
                case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                    rv = leftString.compareTo(rightString) < 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LTE:
                case DPLLexer.LTE: {
                    rv = leftString.compareTo(rightString) <= 0;
                    break;
                }
                default: {
                    throw new RuntimeException("CompareStatement: Unknown operation in Comparison");
                }
            }
        }
        else {
            BigDecimal leftNumber = left.getType() == ParsedResult.Type.DOUBLE ? BigDecimal
                    .valueOf(left.getDouble()) : BigDecimal.valueOf(left.getLong());
            BigDecimal rightNumber = right.getType() == ParsedResult.Type.DOUBLE ? BigDecimal
                    .valueOf(right.getDouble()) : BigDecimal.valueOf(right.getLong());

            switch (operationType) {
                case DPLLexer.EQ:
                case DPLLexer.EVAL_LANGUAGE_MODE_EQ: {
                    rv = leftNumber.compareTo(rightNumber) == 0;
                    break;
                }
                case DPLLexer.NEQ:
                case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                    rv = leftNumber.compareTo(rightNumber) != 0;
                    break;
                }
                case DPLLexer.GT:
                case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                    rv = leftNumber.compareTo(rightNumber) > 0;
                    break;
                }
                case DPLLexer.GTE:
                case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                    rv = leftNumber.compareTo(rightNumber) >= 0;
                    break;
                }
                case DPLLexer.LT:
                case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                    rv = leftNumber.compareTo(rightNumber) < 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LTE:
                case DPLLexer.LTE: {
                    rv = leftNumber.compareTo(rightNumber) <= 0;
                    break;
                }
                default: {
                    throw new RuntimeException("CompareStatement: Unknown operation in Comparison");
                }
            }
        }

        return rv;
    }
}
