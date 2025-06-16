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
package com.teragrep.pth10.ast.commands.evalstatement.UDFs;

import com.teragrep.pth10.steps.ParsedResult;
import com.teragrep.pth10.steps.TypeParser;
import com.teragrep.pth_03.antlr.DPLLexer;
import org.apache.spark.sql.api.java.UDF3;

import java.math.BigDecimal;
import java.util.List;

/**
 * UDF for comparing two fields.
 */
public class EvalOperation implements UDF3<Object, Integer, Object, Boolean> {

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

        // Only two numbers or two strings allowed. Throw error if mixed.
        if (
            (leftType == ParsedResult.Type.STRING
                    && (rightType == ParsedResult.Type.DOUBLE || rightType == ParsedResult.Type.LONG))
                    || (rightType == ParsedResult.Type.STRING
                            && (leftType == ParsedResult.Type.DOUBLE || leftType == ParsedResult.Type.LONG))
        ) {
            throw new IllegalArgumentException("Eval comparisons only allow using two numbers or two strings.");
        }
        // If both are Strings
        else if (leftType == ParsedResult.Type.STRING && rightType == ParsedResult.Type.STRING) {
            String leftString = left.getString();
            String rightString = right.getString();

            switch (operationType) {
                case DPLLexer.EVAL_LANGUAGE_MODE_EQ:
                case DPLLexer.EVAL_LANGUAGE_MODE_DEQ: {
                    rv = leftString.compareTo(rightString) == 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                    rv = leftString.compareTo(rightString) != 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                    rv = leftString.compareTo(rightString) > 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                    rv = leftString.compareTo(rightString) >= 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                    rv = leftString.compareTo(rightString) < 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LTE: {
                    rv = leftString.compareTo(rightString) <= 0;
                    break;
                }
                default: {
                    throw new RuntimeException("EvalStatement: Unknown operation in EvalOperation");
                }
            }
        }
        // If both are numbers
        else if (
            (leftType == ParsedResult.Type.DOUBLE || leftType == ParsedResult.Type.LONG)
                    && (rightType == ParsedResult.Type.DOUBLE || rightType == ParsedResult.Type.LONG)
        ) {
            // change left and right numbers into BigDecimal
            BigDecimal leftNumber = left.getType() == ParsedResult.Type.DOUBLE ? BigDecimal
                    .valueOf(left.getDouble()) : BigDecimal.valueOf(left.getLong());
            BigDecimal rightNumber = right.getType() == ParsedResult.Type.DOUBLE ? BigDecimal
                    .valueOf(right.getDouble()) : BigDecimal.valueOf(right.getLong());

            switch (operationType) {
                case DPLLexer.EVAL_LANGUAGE_MODE_EQ:
                case DPLLexer.EVAL_LANGUAGE_MODE_DEQ: {
                    rv = leftNumber.compareTo(rightNumber) == 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_NEQ: {
                    rv = leftNumber.compareTo(rightNumber) != 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_GT: {
                    rv = leftNumber.compareTo(rightNumber) > 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_GTE: {
                    rv = leftNumber.compareTo(rightNumber) >= 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LT: {
                    rv = leftNumber.compareTo(rightNumber) < 0;
                    break;
                }
                case DPLLexer.EVAL_LANGUAGE_MODE_LTE: {
                    rv = leftNumber.compareTo(rightNumber) <= 0;
                    break;
                }
                default: {
                    throw new RuntimeException("EvalStatement: Unknown operation in EvalOperation");
                }
            }
        }
        else if (leftType == ParsedResult.Type.LIST && rightType == ParsedResult.Type.LIST) {
            // both lists; check internal elements to match
            List<?> leftList = left.getList();
            List<?> rightList = right.getList();
            rv = leftList.equals(rightList);

        }
        else if (
            (leftType == ParsedResult.Type.LIST && rightType == ParsedResult.Type.STRING)
                    || (leftType == ParsedResult.Type.STRING && rightType == ParsedResult.Type.LIST)
        ) {
            // one is list, other string
            // in this case check if list contains string
            List<?> list;
            String str;
            if (leftType == ParsedResult.Type.LIST) {
                list = left.getList();
                str = right.getString();
            }
            else {
                list = right.getList();
                str = left.getString();
            }

            rv = list.contains(str);
        }
        else {
            throw new IllegalArgumentException("Eval comparison: Unsupported datatype detected");
        }

        return rv;
    }
}
