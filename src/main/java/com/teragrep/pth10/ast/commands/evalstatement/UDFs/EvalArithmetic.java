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
import org.apache.spark.sql.api.java.UDF3;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Checks if left and right side are longs/doubles, and performs basic arithmetic on them if they are, otherwise
 * concatenate them as strings
 */
public class EvalArithmetic implements UDF3<Object, String, Object, String> {

    @Override
    public String call(Object l, String op, Object r) throws Exception {
        // try long
        TypeParser typeParser = new TypeParser();
        ParsedResult left = typeParser.parse(l);
        ParsedResult right = typeParser.parse(r);

        // Check for Strings, concatenate if found
        if (left.getType() == ParsedResult.Type.STRING || right.getType() == ParsedResult.Type.STRING) {
            if (op.equals("+")) {
                return l.toString().concat(r.toString());
            }
            else {
                throw new IllegalArgumentException("Eval arithmetics only allow Strings for the + operator.");
            }
        }

        // change left and right numbers into BigDecimal
        BigDecimal leftNumber = left.getType() == ParsedResult.Type.DOUBLE ? BigDecimal
                .valueOf(left.getDouble()) : BigDecimal.valueOf(left.getLong());
        BigDecimal rightNumber = right.getType() == ParsedResult.Type.DOUBLE ? BigDecimal
                .valueOf(right.getDouble()) : BigDecimal.valueOf(right.getLong());

        switch (op) {
            case "+":
                return leftNumber.add(rightNumber).stripTrailingZeros().toPlainString();
            case "-":
                return leftNumber.subtract(rightNumber).stripTrailingZeros().toPlainString();
            case "*":
                return leftNumber.multiply(rightNumber).stripTrailingZeros().toPlainString();
            case "/":
                try {
                    return leftNumber.divide(rightNumber).stripTrailingZeros().toPlainString();
                }
                catch (ArithmeticException e) {
                    // show 7 first decimals if the result of the division is a repeating number
                    return leftNumber.divide(rightNumber, 7, RoundingMode.HALF_UP).toPlainString();
                }
            case "%":
                return leftNumber.remainder(rightNumber).stripTrailingZeros().toPlainString();
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + op);
        }
    }
}
