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

import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;

/**
 * UDF for acosh(x), asinh(x) and atanh(x)<br>
 * Spark built-in functions exist in version {@literal >=3.1.0}
 * 
 * @author eemhu
 */
public class InverseHyperbolicFunction implements UDF1<Object, Double>, Serializable {

    private static final long serialVersionUID = 1L;
    private String function = "acosh";

    public InverseHyperbolicFunction(String function) {
        super();
        this.function = function;
    }

    @Override
    public Double call(Object x) throws Exception {
        // Take x in as Object, so it can be multiple different types
        // instead of making one UDF for each possible type
        Double xAsDouble = convertObjectToDouble(x);

        switch (function) {
            case "acosh": {
                // Use Apache Commons function for acosh
                org.apache.commons.math3.analysis.function.Acosh acoshFunction = new org.apache.commons.math3.analysis.function.Acosh();

                return acoshFunction.value(xAsDouble);
            }
            case "asinh": {
                org.apache.commons.math3.analysis.function.Asinh asinhFunction = new org.apache.commons.math3.analysis.function.Asinh();

                return asinhFunction.value(xAsDouble);
            }
            case "atanh": {
                org.apache.commons.math3.analysis.function.Atanh atanhFunction = new org.apache.commons.math3.analysis.function.Atanh();

                return atanhFunction.value(xAsDouble);
            }
            default: {
                throw new RuntimeException("Invalid inverse hyperbolic function: " + function);
            }
        }

    }

    private Double convertObjectToDouble(Object x) {
        Double xAsDouble = null;

        if (x instanceof Long) {
            xAsDouble = ((Long) x).doubleValue();
        }
        else if (x instanceof Integer) {
            xAsDouble = ((Integer) x).doubleValue();
        }
        else if (x instanceof Double) {
            xAsDouble = (Double) x;
        }
        else if (x instanceof Float) {
            xAsDouble = ((Float) x).doubleValue();
        }
        else if (x instanceof String) {
            xAsDouble = Double.valueOf((String) x);
        }
        else {
            throw new RuntimeException(
                    this.function
                            + " input value couldn't be converted to Double. Expected Long, Integer, Double, Float or String."
            );
        }

        return xAsDouble;
    }

}
