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

import org.apache.spark.sql.api.java.UDF3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

/**
 * UDF used for command sigfig(x)<br>
 * <p>
 * The computation for sigfig is based on the type of calculation that generates the number:
 * </p>
 * <p>
 * * / result should have minimum number of significant figures of all of the operands<br>
 * + - result should have the same amount of sigfigs as the least precise number of all of the operands
 * </p>
 * 
 * @author eemhu
 */
public class Sigfig implements UDF3<Object, String, WrappedArray<Object>, Double>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sigfig.class);

    private static final long serialVersionUID = 1L;

    // Characters used for different arithmetic operations
    private static final char MULTIPLICATION_CHAR = '*';
    private static final char DIVISION_CHAR = '/';
    private static final char ADDITION_CHAR = '+';
    private static final char SUBTRACTION_CHAR = '-';

    @Override
    public Double call(Object calcResAsObject, String calcText, WrappedArray<Object> wrappedArrayOfColObjects)
            throws Exception {

        BigDecimal calcRes = new BigDecimal(calcResAsObject.toString());

        // Get the iterator for WrappedArray
        Iterator<Object> itr = wrappedArrayOfColObjects.iterator();
        List<BigDecimal> calculatedCols = new ArrayList<>();

        // Get all objects (numbers) as BigDecimal with the iterator
        // and put them into calculatedCols java list
        // BigDecimal retains insignificant digits unlike double
        while (itr.hasNext()) {
            Object obj = itr.next();

            if (obj instanceof Long) {
                calculatedCols.add(BigDecimal.valueOf((Long) obj));
            }
            else if (obj instanceof Integer) {
                calculatedCols.add(BigDecimal.valueOf((Integer) obj));
            }
            else if (obj instanceof Float) {
                calculatedCols.add(BigDecimal.valueOf((Float) obj));
            }
            else if (obj instanceof Double) {
                calculatedCols.add(BigDecimal.valueOf((Double) obj));
            }
            else if (obj instanceof String) {
                calculatedCols.add(new BigDecimal((String) obj));
            }
            else {
                // Throw exception if number was not any of the types tested above
                throw new RuntimeException("Sigfig: Could not parse field content into java.math.BigDecimal");
            }
        }

        // The computation for sigfig is based on the type of calculation that generates the number
        // * / result should have minimum number of significant figures of all of the operands
        // + - result should have the same amount of sigfigs as the least precise number of all of the operands

        double rv;
        BigDecimal input = calcRes; //BigDecimal.valueOf(calcRes);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("calc(result)= <{}>", calcRes);
            LOGGER.debug("calc(text)= <{}>", calcText);
        }

        int multiIndex = calcText.indexOf(MULTIPLICATION_CHAR);
        int divIndex = calcText.indexOf(DIVISION_CHAR);
        int plusIndex = calcText.indexOf(ADDITION_CHAR);
        int minusIndex = calcText.indexOf(SUBTRACTION_CHAR);

        // * or /
        if (multiIndex != -1 || divIndex != -1) {
            int minPrecision = Integer.MAX_VALUE;

            for (BigDecimal val : calculatedCols) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("val=<{}>", val);
                }

                BigDecimal currentValue = val;
                int scale = currentValue.scale();
                int precision = currentValue.precision();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("scale=<{}> precision=<{}>", scale, precision);
                }

                if (precision < minPrecision) {
                    minPrecision = precision;
                }
            }

            rv = input.round(new MathContext(minPrecision)).doubleValue();
        }
        // + or -
        else if (plusIndex != -1 || minusIndex != -1) {
            int minScale = Integer.MAX_VALUE;

            for (BigDecimal val : calculatedCols) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("val=<{}>", val);
                }

                BigDecimal currentValue = val;
                int scale = currentValue.scale();
                int precision = currentValue.precision();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("scale=<{}> precision=<{}>", scale, precision);
                }

                if (scale < minScale) {
                    minScale = scale;
                }
            }

            rv = input.round(new MathContext(minScale)).doubleValue();
        }
        else {
            // No * / + - found
            throw new RuntimeException("Sigfig: Could not determine the type of arithmetic operation");
        }

        return rv;
    }
}
