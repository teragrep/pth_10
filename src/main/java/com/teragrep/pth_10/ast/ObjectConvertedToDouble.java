/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
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
package com.teragrep.pth_10.ast;

import java.util.Objects;

public class ObjectConvertedToDouble {

    private final Object object;

    public ObjectConvertedToDouble(final Object object) {
        this.object = object;
    }

    public Double convertToDouble() {
        //remove null
        Double objectAsDouble = null;

        if (object instanceof Long) {
            objectAsDouble = ((Long) object).doubleValue();
        }
        else if (object instanceof Integer) {
            objectAsDouble = ((Integer) object).doubleValue();
        }
        else if (object instanceof Double) {
            objectAsDouble = (Double) object;
        }
        else if (object instanceof Float) {
            objectAsDouble = ((Float) object).doubleValue();
        }
        else if (object instanceof String) {
            try {
                objectAsDouble = Double.valueOf((String) object);
            }
            catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Non-numerical strings are not valid inputs. <" + nfe + " >");
            }
        }
        else {
            throw new RuntimeException(
                    objectAsDouble
                            + " input value couldn't be converted to Double. Expected Long, Integer, Double, Float or String."
            );
        }
        return objectAsDouble;
    }

    @Override
    public boolean equals(Object o) {
        final boolean isEquals;
        if (this == o)
            isEquals = true;
        else if (o == null || getClass() != o.getClass())
            isEquals = false;
        else {
            final ObjectConvertedToDouble convertedObject = (ObjectConvertedToDouble) o;
            isEquals = Objects.equals(object, convertedObject.object);
        }
        return isEquals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(object);
    }
}
