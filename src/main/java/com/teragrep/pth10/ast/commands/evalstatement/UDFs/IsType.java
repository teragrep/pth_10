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
 * UDF used for evals isBool, isInt, isNum and isStr<br>
 * Checks the type of the input against Java types and returns true if it matches or false otherwise
 * 
 * @author eemhu
 */
public class IsType implements UDF1<Object, Boolean>, Serializable {

    private static final long serialVersionUID = 1L;
    private String checkForType = "Boolean";

    public IsType(String type) {
        super();
        this.checkForType = type;
    }

    @Override
    public Boolean call(Object obj) throws Exception {
        switch (checkForType) {
            case "Boolean":
                return (obj instanceof Boolean);
            case "Integer":
                return (obj instanceof Integer);
            case "Numeric":
                return (obj instanceof Integer || obj instanceof Double || obj instanceof Long || obj instanceof Float);
            case "String":
                return (obj instanceof String);
            default:
                throw new UnsupportedOperationException("TypeOf UDF cannot check for type: " + checkForType);
        }
    }
}
