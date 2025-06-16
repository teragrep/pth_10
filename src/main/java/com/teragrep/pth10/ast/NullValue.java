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
package com.teragrep.pth10.ast;

import java.io.Serializable;

/**
 * Object, that is used to provide the value to be used as the null value across the project.
 */
public class NullValue implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * <p>
     * Defines which type of null to use as the null value.
     * </p>
     * <ul>
     * <li>{@link Type#DEFAULT_NULL}: Java <code>null</code> value</li>
     * <li>{@link Type#EMPTY_STRING}: <code>""</code> string</li>
     * <li>{@link Type#NULL_AS_STRING}: <code>"null"</code> string</li>
     * </ul>
     */
    public enum Type {
        DEFAULT_NULL, EMPTY_STRING, NULL_AS_STRING
    }

    private final Type type;

    public NullValue() {
        this.type = Type.DEFAULT_NULL;
    }

    public NullValue(Type type) {
        this.type = type;
    }

    public String value() {
        switch (type) {
            case DEFAULT_NULL:
                return null;
            case EMPTY_STRING:
                return "";
            case NULL_AS_STRING:
                return "null";
        }
        throw new IllegalStateException("Invalid Null type given: " + type);
    }

    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return value();
    }
}
