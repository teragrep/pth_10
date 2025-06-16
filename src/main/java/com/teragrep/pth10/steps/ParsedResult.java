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
package com.teragrep.pth10.steps;

import java.io.Serializable;

import java.util.List;

/**
 * Wrapper class for the result of parsing a String that might be a number.
 */
public class ParsedResult implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {
        LONG, DOUBLE, STRING, LIST
    }

    private Long longValue = null;
    private Double doubleValue = null;
    private String string = null;
    private List<String> list = null;
    private final Type type;

    public ParsedResult(Long longValue) {
        this.longValue = longValue;
        this.type = Type.LONG;
    }

    public ParsedResult(Double doubleValue) {
        this.doubleValue = doubleValue;
        this.type = Type.DOUBLE;
    }

    public ParsedResult(List<String> list) {
        this.list = list;
        this.type = Type.LIST;
    }

    public ParsedResult(String string) {
        this.string = string;
        this.type = Type.STRING;
    }

    public long getLong() {
        return this.longValue;
    }

    public double getDouble() {
        return this.doubleValue;
    }

    public String getString() {
        return this.string;
    }

    public Type getType() {
        return this.type;
    }

    public List<String> getList() {
        return this.list;
    }

}
