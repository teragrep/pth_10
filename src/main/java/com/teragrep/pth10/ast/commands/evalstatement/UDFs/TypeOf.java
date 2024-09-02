/*
 * Teragrep DPL to Catalyst Translator PTH-10
 * Copyright (C) 2019, 2020, 2021, 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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
 * <p>UDF used for eval typeof()</p>
 * <pre>
 *     Java Type 	| TypeOf Result
 *     ----------------------------------
 *     Boolean 		| Boolean
 *     Integer 		| Number
 *     Long 		| Number
 *     Double 		| Number
 *     Float 		| Number
 *     Timestamp 	| Number
 *     String 		| String
 *     (Other) 		| Invalid
 * </pre>
 * @author eemhu
 *
 */
public class TypeOf implements UDF1<Object, String>, Serializable {

	private static final long serialVersionUID = 1L;
	
	@Override
	public String call(Object obj) throws Exception {
		
		if (obj instanceof Boolean) {
			return "Boolean";
		}
		else if (obj instanceof Integer || obj instanceof Long || obj instanceof Double || obj instanceof Float || obj instanceof java.sql.Timestamp) {
			// timestamp is also a number based on docs
			return "Number";
		}
		else if (obj instanceof String) {
			return "String";
		}
		else {
			return "Invalid";
		}
		
	}
}
