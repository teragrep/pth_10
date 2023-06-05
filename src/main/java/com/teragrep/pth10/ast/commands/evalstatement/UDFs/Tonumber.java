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

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * UDF for eval method tonumber(numstr, base)<br>
 * Converts a numeric string to an integer of base
 */
public class Tonumber implements UDF2<String, Integer, Integer>, Serializable{
	private static final Logger LOGGER = LoggerFactory.getLogger(Tonumber.class);
	private static final long serialVersionUID = 1L;

	@Override
	public Integer call(String numstr, Integer base) throws Exception {
		
		Integer rv = null;
		
		if (base < 2 || base > 36) {
			throw new UnsupportedOperationException("Tonumber: 'base' argument should be an integer value between 2 and 36.");
		}
		
		// try parsing, otherwise return null
		try {
			rv = Integer.valueOf(numstr, base);
		}
		catch (NumberFormatException nfe) {
			LOGGER.info("Tonumber: Error parsing, returning 'null': " + nfe.getMessage());
			// Could not parse, return null
		}
		
		return rv;
	}

}
