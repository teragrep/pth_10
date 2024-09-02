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
import java.util.ArrayList;
import java.util.List;

/**
 * UDF for command commands(x)<br>
 * x is a string or field containing a search string<br>
 * Returns a mvfield with all the commands used in x
 *
 * @author eemhu
 */
public class Commands implements UDF1<String, List<String>>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public List<String> call(String searchStr) throws Exception {
		List<String> rv = new ArrayList<>();
		
		// Running this command should result in following input->output:
		// example: "search foo | stats count | sort count" -> search, stats, sort
		
		// should result in "search foo", "stats count", "sort count"
		String[] cmds = searchStr.split("\\|");
			
		// remove unnecessary whitespace and create mv field (Spark ArrayType)
		for (int i = 0; i < cmds.length; i++) {
			String currentCmd = cmds[i].trim();
			rv.add(currentCmd.substring(0, currentCmd.indexOf(' ')));
		}
		
		return rv;
	}
}
