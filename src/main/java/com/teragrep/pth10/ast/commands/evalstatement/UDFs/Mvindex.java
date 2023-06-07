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

import org.apache.spark.sql.api.java.UDF3;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;

/**
 * UDF for command mvindex(mvfield, startindex, endindex)<br>
 * Returns the values between startindex and endindex (inclusive)<br>
 * Indices can be negative, first value is 0 and last element can be accessed with -1.<br>
 * @author p000043u
 *
 */
public class Mvindex implements UDF3<WrappedArray<String>, Integer, Integer, WrappedArray<String>>, Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public WrappedArray<String> call(WrappedArray<String> mvField, Integer startIndex, Integer endIndex) throws Exception {
		
		// If endIndex was not given or it was set to -1, default to last element
		if (endIndex == null || endIndex == -1) {
			endIndex = mvField.size() - 1;
		}
		
		// Drop elements from left and right based on given indices
		// However, if nothing is to be dropped, don't even call the drop()/dropRight() function
		
		int nDropFromRight = (mvField.size() - (endIndex+1));
		if (nDropFromRight > 0) mvField = (WrappedArray<String>) mvField.dropRight(nDropFromRight);
		
		int nDropFromLeft = startIndex;
		if (nDropFromLeft > 0) mvField = (WrappedArray<String>) mvField.drop(nDropFromLeft);
		
		
		return mvField;
	}

}
