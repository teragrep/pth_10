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
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * UDF for command mvzip(x, y, "z")<br>
 * x = mv field 1<br>
 * y = mv field 2<br>
 * z = (optional) delimiter, defaults to comma<br>
 * 
 * @author eemhu
 */
public class Mvzip implements UDF3<WrappedArray<String>, WrappedArray<String>, String, List<String>>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public List<String> call(WrappedArray<String> mvfield1, WrappedArray<String> mvfield2, String delimiter)
            throws Exception {
        // If delimiter is null, replace with default.
        // Should not happen in practice since it is already checked in EvalStatement.evalMethodMvzipEmitCatalyst()
        if (delimiter == null) {
            delimiter = ",";
        }

        // iterators for both mvfields
        Iterator<String> it1 = mvfield1.iterator();
        Iterator<String> it2 = mvfield2.iterator();

        // Go through the arrays and add to zipped list
        List<String> zipped = new ArrayList<String>();
        while (it1.hasNext() && it2.hasNext()) {
            zipped.add(it1.next() + delimiter + it2.next());
        }

        return zipped;
    }

}
