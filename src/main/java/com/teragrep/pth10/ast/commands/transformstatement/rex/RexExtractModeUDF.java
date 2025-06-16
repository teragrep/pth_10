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
package com.teragrep.pth10.ast.commands.transformstatement.rex;

import com.teragrep.jpr_01.JavaPcre;
import org.apache.spark.sql.api.java.UDF2;

import java.util.HashMap;
import java.util.Map;

public class RexExtractModeUDF implements UDF2<String, String, Map<String, String>> {

    @Override
    public Map<String, String> call(String inputStr, String regexStr) throws Exception {
        // Create JavaPCRE instance
        final JavaPcre pcre = new JavaPcre();

        // Compile regex matcher for given pattern
        pcre.compile_java(regexStr);

        // Match on input string
        pcre.singlematch_java(inputStr, 0);

        // Maps containing matches and names for capture groups
        final Map<Integer, String> matchTable = pcre.get_match_table();
        final Map<String, Integer> nameTable = pcre.get_name_table();

        // save results on name, value map
        final Map<String, String> results = new HashMap<>();
        for (final Map.Entry<String, Integer> me : nameTable.entrySet()) {
            final String value = matchTable.get(me.getValue());
            final String name = me.getKey();
            results.put(name, value);
        }

        // clean compile
        // Note: Can throw IllegalStateException if nothing to free but should
        // realistically never happen as it is only called after compile
        pcre.jcompile_free();

        return results;
    }
}
