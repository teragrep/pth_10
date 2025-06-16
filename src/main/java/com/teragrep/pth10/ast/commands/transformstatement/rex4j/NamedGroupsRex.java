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
package com.teragrep.pth10.ast.commands.transformstatement.rex4j;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used to get the capture groups for the rex4j command, which means which regex will match which new generated
 * field.<br>
 * Syntax example:<br>
 * {@literal .*latitude\":\s(?<latiTUDE>-?\d+.\d+)}<br>
 * Would return anything matching ' latitude": 0.0 ' as a new field latiTUDE with the 0.0 being the contents of that new
 * generated field.
 */
public class NamedGroupsRex {

    /**
     * Gets multiple new groups
     * 
     * @param regex Regex and group
     * @return map with group and group index
     */
    public static Map<String, Integer> getNamedGroups(String regex) {
        List<String> namedGroups = new ArrayList<>();
        Map<String, Integer> rv = new LinkedHashMap<>();
        HashMap<String, Integer> offsets = new HashMap<>();
        Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);
        int ind = 1;
        while (m.find()) {
            namedGroups.add(m.group(1));
            //            LOGGER.info(m.group()+" groupCount"+(ind));
            rv.put(m.group(1), ind++);
        }
        return rv;
    }

    /**
     * Gets a single group
     * 
     * @param regex Regex and group
     * @return group
     */
    public static String getNamedGroup(String regex) {
        Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);

        if (m.find()) {
            return m.group(1);
        }

        return null;
    }

}
