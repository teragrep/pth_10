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
package com.teragrep.pth_10;

import com.teragrep.pth_10.ast.FilteredColumns;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class FilteredColumnsTest {

    @Test
    public void testWildcard() {
        String field = "fo*";
        String[] cols = new String[] {
                "foo", "bar"
        };
        String expected = "foo";

        List<String> actual = new FilteredColumns(field, cols).filtered();
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.get(0));
    }

    @Test
    public void multipleWildcardTest() {
        String field = "b*f*";
        String[] cols = new String[] {
                "a", "barfoo"
        };
        String expected = "barfoo";

        List<String> actual = new FilteredColumns(field, cols).filtered();
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.get(0));
    }

    @Test
    public void testSingleWildcard() {
        String field = "*";
        String[] cols = new String[] {
                "foo", "bar"
        };
        List<String> expected = Arrays.asList("foo", "bar");

        List<String> actual = new FilteredColumns(field, cols).filtered();
        Assertions.assertEquals(2, actual.size());
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testWithoutWildcard() {
        String field = "bar";
        String[] cols = new String[] {
                "foo", "bar"
        };
        String expected = "bar";

        List<String> actual = new FilteredColumns(field, cols).filtered();
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.get(0));
    }

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(FilteredColumns.class).verify();
    }

    @Test
    public void testNotEquals() {
        String field1 = "foo";
        String field2 = "bar";
        String[] cols = new String[] {
                "foo", "bar"
        };
        Assertions
                .assertNotEquals(new FilteredColumns(field1, cols).filtered(), new FilteredColumns(field2, cols).filtered());
    }
}
