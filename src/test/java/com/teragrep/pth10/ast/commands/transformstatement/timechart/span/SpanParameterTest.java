/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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
package com.teragrep.pth10.ast.commands.transformstatement.timechart.span;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpanParameterTest {

    @Test
    public void testEqualsVerifier() {
        // equalsVerifier flags Spark's Column as a recursive data structure, have to use prefabs
        EqualsVerifier
                .forClass(SpanParameter.class)
                .withNonnullFields("column", "ival")
                .withPrefabValues(Column.class, new Column("_time"), functions.lit(""))
                .verify();
    }

    @Test
    public void testAsColumn() {
        TimeRange timeRange = new TimeRange("5 minutes");
        SpanParameter spanParameter = new SpanParameter(timeRange);

        long microSeconds = timeRange.asSeconds() * 1000 * 1000;
        Column expectedColumn = functions
                .window(new Column("_time"), String.valueOf(new CalendarInterval(0, 0, microSeconds)));

        // Spark's Columns seem to be equal only with same reference. Using String representation instead.
        Assertions.assertEquals(expectedColumn.toString(), spanParameter.asColumn().toString());
    }
}
