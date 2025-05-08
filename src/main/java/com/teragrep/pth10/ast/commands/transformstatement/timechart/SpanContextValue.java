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
package com.teragrep.pth10.ast.commands.transformstatement.timechart;

import com.teragrep.pth10.ast.ContextValue;
import com.teragrep.pth10.ast.DPLParserCatalystContext;

import com.teragrep.pth10.ast.commands.transformstatement.timechart.span.Span;
import com.teragrep.pth10.ast.commands.transformstatement.timechart.span.SpanParameter;
import com.teragrep.pth10.ast.commands.transformstatement.timechart.span.TimeRange;
import com.teragrep.pth_03.antlr.DPLParser;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Parses a Column from the "| timechart" command's span parameter
 */
public final class SpanContextValue implements ContextValue<Column> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpanContextValue.class);

    private final List<DPLParser.T_timechart_binOptParameterContext> ctxList;
    private final DPLParserCatalystContext catCtx;
    private final TimeRange defaultTimeRange;

    public SpanContextValue(
            List<DPLParser.T_timechart_binOptParameterContext> ctxList,
            DPLParserCatalystContext catCtx
    ) {
        this(ctxList, catCtx, catCtx.getParserConfig().getTimeRange());
    }

    public SpanContextValue(
            List<DPLParser.T_timechart_binOptParameterContext> ctxList,
            DPLParserCatalystContext catCtx,
            TimeRange defaultTimeRange
    ) {
        this.ctxList = ctxList;
        this.catCtx = catCtx;
        this.defaultTimeRange = defaultTimeRange;
    }

    @Override
    public Column value() {
        // use default time range from config if span= parameter is not used
        Span span = new SpanParameter(defaultTimeRange);
        TimeRange actualTimeRange = defaultTimeRange;

        boolean spanFound = false;
        for (DPLParser.T_timechart_binOptParameterContext binOpt : ctxList) {
            DPLParser.T_timechart_binSpanParameterContext spanCtx = binOpt
                    .t_timechart_spanOptParameter()
                    .t_timechart_binSpanParameter();
            if (spanCtx != null) {
                if (spanFound) {
                    throw new IllegalArgumentException("| timechart does not allow multiple 'span=' parameters");
                }
                LOGGER.info("visiting t_timechart_binSpanParameter:<{}>", spanCtx.getText());
                actualTimeRange = new TimeRange(spanCtx.getChild(1).getText());
                span = new SpanParameter(actualTimeRange);
                spanFound = true;
            }
        }

        this.catCtx.setTimeChartSpanSeconds(actualTimeRange.asSeconds());
        return span.asColumn();
    }
}
