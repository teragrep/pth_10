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
package com.teragrep.pth10.ast.commands.aggregate;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.bo.ColumnNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.*;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.AggregatorMode.EarliestLatestAggregatorMode;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.FieldIndex.FieldIndexImpl;
import com.teragrep.pth10.ast.commands.aggregate.UDAFs.FieldIndex.FieldIndexStub;
import com.teragrep.pth10.ast.commands.aggregate.utils.PercentileApprox;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes any aggregate functions used for example by the stats command. The aggregation function is returned as a
 * column, which will be applied to the desired dataset using the <code>Dataset.agg()</code> method.
 */
public class AggregateFunction extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateFunction.class);
    private String aggregateField = null;
    private final DPLParserCatalystContext catCtx;

    /**
     * Constructor for the aggregate function, used to initialize the class inside aggregating commands like
     * statsTransformation.
     */
    public AggregateFunction(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public String getAggregateField() {
        return this.aggregateField;
    }

    /**
     * Visit aggregate function<br>
     * <code>func( arg )</code>
     * 
     * @param ctx AggregationFunctionContext
     * @return Node containing the column needed for processing the aggregation
     */
    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        return AggregateFunctionEmitCatalyst(ctx);
    }

    public Node AggregateFunctionEmitCatalyst(DPLParser.AggregateFunctionContext ctx) {
        return visit(ctx.getChild(0));
    }

    /**
     * -- Aggregate method: Count -- Uses the built-in Spark function count().
     */
    @Override
    public Node visitAggregateMethodCount(DPLParser.AggregateMethodCountContext ctx) {
        Node rv = aggregateMethodCountEmitCatalyst(ctx);

        // Default fieldname
        aggregateField = "count";
        return rv;
    }

    public Node aggregateMethodCountEmitCatalyst(DPLParser.AggregateMethodCountContext ctx) {
        Node rv = null;
        ParseTree columnPt = ctx.getChild(1);
        Column col = null;
        String resultColumnName = "count";

        if (columnPt != null) {
            col = new CountAggregator(columnPt.getText(), catCtx.nullValue).toColumn();
            resultColumnName = String.format("count(%s)", columnPt.getText());
        }
        else {
            col = new CountAggregator(null, catCtx.nullValue).toColumn();
        }

        rv = new ColumnNode(col.as(resultColumnName));
        return rv;
    }

    /**
     * -- Aggregate method: Sum -- Uses the built-in Spark function sum().
     */
    @Override
    public Node visitAggregateMethodSum(DPLParser.AggregateMethodSumContext ctx) {
        Node rv = aggregateMethodSumEmitCatalyst(ctx);

        // Default fieldname
        aggregateField = "sum";
        return rv;
    }

    public Node aggregateMethodSumEmitCatalyst(DPLParser.AggregateMethodSumContext ctx) {
        Node rv;
        Column col;

        ParseTree columnPt = ctx.getChild(1);
        String resultColumnName = "sum";

        if (columnPt != null) {
            col = new SumAggregator(new FieldIndexImpl(columnPt.getText()), catCtx.nullValue).toColumn();
            resultColumnName = String.format("sum(%s)", columnPt.getText());
        }
        else {
            col = new SumAggregator(new FieldIndexStub(), catCtx.nullValue).toColumn();
        }

        rv = new ColumnNode(col.as(resultColumnName));
        return rv;
    }

    /**
     * -- Aggregate method: Median -- Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.ExactPercentileAggregator} with
     * <code>percentile = 0.5d</code> to calculate the median.
     */
    public Node visitAggregateMethodMedian(DPLParser.AggregateMethodMedianContext ctx) {
        Node rv = aggregateMethodMedianEmitCatalyst(ctx);

        // Default fieldname
        aggregateField = "median";
        return rv;
    }

    public Node aggregateMethodMedianEmitCatalyst(DPLParser.AggregateMethodMedianContext ctx) {
        Node rv = null;
        String arg = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("median(%s)", arg);

        // use ExactPercentileAggregator as median is just percentile x=50
        Column res = new ExactPercentileAggregator(arg, 0.5d).toColumn();

        rv = new ColumnNode(res.as(resultColumnName));
        return rv;
    }

    /**
     * -- Aggregate method: EstimatedDistinctCount_error -- Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.UDAF_DistinctCount} with built-in spark functions
     * approx_count_distinct(), abs() and divide().
     * <pre>estdc_error = abs(estimate_distinct_count - real_distinct_count)/real_distinct_count</pre>
     */
    public Node visitAggregateMethodEstimatedDistinctErrorCount(
            DPLParser.AggregateMethodEstimatedDistinctErrorCountContext ctx
    ) {
        Node rv = aggregateMethodEstimatedDistinctErrorCountEmitCatalyst(ctx);

        // Default fieldname
        aggregateField = "estdc";
        return rv;
    }

    public Node aggregateMethodEstimatedDistinctErrorCountEmitCatalyst(
            DPLParser.AggregateMethodEstimatedDistinctErrorCountContext ctx
    ) {
        Node rv = null;
        String arg = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("estdc_error(%s)", arg);

        // Formula for calculating estdc_error:
        // estdc_error (rv) = absolute_value(estimate_distinct_count - real_distinct_count)/real_distinct_count

        // Register UDAF
        SparkSession ss = SparkSession.builder().getOrCreate();
        ss.udf().register("UDAF_DistinctCount", new UDAF_DistinctCount());

        // Inside the parenthesis of absolute_value:
        Column estimate_distinct_count = functions.approx_count_distinct(arg);
        Column real_distinct_count = functions.callUDF("UDAF_DistinctCount", functions.col(arg));
        //Column real_distinct_count = new DistinctCountAggregator(arg).toColumn(); // use aggregator
        Column estimate_minus_real = estimate_distinct_count.minus(real_distinct_count);

        // Calculating the absolute value:
        Column abs_value = functions.abs(estimate_minus_real);
        // Final division with real_distinct_count:
        Column estdc_error = abs_value.divide(real_distinct_count);

        rv = new ColumnNode(estdc_error.as(resultColumnName));

        return rv;
    }

    /**
     * -- Aggregate method: Range -- Uses the {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.MinMaxAggregator}
     * in <code>RANGE</code> mode to perform the aggregation. <pre>range(x) = max(x) - min(x)</pre>
     */
    public Node visitAggregateMethodRange(DPLParser.AggregateMethodRangeContext ctx) {
        Node rv = aggregateMethodRangeEmitCatalyst(ctx);

        aggregateField = "range";
        return rv;
    }

    public Node aggregateMethodRangeEmitCatalyst(DPLParser.AggregateMethodRangeContext ctx) {
        Node rv = null;
        String arg = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("range(%s)", arg);

        // Range is max(arg) - min(arg)
        Column rangeCol = new MinMaxAggregator(arg, AggregatorMode.MinMaxAggregatorMode.RANGE).toColumn();

        rv = new ColumnNode(rangeCol.as(resultColumnName));

        return rv;
    }

    /**
     * -- Aggregate method: SumSquare -- Uses the built-in Spark functions pow() and sum() to calculate first the square
     * and then the sum to form the sum of squares. <pre>sumSq = x0^2 + x1^2 + ... + xn^2</pre>
     */
    public Node visitAggregateMethodSumSquare(DPLParser.AggregateMethodSumSquareContext ctx) {
        Node rv = aggregateMethodSumSquareEmitCatalyst(ctx);

        aggregateField = "sumsq";
        return rv;
    }

    public Node aggregateMethodSumSquareEmitCatalyst(DPLParser.AggregateMethodSumSquareContext ctx) {
        Node rv = null;
        String arg = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("sumsq(%s)", arg);

        // First make column x^2 and then sum together
        Column colOfSq = functions.pow(arg, 2);
        Column colOfSumSq = functions.sum(colOfSq);

        rv = new ColumnNode(colOfSumSq.as(resultColumnName));

        return rv;
    }

    /**
     * -- Aggregate method: DistinctCount -- Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.DistinctCountAggregator} to perform the aggregation.
     */
    public Node visitAggregateMethodDistinctCount(DPLParser.AggregateMethodDistinctCountContext ctx) {
        Node rv = aggregateMethodDistinctCountEmitCatalyst(ctx);

        aggregateField = "dc";
        return rv;
    }

    public Node aggregateMethodDistinctCountEmitCatalyst(DPLParser.AggregateMethodDistinctCountContext ctx) {
        Node rv = null;
        String colName = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("dc(%s)", colName);

        // Use aggregator
        Column col = new DistinctCountAggregator(colName, catCtx.nullValue).toColumn();

        rv = new ColumnNode(col.as(resultColumnName));

        return rv;
    }

    /**
     * -- Aggregate method: EstimatedDistinctCount -- Uses the built-in Spark function approx_count_distinct()
     */
    public Node visitAggregateMethodEstimatedDistinctCount(DPLParser.AggregateMethodEstimatedDistinctCountContext ctx) {
        Node rv = aggregateMethodEstimatedDistinctCountEmitCatalyst(ctx);

        aggregateField = "estdc";
        return rv;
    }

    public Node aggregateMethodEstimatedDistinctCountEmitCatalyst(
            DPLParser.AggregateMethodEstimatedDistinctCountContext ctx
    ) {
        Node rv = null;
        String arg = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("estdc(%s)", arg);

        Column col = functions.approx_count_distinct(arg);

        rv = new ColumnNode(col.as(resultColumnName));
        return rv;
    }

    /**
     * -- Aggregate method: Max -- Uses the {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.MinMaxAggregator} in
     * <code>MAX</code> mode to perform the aggregation.
     */
    public Node visitAggregateMethodMax(DPLParser.AggregateMethodMaxContext ctx) {
        Node rv = aggregateMethodMaxEmitCatalyst(ctx);

        aggregateField = "max";
        return rv;
    }

    public Node aggregateMethodMaxEmitCatalyst(DPLParser.AggregateMethodMaxContext ctx) {
        Node rv = null;

        String inputCol = ctx.getChild(1).getText();
        String resultColName = String.format("max(%s)", inputCol);

        Column col = new MinMaxAggregator(inputCol, AggregatorMode.MinMaxAggregatorMode.MAX).toColumn();

        rv = new ColumnNode(col.as(resultColName));

        return rv;
    }

    /**
     * -- Aggregate method: Min -- Uses the {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.MinMaxAggregator} in
     * <code>MIN</code> mode to perform the aggregation.
     */
    public Node visitAggregateMethodMin(DPLParser.AggregateMethodMinContext ctx) {
        Node rv = aggregateMethodMinEmitCatalyst(ctx);

        aggregateField = "min";
        return rv;
    }

    public Node aggregateMethodMinEmitCatalyst(DPLParser.AggregateMethodMinContext ctx) {
        Node rv = null;

        String inputCol = ctx.getChild(1).getText();
        String resultColName = String.format("min(%s)", inputCol);

        Column col = new MinMaxAggregator(inputCol, AggregatorMode.MinMaxAggregatorMode.MIN).toColumn();

        rv = new ColumnNode(col.as(resultColName));

        return rv;
    }

    /**
     * -- Aggregate method: Variance -- Uses the built-in Spark functions var_samp() and var_pop()
     */
    public Node visitAggregateMethodVariance(DPLParser.AggregateMethodVarianceContext ctx) {
        Node rv = aggregateMethodVarianceEmitCatalyst(ctx);

        aggregateField = "var";
        return rv;
    }

    public Node aggregateMethodVarianceEmitCatalyst(DPLParser.AggregateMethodVarianceContext ctx) {
        final Node rv;
        final TerminalNode cmd = (TerminalNode) ctx.getChild(0);
        final String colName = ctx.getChild(1).getText();
        final Column col;
        final String resultColumnName;

        // Switch between var_samp() / var_pop() based on given command
        // var() and varp()
        switch (cmd.getSymbol().getType()) {
            case DPLLexer.METHOD_AGGREGATE_VAR:
                col = functions.var_samp(colName);
                resultColumnName = String.format("var(%s)", colName);
                break;

            case DPLLexer.METHOD_AGGREGATE_VARP:
                col = functions.var_pop(colName);
                resultColumnName = String.format("varp(%s)", colName);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized method variance command");
        }

        rv = new ColumnNode(col.as(resultColumnName));

        return rv;
    }

    /**
     * -- Aggregate method: Standard Deviation -- Uses the built-in Spark function for stddev_samp() and stddev_pop()
     */
    public Node visitAggregateMethodStandardDeviation(DPLParser.AggregateMethodStandardDeviationContext ctx) {
        Node rv = aggregateMethodStandardDeviationEmitCatalyst(ctx);

        aggregateField = "stdev";
        return rv;
    }

    public Node aggregateMethodStandardDeviationEmitCatalyst(DPLParser.AggregateMethodStandardDeviationContext ctx) {
        final Node rv;
        final TerminalNode cmd = (TerminalNode) ctx.getChild(0);
        final String colName = ctx.getChild(1).getText();
        final Column col;
        final String resultColumnName;

        // Switch between stddev_samp() / stddev_pop() based on given command
        // stdev() and stdevp()
        switch (cmd.getSymbol().getType()) {
            case DPLLexer.METHOD_AGGREGATE_STDEV:
                col = functions.stddev_samp(colName);
                resultColumnName = String.format("stdev(%s)", colName);
                break;
            case DPLLexer.METHOD_AGGREGATE_STDEVP:
                col = functions.stddev_pop(colName);
                resultColumnName = String.format("stdevp(%s)", colName);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized method standard deviation command");
        }

        rv = new ColumnNode(col.as(resultColumnName));
        return rv;
    }

    /**
     * -- Aggregate method: Avg -- Uses the built-in Spark function for avg().
     */
    public Node visitAggregateMethodAvg(DPLParser.AggregateMethodAvgContext ctx) {
        Node rv = aggregateMethodAvgEmitCatalyst(ctx);

        aggregateField = "avg";
        return rv;
    }

    public Node aggregateMethodAvgEmitCatalyst(DPLParser.AggregateMethodAvgContext ctx) {
        Node rv = null;
        TerminalNode keyword = (TerminalNode) ctx.getChild(0);
        String resultColumnName = null;
        String colName = ctx.getChild(1).getText();

        // make sure that result uses the same command name
        // as given in the command
        switch (keyword.getSymbol().getType()) {
            case DPLLexer.METHOD_AGGREGATE_AVG:
                resultColumnName = String.format("avg(%s)", colName);
                break;
            case DPLLexer.METHOD_AGGREGATE_MEAN:
                resultColumnName = String.format("mean(%s)", colName);
                break;
        }

        Column col = functions.avg(colName);

        rv = new ColumnNode(col.as(resultColumnName));

        return rv;
    }

    /**
     * -- Aggregate method: Mode -- Returns the field value with most occurrences within a given column. Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.ModeAggregator} to perform the aggregation.
     */
    public Node visitAggregateMethodMode(DPLParser.AggregateMethodModeContext ctx) {
        Node rv = aggregateMethodModeEmitCatalyst(ctx);

        aggregateField = "mode";
        return rv;
    }

    public Node aggregateMethodModeEmitCatalyst(DPLParser.AggregateMethodModeContext ctx) {
        Node rv = null;
        String arg = ctx.getChild(1).getText(); // column name

        String resultColumnName = String.format("mode(%s)", arg);

        // Use ModeAggregator
        Column col = new ModeAggregator(arg).toColumn();
        rv = new ColumnNode(col.as(resultColumnName));
        return rv;
    }

    /**
     * -- Aggregate method: First --
     */
    public Node visitAggregateMethodFirst(DPLParser.AggregateMethodFirstContext ctx) {
        Node rv = aggregateMethodFirstEmitCatalyst(ctx);

        aggregateField = "first";
        return rv;
    }

    public Node aggregateMethodFirstEmitCatalyst(DPLParser.AggregateMethodFirstContext ctx) {
        String colName = ctx.getChild(1).getText();
        String resultColumnName = String.format("first(%s)", colName);
        return new ColumnNode(functions.first(colName).as(resultColumnName));
    }

    /**
     * -- Aggregate method: Last --
     */
    public Node visitAggregateMethodLast(DPLParser.AggregateMethodLastContext ctx) {
        Node rv = aggregateMethodLastEmitCatalyst(ctx);

        aggregateField = "last";
        return rv;
    }

    public Node aggregateMethodLastEmitCatalyst(DPLParser.AggregateMethodLastContext ctx) {
        String colName = ctx.getChild(1).getText();
        String resultColumnName = String.format("last(%s)", colName);
        return new ColumnNode(functions.last(colName).as(resultColumnName));
    }

    /**
     * -- Aggregate method: Earliest -- Returns the row with the earliest timestamp Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.EarliestLatestAggregator_String} in <code>EARLIEST</code>
     * mode to perform the aggregation.
     */
    public Node visitAggregateMethodEarliest(DPLParser.AggregateMethodEarliestContext ctx) {
        Node rv = aggregateMethodEarliestEmitCatalyst(ctx);

        aggregateField = "earliest";
        return rv;
    }

    public Node aggregateMethodEarliestEmitCatalyst(DPLParser.AggregateMethodEarliestContext ctx) {
        String colName = ctx.getChild(1).getText();

        // Ensure the column name matches the DPL command
        String resultColumnName = String.format("earliest(%s)", colName);

        // use aggregator
        Column col = new EarliestLatestAggregator_String(colName, EarliestLatestAggregatorMode.EARLIEST).toColumn();

        return new ColumnNode(col.as(resultColumnName));
    }

    /**
     * -- Aggregation method: Earliest_time -- Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.EarliestLatestAggregator_String} in
     * <code>EARLIEST_TIME</code> mode to perform the aggregation.
     */
    public Node visitAggregateMethodEarliestTime(DPLParser.AggregateMethodEarliestTimeContext ctx) {
        Node rv = aggregateMethodEarliestTimeEmitCatalyst(ctx);

        aggregateField = "earliest_time";
        return rv;
    }

    public Node aggregateMethodEarliestTimeEmitCatalyst(DPLParser.AggregateMethodEarliestTimeContext ctx) {
        //Column earliestTime = functions.min("_time");
        //Column asUnixTime = functions.unix_timestamp(earliestTime, "yyyy-MM-dd'T'HH:mm:ss.SSSX");

        String columnName = ctx.getChild(1).getText();
        String resultColumnName = String.format("earliest_time(%s)", columnName);

        Column asUnixTime = new EarliestLatestAggregator_String(
                columnName,
                AggregatorMode.EarliestLatestAggregatorMode.EARLIEST_TIME
        ).toColumn();

        return new ColumnNode(asUnixTime.as(resultColumnName));
    }

    /**
     * -- Aggregation method: Latest -- Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.EarliestLatestAggregator_String} in <code>LATEST</code>
     * mode to perform the aggregation.
     */
    public Node visitAggregateMethodLatest(DPLParser.AggregateMethodLatestContext ctx) {
        Node rv = aggregateMethodLatestEmitCatalyst(ctx);

        aggregateField = "latest";
        return rv;
    }

    public Node aggregateMethodLatestEmitCatalyst(DPLParser.AggregateMethodLatestContext ctx) {
        String colName = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("latest(%s)", colName);

        // use aggregator
        Column col = new EarliestLatestAggregator_String(colName, AggregatorMode.EarliestLatestAggregatorMode.LATEST)
                .toColumn();

        return new ColumnNode(col.as(resultColumnName));
    }

    /**
     * -- Aggregation method: Latest_time -- Uses the
     * {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.EarliestLatestAggregator_String} in
     * <code>LATEST_TIME</code> mode to perform the aggregation.
     */
    public Node visitAggregateMethodLatestTime(DPLParser.AggregateMethodLatestTimeContext ctx) {
        Node rv = aggregateMethodLatestTimeEmitCatalyst(ctx);

        aggregateField = "latest_time";
        return rv;
    }

    public Node aggregateMethodLatestTimeEmitCatalyst(DPLParser.AggregateMethodLatestTimeContext ctx) {
        //Column latestTime = functions.max("_time");
        //Column asUnixTime = functions.unix_timestamp(latestTime, "yyyy-MM-dd'T'HH:mm:ss.SSSX");

        String colName = ctx.getChild(1).getText();
        String resultColumnName = String.format("latest_time(%s)", colName);

        Column asUnixTime = new EarliestLatestAggregator_String(
                colName,
                AggregatorMode.EarliestLatestAggregatorMode.LATEST_TIME
        ).toColumn();
        return new ColumnNode(asUnixTime.as(resultColumnName));
    }

    /**
     * -- Aggregate method: List -- Uses the {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.ValuesAggregator} in
     * <code>LIST</code> mode to perform the aggregation.
     */
    public Node visitAggregateMethodList(DPLParser.AggregateMethodListContext ctx) {
        Node rv = aggregateMethodListEmitCatalyst(ctx);

        aggregateField = "list";
        return rv;
    }

    public Node aggregateMethodListEmitCatalyst(DPLParser.AggregateMethodListContext ctx) {
        // This also works, but retains [a,b,c,d] form instead of a <LINE_BREAK> b <LINE_BREAK> c <LINE_BREAK> d
        // |
        // v
        // return new ColumnNode(functions.collect_list(ctx.getChild(1).getText()));

        String colName = ctx.getChild(1).getText();

        // Make sure the result column name matches DPL command
        String resultColumnName = String.format("list(%s)", colName);

        // Use ValuesAggregator
        Column col = new ValuesAggregator(colName, AggregatorMode.ValuesAggregatorMode.LIST).toColumn();

        return new ColumnNode(col.as(resultColumnName));
    }

    /**
     * -- Aggregate method: Values -- Uses the {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.ValuesAggregator}
     * to perform the aggregation
     */
    public Node visitAggregateMethodValues(DPLParser.AggregateMethodValuesContext ctx) {
        Node rv = aggregateMethodValuesEmitCatalyst(ctx);

        aggregateField = "values";
        return rv;
    }

    public Node aggregateMethodValuesEmitCatalyst(DPLParser.AggregateMethodValuesContext ctx) {
        // Register and call UDAF
        // Take in _time column and given column with this schema
        String colName = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("values(%s)", colName);

        // Use ValuesAggregator to generate the column for given colName
        Column col = new ValuesAggregator(colName, AggregatorMode.ValuesAggregatorMode.VALUES).toColumn();
        return new ColumnNode(col.as(resultColumnName));

    }

    /**
     * -- Aggregate method: Percentile -- Can calculate the percentile multiple different ways, exactperc(), upperperc()
     * and perc(). Uses the {@link com.teragrep.pth10.ast.commands.aggregate.UDAFs.ExactPercentileAggregator} to perform
     * exactperc(), and {@link com.teragrep.pth10.ast.commands.aggregate.utils.PercentileApprox} for upperperc() and
     * perc().
     */
    public Node visitAggregateMethodPercentileVariable(DPLParser.AggregateMethodPercentileVariableContext ctx) {
        LOGGER.debug("Visiting percX(Y)");
        Node rv = aggregateMethodPercentileVariableEmitCatalyst(ctx);

        aggregateField = "percentile";
        return rv;
    }

    // TODO Implement upperperc
    public Node aggregateMethodPercentileVariableEmitCatalyst(DPLParser.AggregateMethodPercentileVariableContext ctx) {
        final TerminalNode func = (TerminalNode) ctx.getChild(0);
        final String commandName = func.getText();
        final String colName = ctx.getChild(2).getText();
        final Column col;

        // Make sure the result column name matches the DPL command
        final String resultColumnName = String.format("%s(%s)", commandName, colName);

        LOGGER.debug("Command: <[{}]>", commandName);

        // There are four different options for func: pX, percX, exactpercX, upperpercX
        // This separates the X from the command name into its own variable xThPercentileArg.
        final String funcAsString = func.getText();

        final double xThPercentileArg = (funcAsString.length() <= 4) ? Double
                .valueOf(funcAsString.substring(funcAsString.indexOf('p') + 1)) : /* pX */
                Double
                        .valueOf(funcAsString.substring(funcAsString.lastIndexOf('c') + 1)); /* percX, exactpercX, upperpercX */

        LOGGER.debug("perc: Use percentile = <[{}]>", xThPercentileArg);

        switch (func.getSymbol().getType()) {
            case DPLLexer.METHOD_AGGREGATE_P_VARIABLE:
            case DPLLexer.METHOD_AGGREGATE_PERC_VARIABLE:
                col = new PercentileApprox()
                        .percentile_approx(functions.col(colName), functions.lit(xThPercentileArg / 100));
                break;

            case DPLLexer.METHOD_AGGREGATE_EXACTPERC_VARIABLE:
                col = new ExactPercentileAggregator(colName, xThPercentileArg / 100).toColumn();
                break;

            case DPLLexer.METHOD_AGGREGATE_UPPERPERC_VARIABLE:
                // upperperc() returns the same as perc() if under 1000 values
                // TODO over 1000 distinct values
                col = new PercentileApprox()
                        .percentile_approx(functions.col(colName), functions.lit(xThPercentileArg / 100));
                //throw new UnsupportedOperationException("Upper percentile mode not supported yet");
                break;
            default:
                throw new IllegalArgumentException("Unrecognized method precentile variable command");
        }

        return new ColumnNode(col.as(resultColumnName));
    }

    /**
     * -- Aggregate method: rate -- Represents
     * <pre>(latest(X) - earliest(X)) / (latest_time(X) - earliest_time(X))</pre>
     */
    public Node visitAggregateMethodRate(DPLParser.AggregateMethodRateContext ctx) {
        Node rv = aggregateMethodRateEmitCatalyst(ctx);

        aggregateField = "rate";
        return rv;
    }

    public Node aggregateMethodRateEmitCatalyst(DPLParser.AggregateMethodRateContext ctx) {
        String colName = ctx.getChild(1).getText();

        // Make sure the result column name matches the DPL command
        String resultColumnName = String.format("rate(%s)", colName);

        Column res = new EarliestLatestAggregator_Double(colName, AggregatorMode.EarliestLatestAggregatorMode.RATE)
                .toColumn();
        return new ColumnNode(res.as(resultColumnName));
    }

}
