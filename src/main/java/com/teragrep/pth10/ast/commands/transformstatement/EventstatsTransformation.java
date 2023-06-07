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

package com.teragrep.pth10.ast.commands.transformstatement;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLInternalStreamingQuery;
import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.commands.aggregate.AggregateFunction;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class EventstatsTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventstatsTransformation.class);
    private ProcessingStack stack = null;
    private DPLParserCatalystContext catCtx = null;

    private Dataset<Row> ds = null;
    private List<Column> listOfAggregations = new ArrayList<>();

    private boolean aggregatesUsed = false;

    public void setAggregatesUsed(boolean newValue) {
        this.aggregatesUsed = newValue;
    }

    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    public EventstatsTransformation(ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.stack = stack;
        this.catCtx = catCtx;
    }

    @Override
    public Node visitEventstatsTransformation(DPLParser.EventstatsTransformationContext ctx) {
        LOGGER.info("Visiting eventstats transformation: " + ctx.getText());
        // eventstats [allnum=bool] stats-agg-term [by-clause]

        // Pop dataset from stack
        ds = stack.pop();
        assert ds != null : "Popped item was null";

        LOGGER.info("Popped item from stack is " + ((ds.isStreaming()) ? "a streaming dataset" : "NOT a streaming dataset"));

        // BY instruction
        String byInst = null;

        // FIXME implement allnum=bool parameter
        if (ctx.t_eventstats_allnumParameter() != null) {
            LOGGER.warn("Detected allnum parameter; however, it is not yet implemented and will be skipped.");
            Node allNumParamNode = visit(ctx.t_eventstats_allnumParameter());
        }

        // go through stats-agg-term, by clause and as clause
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);

            if (child instanceof DPLParser.T_eventstats_aggregationInstructionContext) {
                LOGGER.info("Detected aggregation instruction");
                visit(child);
            }
            else if (child instanceof DPLParser.T_eventstats_byInstructionContext) {
                LOGGER.info("Detected BY instruction");
                byInst = ((StringNode) visit(child)).toString();
            }
            else if (child instanceof DPLParser.T_eventstats_fieldRenameInstructionContext) {
                LOGGER.info("Detected field rename (AS) instruction");
                visit(child);
            }
        }

        // set aggregatesUsed=true
        LOGGER.info("AggregatesUsed: " + this.getAggregatesUsed() + " set to true");
        this.setAggregatesUsed(true);

        // perform aggregation
        Dataset<Row> aggDs = null;
        Column mainAgg = listOfAggregations.remove(0);
        Seq<Column> seqOfAggs = JavaConversions.asScalaBuffer(listOfAggregations);

        if (byInst != null) {
            LOGGER.info("Performing BY-grouped aggregation");
            aggDs = ds.groupBy(byInst).agg(mainAgg, seqOfAggs);
        }
        else {
            LOGGER.info("Performing direct aggregation");
            aggDs = ds.agg(mainAgg, seqOfAggs);
        }

        assert aggDs != null : "Aggregated dataset was null";

        // Get schemas
        StructType schema = ds.schema();
        StructType aggSchema = aggDs.schema();

        // consts for saving to hdfs
        final String rndId = UUID.randomUUID().toString();
        final String pathForSave = this.stack.getCatVisitor().getHdfsPath();
        final String queryName = "eventstats_query_" + rndId;
        final String checkpointPath = pathForSave + "checkpoint/" + rndId;
        final String path = pathForSave + "data/" + rndId + ".avro";

        LOGGER.info(String.format("Initializing a stream query for eventstats: name: '%s', Path(avro): '%s', Checkpoint path: '%s'", queryName, path, checkpointPath));

        // save ds to HDFS, and perform join on that
        DataStreamWriter<Row> writer = ds
                .writeStream()
                .format("avro")
                .trigger(Trigger.ProcessingTime(0))
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .option("checkpointLocation", checkpointPath)
                .option("path", path)
                .outputMode("append");

        // start query and wait for finish
        SparkSession ss = SparkSession.builder().getOrCreate();

        StreamingQuery query = this.catCtx.getInternalStreamingQueryListener().registerQuery(queryName, writer);

        // Await for StreamingQueryListener to call stop()
        try {
            LOGGER.info("Awaiting for the filesink/hdfs save query to end...");
            query.awaitTermination();
        } catch (StreamingQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // Get saved ds (-> non-streaming) from storage, and join on aggregated (-> streaming) dataset

        Dataset<Row> savedDs = ss.sqlContext().read().format("avro").schema(schema).load(path);
        Dataset<Row> resultDs = null;

        assert savedDs != null : "Dataset read from file sink was null";
        LOGGER.info("Read " + savedDs.count() + " row(s) from the file sink.");

        if (byInst != null) {
            resultDs = savedDs.join(aggDs, byInst);
        }
        else {
            resultDs = savedDs.crossJoin(aggDs);
        }

        assert resultDs != null : "Joined dataset was null";

        // Used to rearrange the columns, join mangles them up a bit
        String[] aggSchemaFields = aggSchema.fieldNames();
        List<String> schemaFields = new ArrayList<>(Arrays.asList(schema.fieldNames()));

        for (String aggColName : aggSchemaFields) {
           if (!schemaFields.contains(aggColName)) {
               schemaFields.add(aggColName);
           }
        }

        Seq<Column> rearranged = JavaConversions.asScalaBuffer(schemaFields.stream().map(functions::col).collect(Collectors.toList()));
        resultDs = resultDs.select(rearranged); // rearrange to look more like original dataset
        stack.push(resultDs);

        LOGGER.info("Eventstats transformation done, proceed to the next step");
        return new CatalystNode(resultDs);
    }

    @Override
    public Node visitT_eventstats_aggregationInstruction(DPLParser.T_eventstats_aggregationInstructionContext ctx) {
        ParseTree cmd = ctx.getChild(0);
        AggregateFunction aggFunction = new AggregateFunction(ds, new ArrayList<>());
        Column aggCol = null;

        if (cmd instanceof TerminalNode) {
           /* if (((TerminalNode)cmd).getSymbol().getType() == DPLLexer.COMMAND_EVENTSTATS_MODE_COUNT) {
                LOGGER.info("Implied wildcard COUNT mode - count(" + ds.columns()[0] + ")");
                aggCol = functions.count(ds.columns()[0]).as("count");
            }*/
        }
        else if (cmd instanceof DPLParser.AggregateFunctionContext) {
            // visit agg function
            LOGGER.info("Aggregate function: " + cmd.getText());
            Node aggNode = aggFunction.visit((DPLParser.AggregateFunctionContext) cmd);
            aggCol = ((ColumnNode) aggNode).getColumn();
        }

        ParseTree fieldRenameInst = ctx.getChild(1);
        if (fieldRenameInst instanceof DPLParser.T_eventstats_fieldRenameInstructionContext) {
            LOGGER.info("Field rename instruction: " + fieldRenameInst.getText());
            // AS new-fieldname
            aggCol = aggCol.as(fieldRenameInst.getChild(1).getText());
        }

        listOfAggregations.add(aggCol);
        return null;
    }

    @Override
    public Node visitT_eventstats_byInstruction(DPLParser.T_eventstats_byInstructionContext ctx) {
        String byInst = ctx.getChild(1).getText();

        LOGGER.info("byInst = " + byInst);

        return new StringNode(new Token(Token.Type.STRING, byInst));
    }

    /**
     * Doesn't seem to get used, goes through aggregationInstruction->fieldRenameInstruction
     * @param ctx
     * @return
     */
    @Override
    public Node visitT_eventstats_fieldRenameInstruction(DPLParser.T_eventstats_fieldRenameInstructionContext ctx) {
        String renameInst = ctx.getChild(1).getText();

        LOGGER.info("renameInst = " + renameInst);

        return new StringNode(new Token(Token.Type.STRING, renameInst));
    }

}
