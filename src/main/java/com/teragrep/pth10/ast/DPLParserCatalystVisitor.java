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

package com.teragrep.pth10.ast;

import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.bo.Token.Type;
import com.teragrep.pth10.ast.commands.logicalstatement.LogicalStatement;
import com.teragrep.pth10.ast.commands.logicalstatement.TimeStatement;
import com.teragrep.pth10.ast.commands.transformstatement.TransformStatement;
import com.teragrep.pth10.datasources.GeneratedDatasource;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;

/*
 * {@literal
 * <!-- index = voyager _index_earliest="04/16/2020:10:25:40" | chart count(_raw) as count by _time | where  count > 70 -->
 * <root>
 * <logicalStatement>
 * <AND>
 * <index operation="EQUALS" value="voyager" />
 * <index_earliest operation="GE" value="1587021940" />
 * </AND>
 * </logicalStatement>
 * <transformStatements>
 * <divideBy field="_time">
 * <chart field="_raw" fieldRename="count" function="count" />
 * </divideBy>
 * <where>
 * <evalCompareStatement field="count" operation="GT" value="70" />
 * </where>
 * </transformStatements>
 * </root>
 * <p>
 * ---------------------------
 * <root>
 * <transformStatements>
 * <where>
 * <evalCompareStatement field="count" operation="GT" value="70" />
 * <transformStatement>
 * <divideBy field="_time">
 * <chart field="_raw" fieldRename="count" function="count">
 * <transformStatement>
 * <search root="true">
 * <logicalStatement>
 * <AND>
 * <index operation="EQUALS" value="voyager" />
 * <index_earliest operation="GE" value="1587021940" />
 * </AND>
 * </logicalStatement>
 * </search>
 * </transformStatement>
 * </chart>
 * </divideBy>
 * </transformStatement>
 * </where>
 * </transformStatements>
 * </root>
 * <p>
 * scala-sample
 * create dataframe (Result of search-transform when root=true)
 * val df = spark.readStream.load().option("query","<AND><index operation=\"EQUALS\" value=\"voyager\" /><index_earliest operation=\"GE\" value=\"1587021940\" /></AND>")
 * process that ( processing resulting dataframe)
 * val resultingDataSet =  df.agg(functions.count(col("`_raw`")).as("`count`")).groupBy(col("`_time`")).where(col("`_raw`").gt(70));
 * <p>
 * Same using single
 * spark.readStream.load().option("query","<AND><index operation=\"EQUALS\" value=\"voyager\" /><index_earliest operation=\"GE\" value=\"1587021940\" /></AND>").agg(functions.count(col("`_raw`")).as("`count`")).groupBy(col("`_time`")).where(col("`_raw`").gt(70));
 * when using treewalker, add "`"-around column names count -> `count`
 * }
 */

/**
 * Visitor used for Catalyst emit mode (main emit mode, XML emit mode only used for archive query)
 */
public class DPLParserCatalystVisitor extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLParserCatalystVisitor.class);

    //private static final Logger LOGGER = LoggerFactory.getLogger(DPLParserCatalystVisitor.class);
    private List<String> traceBuffer = new ArrayList<>(100);

    private List<String> dplHosts = new ArrayList<>();
    private List<String> dplSourceTypes = new ArrayList<>();
    private boolean aggregatesUsed = false;
    private String aggregateField = null;

    private long startTime = 0;
    private long endTime = 0;

    // Time calculation specifics
    private final DateTimeFormatter defaultTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss");
    private Stack<String> timeFormatStack = new Stack<>();

    Node logicalPart = null;
    Node transformPart = null;
    // list of transformation fragments which are added around logical block as
    // nested SELECTS
    private List<Node> transformPipeline = new ArrayList<>();
    // Symbol table for mapping DPL default column names
    private Map<String, Object> symbolTable = new HashMap<>();

    // For generated spark processing pipeline
    Dataset<Row> catalystDataFrame = null;

    // imported implementations
    LogicalStatement logicalStatement = null;
    ProcessingStack processingStack = new ProcessingStack(this);
    TransformStatement transformStatement = null;

    private DPLParserCatalystContext catCtx = null;
    
    // consumer for sequential stack's forEachBatch
    private Consumer<Dataset<Row>> batchConsumer = null;
    
    // dpl recall size
    private Integer dplRecallSize = 10000;
    
    // hdfs path - used for join command's subsearch save
    private String hdfsPath = null;

    // iplocation mmdb database path
    // primarily set through zeppelin config, this is only used
    // when such config is not found (~testing)
    private String iplocationMmdbPath = null;

    // Message handler for StreamingQueryListener
    private Consumer<Map<String, Map<String, String>>> messageHandler = null;

    // For debugging purposes
    public List<String> getTraceBuffer() {
        return traceBuffer;
    }

    public DPLParserCatalystVisitor(DPLParserCatalystContext ctx) {
        this.catCtx = ctx;
        // Tell logicalStatement to emit catalyst
        logicalStatement = new LogicalStatement(processingStack, catCtx, traceBuffer, symbolTable);
    }

    /**
     * Sets the consumer to handle the results of each of the batches
     * @param consumer Consumer with type Dataset to be implemented in pth_07
     */
    public void setConsumer(Consumer<Dataset<Row>> consumer) {
    	this.batchConsumer = consumer;
    }

    /**
     * Gets the consumer, used to call in BatchProcessor
     * @return consumer
     */
    public Consumer<Dataset<Row>> getConsumer() {
    	return this.batchConsumer;
    }

    public void setMessageHandler(Consumer<Map<String, Map<String,String>>> messageHandler) {
        this.messageHandler = messageHandler;
        // register messageHandler to DPLInternalStreamingQueryListener
        if (this.catCtx != null && this.messageHandler != null) {
            this.catCtx.getInternalStreamingQueryListener().registerHandler(this.messageHandler);
        }
        else {
            LOGGER.error("Unable to set message handler successfully.");
        }
    }

    public Consumer<Map<String, Map<String, String>>> getMessageHandler() {
        return messageHandler;
    }

    /**
     * Sets the maximum results for batchCollect
     * @param val int value
     */
    public void setDPLRecallSize(Integer val) {
    	this.dplRecallSize = val;
    }

    /**
     * Gets the dpl recall size (max results from batchCollect)
     * @return max results as int
     */
    public Integer getDPLRecallSize() {
    	return this.dplRecallSize;
    }

    /**
     * HDFS path used for join subsearch save
     * @param path path as string
     */
    public void setHdfsPath(String path) {
    	this.hdfsPath = path;
    }

    /**
     * HDFS path used for join/eventstats/subsearch save <br>
     * Generate a random path if none was set via setHdfsPath()
     * @return path as string
     */
    public String getHdfsPath() {
        if (this.hdfsPath == null && this.catCtx != null && this.catCtx.getSparkSession() != null && this.catCtx.getParagraphUrl() != null) {
            final String appId = this.catCtx.getSparkSession().sparkContext().applicationId();
            final String paragraphId = this.catCtx.getParagraphUrl();
            final String path = String.format("/tmp/%s/%s/%s/", appId, paragraphId, UUID.randomUUID());

            // /applicationId/paragraphId/randomUUID/
            LOGGER.info("No HDFS path specified in visitor, set path to: " + path);
            return path;
        }
        else if (this.hdfsPath == null) {
            LOGGER.info("Random UUID path generation");
            return "/tmp/" + UUID.randomUUID() + "/";
        }
        else {
            LOGGER.info("Get set hdfs path");
            return this.hdfsPath;
        }
    }

    /**
     * Sets the backup mmdb database path used by iplocation command
     * Only used, if the zeppelin config item is not found.
     * @param iplocationMmdbPath new mmdb file path as string
     */
    public void setIplocationMmdbPath(String iplocationMmdbPath) {
        this.iplocationMmdbPath = iplocationMmdbPath;
    }

    /**
     * Gets the backup mmdb database path used by iplocation command
     * Only used, if the zeppelin config item is not found.
     * @return mmdb file path as string
     */
    public String getIplocationMmdbPath() {
        return iplocationMmdbPath;
    }

    // Parameters for Groupmapper queries
    // List of found hosts
    public List<String> getHosts() {
        return this.dplHosts;
    }

    // List of found sourcetypes
    public List<String> getSourcetypes() {
        return this.dplSourceTypes;
    }

    public boolean getAggregatesUsed()
    {
        if(transformStatement != null){
            return transformStatement.getAggregatesUsed();
        }
        return false;
    }

    public void setAggregatesUsed(boolean newValue) {
        if (this.transformStatement != null) {
            this.transformStatement.setAggregatesUsed(newValue);
        }
    }

    public String getAggregateField()
    {
        if(transformStatement != null){
            return transformStatement.getAggregateField();
        }
        return null;
    }

    // return query start timestamp or 0 if not set
    public long getStartTime() {
        return startTime;
    }

    // return query end timestamp or 0 if not set
    public long getEndTime() {
        return endTime;
    }

    public String getLogicalPart() {
        String rv = null;
        if (logicalPart != null) {
            ColumnNode cn = (ColumnNode)logicalPart;
            traceBuffer.add("\ngetLogicalPart incoming=" + logicalPart.toString());
//            rv = logicalPart.toString();
            rv = cn.asExpression().sql();
        }
        return rv;
    }

    public Column getLogicalPartAsColumn() {
        Column rv = null;
        if (logicalPart != null) {
            traceBuffer.add("\ngetLogicalPart incoming=" + logicalPart.toString());
            rv = ((ColumnNode) logicalPart).getColumn();
        }
        return rv;
    }

    public String getTransformPart() {
        String rv = null;
        if (transformPart != null) {
            rv = transformPart.toString();
        }
        return rv;
    }

    public Node getTransformPart(int i) {
        Node rv = null;
        rv = transformPipeline.get(i);
        return rv;
    }

    public Dataset<Row> getCatalystPipe() {
        return catalystDataFrame;
    }

    /**
     * Get current DPLCatalystContext containing for instance audit information
     * @return DPLCatalystContext
     */
    public DPLParserCatalystContext getCatalystContext() {
        return catCtx;
    }

    public ProcessingStack.StackMode getStackMode(){
        return processingStack.getStackMode();
    }

    public ProcessingStack getStack(){
        return processingStack;
    }

    public void initProcessingStack(){
        if(processingStack.isEmpty()){
            if(catCtx.getDs() == null){
                throw new IllegalStateException("Datasource uninitialized");
            }
            processingStack.push("CalystVisitor given dataset used:", catCtx.getDs());
            catalystDataFrame = catCtx.getDs();
           LOGGER.info("CalystVisitor given dataset used:"+ catCtx.getDs());
        }
    }
    /**
     * Visit the root rule - translate the DPL query to Spark dataframe actions and transformations.
     */
    @Override
    public Node visitRoot(DPLParser.RootContext ctx) {
        CatalystNode rv = null;
        traceBuffer.add("CatalystVisitor Root incoming:" + ctx.getText());
        LOGGER.info("CatalystVisitor Root incoming:" + ctx.getText());
        transformPipeline.clear();
        if(logicalStatement == null) // init if missing
           logicalStatement = new LogicalStatement(processingStack, catCtx, traceBuffer);

        // Current version has always 2 nodes at the root level.
        if (ctx.getChildCount() < 1) {
            throw new NumberFormatException("Missing logical and/or transform part." + ctx.getText());
        }

        // Logical part
        if (ctx.searchTransformationRoot() != null) {
            traceBuffer.add("Handle logical part:" + ctx.getChild(0).getText());
            LOGGER.info("visitRoot Handle logical part:" + ctx.getChild(0).getText());
            logicalPart = visitSearchTransformationRoot(ctx.searchTransformationRoot());
            traceBuffer.add("after logical part stack=" + processingStack.isEmpty() + " stack:" + processingStack+ " rv:"+rv);
        }
        // Just transform part
        if (ctx.transformStatement() != null) {
            if(processingStack.isEmpty()){
                if(catCtx.getDs() == null){
                	// Use fake stream if no other data present
                	GeneratedDatasource gds = new GeneratedDatasource(catCtx);
                	Dataset<Row> constructed = null;

                	try {
						constructed = gds.constructStream("fake data", null);
					} catch (UnknownHostException | StreamingQueryException | InterruptedException e) {
						throw new RuntimeException("Error constructing fake stream @ DPLParserCatalystVisitor:visitRoot\nError: " + e.getMessage());
					}

                	catCtx.setDs(constructed);
                	//throw new IllegalStateException("Datasource uninitialized");
                }
                processingStack.push("CatalystVisitor given dataset used:", catCtx.getDs());
            }
            transformPart = visitTransformStatement(ctx.transformStatement());
        } else {
            throw new NumberFormatException("Missing mandatory transform part." + ctx.getText());
        }
        if (transformPart != null)
            traceBuffer.add("\n---final transform part:" + transformPart.toString());
        else {
            traceBuffer.add("---No additional transform part just logical part used:" + processingStack.isEmpty() + " stack:" + processingStack+"\n");
        }
        traceBuffer.add("\nafter logical part 2 stack=" + processingStack.isEmpty() + " stack:" + processingStack+" rv:"+rv);

        if (logicalPart != null) {
            // add logical part as it is a  special kind of transformation
            transformPipeline.add(logicalPart);
            Column c = ((ColumnNode) logicalPart).getColumn();

            if (transformPart != null) {
                traceBuffer.add("\n transformation Pipeline size=" + transformPipeline.size());
                transformPipeline.forEach(nd -> {

                    if (nd instanceof ColumnNode) {
                        Column col = ((ColumnNode) nd).getColumn();
                        traceBuffer.add("\nTransform node value:" + col.expr().sql());
                    } else {
                        traceBuffer.add("\nTransform node:" + nd.getClass().getName());
                    }
                });
            }
        } else {
            // Only transformation parts
            rv = new CatalystNode(processingStack.pop("visitRoot no logicalParts"));
            traceBuffer.add("\nOnly transformation parts processingPipe.push(" + rv.getDataset() + ")");
            // put it back
            processingStack.push(rv.getDataset());
        }
        LOGGER.info("::: visitRoot ::: 1 ProcessingStack size=" + processingStack.size());
        traceBuffer.add("\nafter logical part 3 stackIsEmpty=" + processingStack.isEmpty() + " stack:" + processingStack+" rv:"+rv);

        if(rv == null){ // logical part exists
            LOGGER.info("Logical part exists");
            //catalystDataFrame = processingStack.pop("visitRoot, get end result dataset");
        	
        	// Old code is above, commented out. The code below is used to preserve last item in stack, if the 
        	// stack is in sequential mode
        	// Used to pop the DataStreamWriter from stack in StackTest, so it can be started there
        	SequentialOperationItem seqOpsItem = processingStack.getSequentialOpsStack().peek();
        	Dataset<Row> seqOpsDf = null;
        	DataStreamWriter<Row> seqOpsDsw = null;
        	
        	if (seqOpsItem != null) { 
        		seqOpsDf = seqOpsItem.getDataframe();
        		seqOpsDsw = seqOpsItem.getStreamWriter();
        	}
            
        	if (seqOpsDf != null)  {
            	catalystDataFrame = seqOpsDf;
            }
            else {
                LOGGER.info("In parallel mode, make DataStreamWriter");
            	catalystDataFrame = processingStack.pop("visitRoot, get end result dataset");

            	// Parallel mode, but make DataStreamWriter
            	if (catalystDataFrame != null && catalystDataFrame.isStreaming()) {
            		BatchProcessor bp = new BatchProcessor(this.processingStack.isSorted() ? null : "_time",this, this.aggregatesUsed, this.processingStack.getParseTree());
                	bp.setBatchHandler(this.batchConsumer);
                	if (this.aggregatesUsed) {
                		seqOpsDsw = catalystDataFrame.writeStream().outputMode("complete").foreachBatch(bp);
                	}
                	else {
                		seqOpsDsw = catalystDataFrame.writeStream().outputMode("append").foreachBatch(bp);
                	}
            	}
            }
        	
            rv = new CatalystNode(catalystDataFrame);
            rv.setDataStreamWriter(seqOpsDsw);
        }
        else { // no logical part, e.g. makeresults
            LOGGER.info("No logical part found (e.g. makeresults command used instead of indices)");
            if (rv.getDataset() != null && rv.getDataset().isStreaming()) {
                DataStreamWriter<Row> dsw = null;
                BatchProcessor bp = new BatchProcessor(this.processingStack.isSorted() ? null : "_time", this, this.aggregatesUsed, this.processingStack.getParseTree());
                bp.setBatchHandler(this.batchConsumer);

                if (this.aggregatesUsed) {
                    dsw = rv.getDataset().writeStream().outputMode("complete").foreachBatch(bp);
                }
                else {
                    dsw = rv.getDataset().writeStream().outputMode("append").foreachBatch(bp);
                }
                rv.setDataStreamWriter(dsw);
            }
        }

        if (rv.getDataStreamWriter() == null) {
            LOGGER.error("The parse tree walk did not provide a data stream writer! Most likely " +
                    "this means that the command provided a non-streaming dataset. Please report this error. Query: " + ctx.getText());
        }
        
        LOGGER.info("::: visitRoot ::: 2 ProcessingStack size=" + processingStack.size());
        return rv;
    }

    @Override
    public Node visitSearchTransformationRoot(DPLParser.SearchTransformationRootContext ctx) {
        traceBuffer.add("visitSearchTransformationRoot:" + ctx.getText() + "\n");
        LOGGER.info("CatalystVisitor visitSearchTransformationRoot:" + ctx.getText());
        return logicalStatement.visitSearchTransformationRoot(ctx);
    }

    /**
     * logicalStatement : macroStatement | subsearchStatement | sublogicalStatement
     * | timeStatement | searchQualifier | Not logicalStatement | indexStatement |
     * comparisonStatement | logicalStatement Or logicalStatement | logicalStatement
     * And? logicalStatement ;
     */
    @Override
    public Node visitLogicalStatement(DPLParser.LogicalStatementContext ctx) {
        Node rv = logicalStatement.visitLogicalStatement(ctx);
        return rv;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     */
    @Override
    public Node visitComparisonStatement(DPLParser.ComparisonStatementContext ctx) {
        String value = ctx.getChild(0).getText() + ctx.getChild(1).getText() + ctx.getChild(2).getText();
        traceBuffer.add("--VisitComparisonStatement: childCount" + ctx.getChildCount() + " Statement:" + value+"\n");
        return new StringNode(new Token(Type.STRING, value));
    }
    // Time format handling

    /**
     * timeStatement : timeFormatQualifier? timeQualifier ;
     */
    @Override
    public Node visitTimeStatement(DPLParser.TimeStatementContext ctx) {
        traceBuffer.add("DPLParserCatalystVisitor.visitTimeStatement() \n");
        TimeStatement timeStatement = new TimeStatement(timeFormatStack);
        Node rv = timeStatement.visitTimeStatement(ctx);
        return rv;
    }

    /**
     * Syntactic transform statement <blockquote>
     *
     * <pre>
     * transformStatement
     * : PIPE transform_XXX_Operation
     * ;
     * </pre>
     *
     * </blockquote>
     */
    @Override
    public Node visitTransformStatement(DPLParser.TransformStatementContext ctx) {
        transformStatement = new TransformStatement(catCtx, processingStack, traceBuffer, symbolTable);
        transformStatement.setAggregatesUsed(this.aggregatesUsed);
        traceBuffer.add(ctx.getChildCount() + " VisitTransformStatement:" + ctx.getText()+"\n");
        Node rv = transformStatement.visit(ctx);
        this.transformPipeline = transformStatement.getTransformPipeline();
        this.aggregatesUsed = transformStatement.getAggregatesUsed(); // update aggregatesUsed value in visitor
        return rv;
    }

    @Override
    public Node visitL_evalStatement_evalCompareStatement(DPLParser.L_evalStatement_evalCompareStatementContext ctx) {
        return logicalStatement.visitL_evalStatement_evalCompareStatement(ctx);
    }

    @Override
    public Node visitAggregateFunction(DPLParser.AggregateFunctionContext ctx) {
        ElementNode rv = (ElementNode) visitChildren(ctx);
       traceBuffer.add(" adding visitAggregateFunction:" + ctx.getText() + " rv=" + rv.toString()+"\n");
       this.aggregatesUsed = true;
        return rv;
    }
}
