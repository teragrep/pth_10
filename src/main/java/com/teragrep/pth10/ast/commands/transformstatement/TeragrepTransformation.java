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

import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepListNode;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.ast.bo.StringNode;
import com.teragrep.pth10.ast.bo.Token;
import com.teragrep.pth10.ast.commands.logicalstatement.LogicalStatementCatalyst;
import com.teragrep.pth10.ast.commands.logicalstatement.LogicalStatementXML;
import com.teragrep.pth10.steps.AbstractStep;
import com.teragrep.pth10.steps.teragrep.*;
import com.teragrep.pth10.steps.tokenizer.AbstractTokenizerStep;
import com.teragrep.pth10.steps.tokenizer.TokenizerStep;
import com.typesafe.config.Config;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.Arrays;

/**
 * Class containing the visitor methods for all "| teragrep" subcommands
 */
public class TeragrepTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepTransformation.class);
    DPLParserCatalystContext catCtx;
    DPLParserCatalystVisitor catVisitor;

    // host, port for relp connection
    private String host = "127.0.0.1";
    private int port = 601;

    // can user set host and port in command?
    private boolean enforceDestination = false;

    private Config zplnConfig;
    // zeppelin config key names
    // syslog
    private static final String hostCfgItem = "dpl.pth_10.transform.teragrep.syslog.parameter.host";
    private static final String portCfgItem = "dpl.pth_10.transform.teragrep.syslog.parameter.port";
    private static final String enforceDestinationCfgItem = "dpl.pth_10.transform.teragrep.syslog.restrictedMode";

    public TeragrepTransformation(DPLParserCatalystContext catCtx, DPLParserCatalystVisitor catVisitor)
    {
        this.catCtx = catCtx;
        this.catVisitor = catVisitor;
    }

    /**
     * Topmost visitor, teragrep subcommand visiting starts from this function
     * COMMAND_MODE_TERAGREP (t_modeParameter | t_getParameter) t_hostParameter?
     */
    @Override
    public Node visitTeragrepTransformation(DPLParser.TeragrepTransformationContext ctx) {
        return teragrepTransformationEmitCatalyst(ctx);
    }

    /**
     * Visits the subrules and sets the parameters based on the parse tree.
     * Also uses the zeppelin config to set defaults, if available
     * @param ctx Main parse tree
     * @return CatalystNode
     */
    private Node teragrepTransformationEmitCatalyst(DPLParser.TeragrepTransformationContext ctx) {

        LOGGER.info("TeragrepTransformation Emit Catalyst");

        // get zeppelin config
        zplnConfig = catCtx.getConfig();
        if (zplnConfig != null) {
            // host and port for syslog stream
            if (zplnConfig.hasPath(hostCfgItem) && !zplnConfig.getString(hostCfgItem).equals("")) {
                LOGGER.info("Host set via zeppelin config item");
                this.host = zplnConfig.getString(hostCfgItem);
            }

            if (zplnConfig.hasPath(portCfgItem)) {
                LOGGER.info("Port set via zeppelin config item");
                this.port = zplnConfig.getInt(portCfgItem);
            }

            // enforce destination for syslog stream
            if (zplnConfig.hasPath(enforceDestinationCfgItem)) {
                LOGGER.info("Enforce destination set via zeppelin config item");
                this.enforceDestination = zplnConfig.getBoolean(enforceDestinationCfgItem);
            }
        }
        else {
            LOGGER.error("Zeppelin config was not provided to the Teragrep command: host and port will be set as default, {}",
                    "and no destination will be enforced.");
        }

        return visit(ctx.getChild(1));
    }

    /**
     * Sets the <code>cmdMode</code> based on the parse tree given<br>
     * @param ctx getParameter sub parse tree
     * @return null, as the function sets a global variable <code>cmdMode</code>
     */
    @Override
	public Node visitT_getParameter(DPLParser.T_getParameterContext ctx) {
        // get archive summary OR get system version
        if (ctx.t_getTeragrepVersionParameter() != null) {
            return visit(ctx.t_getTeragrepVersionParameter());
        } else if (ctx.t_getArchiveSummaryParameter() != null) {
            return visit(ctx.t_getArchiveSummaryParameter());
        } else {
            throw new IllegalArgumentException("Unsupported teragrep command: " + ctx.getText());
        }
    }

    @Override
    public Node visitT_getArchiveSummaryParameter(DPLParser.T_getArchiveSummaryParameterContext ctx) {
        // archive summary
        Document doc;
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        }
        catch (ParserConfigurationException pce) {
            throw new RuntimeException("Error occurred during initialization of XML document in metadata query: <{" +
                    pce.getMessage() + "}>");
        }
        // get metadata via logicalStatement and isMetadataQuery=true
        LogicalStatementXML logiXml = new LogicalStatementXML(catCtx, doc, true);
        AbstractStep xmlStep = logiXml.visitLogicalStatementXML(ctx.searchTransformationRoot());
        LogicalStatementCatalyst logiCat = new LogicalStatementCatalyst(catVisitor, catCtx);
        AbstractStep catStep = logiCat.visitLogicalStatementCatalyst(ctx.searchTransformationRoot());
        return new StepListNode(Arrays.asList(xmlStep, catStep));
    }

    @Override
    public Node visitT_getTeragrepVersionParameter(DPLParser.T_getTeragrepVersionParameterContext ctx) {
        // teragrep system version
        return new StepNode(new TeragrepSystemStep(this.catCtx));
    }

    /**
     * Sets the host and port, if given
     * @param ctx hostParameter sub parse tree
     * @return null, as the function sets the global variables <code>host</code> and <code>port</code>
     */
    @Override
    public Node visitT_hostParameter(DPLParser.T_hostParameterContext ctx) {
        if (ctx.getChild(1) != null) {
            this.host = ctx.getChild(1).getText();
        }

        if (ctx.getChild(3) != null) {
            this.port = Integer.parseUnsignedInt(ctx.getChild(3).getText());
        }

        LOGGER.info("Teragrep command: Host and port set to <[{}]>:<[{}]>", host, port);
        return null;
    }

    @Override
    public Node visitT_execParameter(DPLParser.T_execParameterContext ctx) {
        /*
        t_execParameter
            : COMMAND_TERAGREP_MODE_EXEC (t_syslogModeParameter
            | t_listModeParameter
            | t_saveModeParameter
            | t_parserExplainParameter
            | t_deleteModeParameter
            | t_loadModeParameter
            | t_kafkaSaveModeParameter
            | t_bloomModeParameter)
            ;
         */

        return visitChildren(ctx);
    }

    // exec syslog stream host 127.0.0.123 port 123
    @Override
    public Node visitT_syslogModeParameter(DPLParser.T_syslogModeParameterContext ctx) {
        DPLParser.T_hostParameterContext hostParamCtx = ctx.t_hostParameter();
        if (hostParamCtx != null && !enforceDestination) {
            DPLParser.T_portParameterContext portParamCtx = hostParamCtx.t_portParameter();
            if (portParamCtx != null) {
                // actually ip, even though named portParameterContext ...
                host = portParamCtx.getText();
            }

            if (hostParamCtx.COMMAND_TERAGREP_MODE_PORT_NUM() != null) {
                port = Integer.parseInt(hostParamCtx.COMMAND_TERAGREP_MODE_PORT_NUM().getText());
            }
        }

        return new StepNode(new TeragrepSyslogStep(host, port));
    }

    // exec hdfs save path retention
    @Override
    public Node visitT_saveModeParameter(DPLParser.T_saveModeParameterContext ctx) {
        String hdfsPath = null;
        String hdfsRetentionSpan = null;
        boolean hdfsOverwrite = false;
        boolean header = true;

        if (ctx.t_pathParameter() != null && !ctx.t_pathParameter().isEmpty()) {
            if (ctx.t_pathParameter().size() != 1) {
                throw new IllegalArgumentException("Path parameter was provided multiple times! Please provide it only once.");
            }
            hdfsPath = visit(ctx.t_pathParameter(0)).toString();
        }
        if (ctx.t_retentionParameter() != null && !ctx.t_retentionParameter().isEmpty()) {
            if (ctx.t_retentionParameter().size() != 1) {
                throw new IllegalArgumentException("Retention parameter was provided multiple times! Please provide it only once.");
            }
            hdfsRetentionSpan = ctx.t_retentionParameter(0).spanType().getText();
        }

        if (ctx.t_overwriteParameter() != null && !ctx.t_overwriteParameter().isEmpty()) {
            if (ctx.t_overwriteParameter().size() != 1) {
                throw new IllegalArgumentException("Overwrite parameter was provided multiple times! Please provide it only once.");
            }
            TerminalNode overwriteBoolNode = (TerminalNode) ctx.t_overwriteParameter(0).booleanType().getChild(0);
            switch (overwriteBoolNode.getSymbol().getType()) {
                case DPLLexer.GET_BOOLEAN_TRUE:
                    hdfsOverwrite = true;
                    break;
                case DPLLexer.GET_BOOLEAN_FALSE:
                    hdfsOverwrite = false;
                    break;
                default:
                    throw new RuntimeException("Expected a boolean value for parameter 'overwrite', instead it was something else.\n" +
                            "Try replacing the text after 'overwrite=' with 'true' or 'false'.");
            }
        }

        TeragrepHdfsSaveStep.Format format = TeragrepHdfsSaveStep.Format.AVRO;
        if (ctx.t_hdfsFormatParameter() != null && !ctx.t_hdfsFormatParameter().isEmpty()) {
            if (ctx.t_hdfsFormatParameter().size() != 1) {
                throw new IllegalArgumentException("'format=' parameter was provided multiple times! Please provide it only once.");
            }

            TerminalNode formatNode = (TerminalNode) ctx.t_hdfsFormatParameter(0).getChild(1);
            switch (formatNode.getSymbol().getType()) {
                case DPLLexer.COMMAND_TERAGREP_MODE_CSV_FORMAT:
                    format = TeragrepHdfsSaveStep.Format.CSV;
                    break;
                case DPLLexer.COMMAND_TERAGREP_MODE_JSON_FORMAT:
                    format = TeragrepHdfsSaveStep.Format.JSON;
                    break;
            }
        }

        if (ctx.t_headerParameter() != null && !ctx.t_headerParameter().isEmpty()) {
            if (ctx.t_headerParameter().size() != 1) {
                throw new IllegalArgumentException("'header=' parameter was provided multiple times! Please provide it only once.");
            }
            TerminalNode headerNode = (TerminalNode) ctx.t_headerParameter(0).booleanType().getChild(0);
            switch (headerNode.getSymbol().getType()) {
                case DPLLexer.GET_BOOLEAN_TRUE:
                    // already defaults to true
                    break;
                case DPLLexer.GET_BOOLEAN_FALSE:
                    header = false;
                    break;
                default:
                    throw new IllegalStateException("Invalid boolean value: " + headerNode.getText());
            }
        }

        return new StepNode(new TeragrepHdfsSaveStep(catCtx, hdfsOverwrite, hdfsPath, hdfsRetentionSpan, format, header));
    }

    // exec hdfs load path
    @Override
    public Node visitT_loadModeParameter(DPLParser.T_loadModeParameterContext ctx) {
        String hdfsPath = null;
        boolean header = true;
        String schema = "";

        if (ctx.t_pathParameter() != null && !ctx.t_pathParameter().isEmpty()) {
            if (ctx.t_pathParameter().size() != 1) {
                throw new IllegalArgumentException("Path parameter was provided multiple times! Please provide it only once.");
            }
            hdfsPath = visit(ctx.t_pathParameter(0)).toString();
        }

        TeragrepHdfsLoadStep.Format format = TeragrepHdfsLoadStep.Format.AVRO;
        if (ctx.t_hdfsFormatParameter() != null && !ctx.t_hdfsFormatParameter().isEmpty()) {
            if (ctx.t_hdfsFormatParameter().size() != 1) {
                throw new IllegalArgumentException("'format=' parameter was provided multiple times! Please provide it only once.");
            }
            TerminalNode formatNode = (TerminalNode) ctx.t_hdfsFormatParameter(0).getChild(1);
            switch (formatNode.getSymbol().getType()) {
                case DPLLexer.COMMAND_TERAGREP_MODE_CSV_FORMAT:
                    format = TeragrepHdfsLoadStep.Format.CSV;
                    break;
                case DPLLexer.COMMAND_TERAGREP_MODE_JSON_FORMAT:
                    format = TeragrepHdfsLoadStep.Format.JSON;
                    break;
            }
        }

        if (ctx.t_headerParameter() != null && !ctx.t_headerParameter().isEmpty()) {
            if (ctx.t_headerParameter().size() != 1) {
                throw new IllegalArgumentException("'header=' parameter was provided multiple times! Please provide it only once.");
            }
            TerminalNode headerNode = (TerminalNode) ctx.t_headerParameter(0).booleanType().getChild(0);
            switch (headerNode.getSymbol().getType()) {
                case DPLLexer.GET_BOOLEAN_TRUE:
                    // already defaults to true
                    break;
                case DPLLexer.GET_BOOLEAN_FALSE:
                    header = false;
                    break;
                default:
                    throw new IllegalStateException("Invalid boolean value: " + headerNode.getText());
            }
        }

        if (ctx.t_schemaParameter() != null && !ctx.t_schemaParameter().isEmpty()) {
            if (ctx.t_schemaParameter().size() != 1){
                throw new IllegalArgumentException("'schema=' parameter was provided multiple times! Please provide it only once.");
            }
            schema = new UnquotedText(new TextString(ctx.t_schemaParameter(0).stringType().getText())).read();
        }

        return new StepNode(new TeragrepHdfsLoadStep(this.catCtx, hdfsPath, format, header, schema));
    }

    // exec hdfs list path
    @Override
    public Node visitT_listModeParameter(DPLParser.T_listModeParameterContext ctx) {
        String hdfsPath = null;
        if (ctx.t_pathParameter() != null) {
            hdfsPath = visit(ctx.t_pathParameter()).toString();
        }
        else {
            LOGGER.info("Defaulting to home directory: Path was not provided to the hdfs list operation.");
        }

        return new StepNode(new TeragrepHdfsListStep(this.catCtx, hdfsPath));
    }

    // exec hdfs delete path
    @Override
    public Node visitT_deleteModeParameter(DPLParser.T_deleteModeParameterContext ctx) {
        String hdfsPath = null;
        if (ctx.t_pathParameter() != null) {
            hdfsPath = visit(ctx.t_pathParameter()).toString();
        }
        else {
            throw new IllegalArgumentException("Path was not provided to the hdfs delete operation.");
        }

        return new StepNode(new TeragrepHdfsDeleteStep(this.catCtx, hdfsPath));
    }

    // exec kafka save topic
    @Override
    public Node visitT_kafkaSaveModeParameter(DPLParser.T_kafkaSaveModeParameterContext ctx) {
        if (ctx.t_topicParameter() != null) {
            String kafkaTopic = new UnquotedText(new TextString(ctx.t_topicParameter().getText())).read();
            return new StepNode(new TeragrepKafkaStep(this.catVisitor.getHdfsPath(), catCtx, zplnConfig, kafkaTopic));
        }
        else {
            throw new IllegalArgumentException("Topic was not provided to the kafka save operation.");
        }
    }

    // exec bloom (create|update|estimate)
    @Override
    public Node visitT_bloomModeParameter(DPLParser.T_bloomModeParameterContext ctx) {
        TeragrepBloomStep.BloomMode mode = TeragrepBloomStep.BloomMode.DEFAULT;
        String inputCol = null;
        String outputCol = null;
        String estimateCol = null;
        if (ctx.t_bloomOptionParameter() != null) {
            if (ctx.t_bloomOptionParameter().COMMAND_TERAGREP_MODE_CREATE() != null) {
                // bloom create
                mode = TeragrepBloomStep.BloomMode.CREATE;
            }
            else if (ctx.t_bloomOptionParameter().COMMAND_TERAGREP_MODE_UPDATE() != null) {
                // bloom update
                mode = TeragrepBloomStep.BloomMode.UPDATE;
            }
            else if(ctx.t_bloomOptionParameter().COMMAND_TERAGREP_MODE_ESTIMATE() != null) {
                // bloom estimate
                mode = TeragrepBloomStep.BloomMode.ESTIMATE;
            }

            if (ctx.t_bloomOptionParameter().t_inputParamater() != null) {
                inputCol = new UnquotedText(new TextString(ctx.t_bloomOptionParameter().t_inputParamater().fieldType().getText())).read();
            } else {
                inputCol = "tokens";
            }

            if (ctx.t_bloomOptionParameter().t_outputParameter() != null) {
                outputCol = new UnquotedText(new TextString(ctx.t_bloomOptionParameter().t_outputParameter().fieldType().getText())).read();
            } else {
                outputCol = String.format("estimate(%s)", inputCol);
            }

            if (ctx.t_bloomOptionParameter().t_estimatesParameter() != null) {
                estimateCol = new UnquotedText(new TextString(ctx.t_bloomOptionParameter().t_estimatesParameter().fieldType().getText())).read();
            } else {
                estimateCol = String.format("estimate(%s)", inputCol);
            }
        }

        TeragrepBloomStep bloomStep = new TeragrepBloomStep(this.zplnConfig, mode, inputCol, outputCol, estimateCol);

        if (mode == TeragrepBloomStep.BloomMode.CREATE || mode == TeragrepBloomStep.BloomMode.UPDATE) {
            // create aggregate step to run before bloom create and bloom update
            TeragrepBloomStep aggregateStep = new TeragrepBloomStep(this.zplnConfig, TeragrepBloomStep.BloomMode.AGGREGATE,
                    inputCol, outputCol, estimateCol);

            return new StepListNode(Arrays.asList(aggregateStep, bloomStep));
        }

        return new StepNode(bloomStep);
    }

    @Override
    public Node visitT_tokenizerParameter(DPLParser.T_tokenizerParameterContext ctx) {
        // exec tokenizer
        String inputCol = "_raw";
        String outputCol = "tokens";
        AbstractTokenizerStep.TokenizerFormat tokenizerFormat = AbstractTokenizerStep.TokenizerFormat.STRING;
        if (ctx.t_formatParameter() != null) {
            final String format = new UnquotedText(new TextString(ctx.t_formatParameter().stringType().getText())).read();
            if (format.equalsIgnoreCase("string")) {
                tokenizerFormat = AbstractTokenizerStep.TokenizerFormat.STRING;
            } else if (format.equalsIgnoreCase("bytes")) {
                tokenizerFormat = AbstractTokenizerStep.TokenizerFormat.BYTES;
            } else {
                throw new IllegalArgumentException("Invalid format parameter '" + format + "'. Expected 'string' or 'bytes'");
            }
        }
        if (ctx.t_inputParamater() != null) {
            inputCol = new UnquotedText(new TextString(ctx.t_inputParamater().fieldType().getText())).read();
        }
        if (ctx.t_outputParameter() != null) {
            outputCol = new UnquotedText(new TextString(ctx.t_outputParameter().fieldType().getText())).read();
        }

        return new StepNode(new TokenizerStep(tokenizerFormat, inputCol, outputCol));
    }

    @Override
    public Node visitT_dynatraceParameter(DPLParser.T_dynatraceParameterContext ctx) {
        final String metricKey;
        final String url;
        if (ctx.stringType() != null) {
            metricKey = new UnquotedText(new TextString(ctx.stringType().getText())).read();
        } else if (catCtx.getNotebookUrl() != null && !catCtx.getNotebookUrl().isEmpty()){
            metricKey = catCtx.getNotebookUrl();
        } else {
            metricKey = "NoteBookID";
        }

        if (catCtx.getConfig() != null && catCtx.getConfig().hasPath("dpl.pth_10.transform.teragrep.dynatrace.api.url")) {
            url = catCtx.getConfig().getString("dpl.pth_10.transform.teragrep.dynatrace.api.url");
        } else {
            url = "http://localhost:9001/metrics/ingest";
        }

        return new StepNode(new TeragrepDynatraceStep(catCtx, metricKey, url));
    }

    // visit the hdfs path String
    @Override
    public Node visitT_pathParameter(DPLParser.T_pathParameterContext ctx) {
        String path = new UnquotedText(new TextString(ctx.stringType().getText())).read();
        return new StringNode(new Token(Token.Type.STRING, path));
    }
}
