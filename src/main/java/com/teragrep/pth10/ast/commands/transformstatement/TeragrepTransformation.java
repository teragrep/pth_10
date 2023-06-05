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
import com.teragrep.pth10.ast.ProcessingStack;
import com.teragrep.pth10.ast.Util;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.steps.teragrep.AbstractTeragrepStep;
import com.teragrep.pth10.steps.teragrep.TeragrepStep;
import com.typesafe.config.Config;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Class containing the visitor methods for all "| teragrep" subcommands
 */
public class TeragrepTransformation extends DPLParserBaseVisitor<Node> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepTransformation.class);

    private List<String> traceBuffer;
    DPLParserCatalystContext catCtx = null;
    ProcessingStack processingPipe = null;


    // host, port for relp connection
    private String host = "127.0.0.1";
    private int port = 601;
    // hdfs save
    private String hdfsPath = null;
    private String hdfsRetentionSpan = null;
    private boolean hdfsOverwrite = false;
    // kafka
    private String kafkaTopic = null;

    // current command mode
    private AbstractTeragrepStep.TeragrepCommandMode cmdMode = null;
    // can user set host and port in command?
    private boolean enforceDestination = false;

    // zeppelin config key names
    // syslog
    private static final String hostCfgItem = "dpl.pth_10.transform.teragrep.syslog.parameter.host";
    private static final String portCfgItem = "dpl.pth_10.transform.teragrep.syslog.parameter.port";
    private static final String enforceDestinationCfgItem = "dpl.pth_10.transform.teragrep.syslog.restrictedMode";

    private final boolean aggregatesUsed;

    public TeragrepStep teragrepStep = null;

    public TeragrepTransformation(DPLParserCatalystContext catCtx, ProcessingStack processingPipe, List<String> buf, boolean aggregatesUsed)
    {
        this.processingPipe = processingPipe;
        this.traceBuffer = buf;
        this.catCtx = catCtx;
        this.aggregatesUsed = aggregatesUsed;
    }

    /**
     * Topmost visitor, teragrep subcommand visiting starts from this function
     * COMMAND_MODE_TERAGREP (t_modeParameter | t_getParameter) t_hostParameter?
     */
    public Node visitTeragrepTransformation(DPLParser.TeragrepTransformationContext ctx) {
        LOGGER.info(ctx.getChildCount() + " TeragrepTransformation:" + ctx.getText());
        return teragrepTransformationEmitCatalyst(ctx);
    }

    /**
     * Visits the subrules and sets the parameters based on the parse tree.
     * Also uses the zeppelin config to set defaults, if available
     * @param ctx Main parse tree
     * @return CatalystNode
     */
    private Node teragrepTransformationEmitCatalyst(DPLParser.TeragrepTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!this.processingPipe.isEmpty()) {
            ds = this.processingPipe.pop();
        }
        this.teragrepStep = new TeragrepStep(ds);

        LOGGER.info("TeragrepTransformation Emit Catalyst");

        // get zeppelin config
        Config zplnConfig = catCtx.getConfig();
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
            LOGGER.error("Zeppelin config was not provided to the Teragrep command: host and port will be set as default, " +
                    "and no destination will be enforced.");
        }

        // execParameter or getParameter
        if (ctx.getChild(1) != null) {
            visit(ctx.getChild(1));
        }

        LOGGER.debug("Aggregates used: " + aggregatesUsed);

        // Process the actual command based on mode
        this.teragrepStep.setCmdMode(cmdMode);
        this.teragrepStep.setAggregatesUsed(aggregatesUsed);
        this.teragrepStep.setHost(host);
        this.teragrepStep.setPort(port);
        this.teragrepStep.setPath(hdfsPath);
        this.teragrepStep.setKafkaTopic(kafkaTopic);
        this.teragrepStep.setRetentionSpan(hdfsRetentionSpan);
        this.teragrepStep.setOverwrite(hdfsOverwrite);
        this.teragrepStep.setZeppelinConfig(zplnConfig);
        this.teragrepStep.setHdfsPath(this.processingPipe.getCatVisitor().getHdfsPath());

        this.teragrepStep.setCatCtx(catCtx);

        ds = this.teragrepStep.get();

        this.processingPipe.push(ds);
        return new CatalystNode(ds);
    }

    /**
     * Sets the <code>cmdMode</code> based on the parse tree given<br>
     * @param ctx getParameter sub parse tree
     * @return null, as the function sets a global variable <code>cmdMode</code>
     */
    public Node visitT_getParameter(DPLParser.T_getParameterContext ctx) {
        TerminalNode getOrSet = ((TerminalNode) ctx.getChild(0));

        // get mode
        if (getOrSet.getSymbol().getType() == DPLLexer.COMMAND_TERAGREP_MODE_GET) {
            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.GET_TERAGREP_VERSION;
        }
        // set mode
        else if (getOrSet.getSymbol().getType() == DPLLexer.COMMAND_TERAGREP_MODE_SET) {
            throw new IllegalArgumentException("Cannot set teragrep version. Did you mean: get?");
            //this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.SET_TERAGREP_VERSION;
        }

        // Only supported get/set is system version, check for validity only
        TerminalNode system = ((TerminalNode) ctx.getChild(1));
        TerminalNode version = ((TerminalNode) ctx.getChild(2));

        if (system.getSymbol().getType() != DPLLexer.COMMAND_TERAGREP_MODE_SYSTEM ||
            version.getSymbol().getType() != DPLLexer.COMMAND_TERAGREP_MODE_VERSION) {
                throw new IllegalArgumentException("Invalid arguments detected on teragrep command. Expected: 'teragrep get|set system version");
        }

        LOGGER.info("Teragrep command: Mode set to " + this.cmdMode);
        return null;
    }

    /**
     * Sets the host and port, if given
     * @param ctx hostParameter sub parse tree
     * @return null, as the function sets the global variables <code>host</code> and <code>port</code>
     */
    public Node visitT_hostParameter(DPLParser.T_hostParameterContext ctx) {
        if (ctx.getChild(1) != null) {
            this.host = ctx.getChild(1).getText();
        }

        if (ctx.getChild(3) != null) {
            this.port = Integer.parseUnsignedInt(ctx.getChild(3).getText());
        }

        LOGGER.info("Teragrep command: Host and port set to " + host + ":" + port);
        return null;
    }

    public Node visitT_execParameter(DPLParser.T_execParameterContext ctx) {
        if (ctx.t_syslogModeParameter() != null) {
            // exec syslog stream host 127.0.0.123 port 123
            DPLParser.T_hostParameterContext hostParamCtx = ctx.t_syslogModeParameter().t_hostParameter();

            if (hostParamCtx != null && !enforceDestination) {
                DPLParser.T_portParameterContext portParamCtx = hostParamCtx.t_portParameter();
                if (portParamCtx != null) {
                    // actually ip, even though named portParameterContext ...
                    this.host = portParamCtx.getText();
                }

                if (hostParamCtx.COMMAND_TERAGREP_MODE_PORT_NUM() != null) {
                    this.port = Integer.parseInt(hostParamCtx.COMMAND_TERAGREP_MODE_PORT_NUM().getText());
                }
            }

            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_SYSLOG_STREAM;
        }
        else if (ctx.t_saveModeParameter() != null) {
            // exec hdfs save path retention
            if (ctx.t_saveModeParameter().t_pathParameter() != null) {
                this.hdfsPath = Util.stripQuotes(ctx.t_saveModeParameter().t_pathParameter().stringType().getText());
            }

            if (ctx.t_saveModeParameter().t_retentionParameter() != null) {
                this.hdfsRetentionSpan = ctx.t_saveModeParameter().t_retentionParameter().spanType().getText();
            }

            if (ctx.t_saveModeParameter().t_overwriteParameter() != null) {
                TerminalNode overwriteBoolNode = (TerminalNode) ctx.t_saveModeParameter().t_overwriteParameter().booleanType().getChild(0);
                switch (overwriteBoolNode.getSymbol().getType()) {
                    case DPLLexer.GET_BOOLEAN_TRUE:
                        this.hdfsOverwrite = true;
                        break;
                    case DPLLexer.GET_BOOLEAN_FALSE:
                        this.hdfsOverwrite = false;
                        break;
                    default:
                        throw new RuntimeException("Expected a boolean value for parameter 'overwrite', instead it was something else.\n" +
                                "Try replacing the text after 'overwrite=' with 'true' or 'false'.");
                }
            }

            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_SAVE;
        }
        else if (ctx.t_loadModeParameter() != null) {
            // exec hdfs load path
            if (ctx.t_loadModeParameter().t_pathParameter() != null) {
                this.hdfsPath = Util.stripQuotes(ctx.t_loadModeParameter().t_pathParameter().stringType().getText());
            }

            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_LOAD;
        }
        else if (ctx.t_listModeParameter() != null) {
            // exec hdfs list path
            if (ctx.t_listModeParameter().t_pathParameter() != null) {
                this.hdfsPath = Util.stripQuotes(ctx.t_listModeParameter().t_pathParameter().stringType().getText());
            }
            else {
                throw new IllegalArgumentException("Path was not provided to the hdfs list operation.");
            }

            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_LIST;
        }
        else if (ctx.t_deleteModeParameter() != null) {
            // exec hdfs delete path
            if (ctx.t_deleteModeParameter().t_pathParameter() != null) {
                this.hdfsPath = Util.stripQuotes(ctx.t_deleteModeParameter().t_pathParameter().stringType().getText());
            }
            else {
                throw new IllegalArgumentException("Path was not provided to the hdfs delete operation.");
            }

            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_HDFS_DELETE;
        }
        else if (ctx.t_kafkaSaveModeParameter() != null) {
            // exec kafka save topic
            if (ctx.t_kafkaSaveModeParameter().t_topicParameter() != null) {
                this.kafkaTopic = Util.stripQuotes(ctx.t_kafkaSaveModeParameter().t_topicParameter().getText());
            }
            else {
                throw new IllegalArgumentException("Topic was not provided to the kafka save operation.");
            }

            this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_KAFKA_SAVE;
        }
        else if (ctx.t_bloomModeParameter() != null) {
            // exec bloom (create|update)
            if (ctx.t_bloomModeParameter().t_bloomOptionParameter() != null) {
                if (ctx.t_bloomModeParameter().t_bloomOptionParameter().COMMAND_TERAGREP_MODE_CREATE() != null) {
                    // bloom create
                    this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_BLOOM_CREATE;
                }
                else if (ctx.t_bloomModeParameter().t_bloomOptionParameter().COMMAND_TERAGREP_MODE_UPDATE() != null) {
                    // bloom update
                    this.cmdMode = AbstractTeragrepStep.TeragrepCommandMode.EXEC_BLOOM_UPDATE;
                }
            }
        }
        else {
            throw new RuntimeException("Teragrep command is missing a required exec parameter");
        }

        return null;
    }
}
