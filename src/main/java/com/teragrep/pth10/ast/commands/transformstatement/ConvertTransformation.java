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
import com.teragrep.pth10.ast.bo.StringNode;
import com.teragrep.pth10.ast.bo.Token;
import com.teragrep.pth10.steps.convert.AbstractConvertStep;
import com.teragrep.pth10.steps.convert.ConvertStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The 'convert' command is used to convert field values in search results into numerical values.<br>
 * Original values are replaced by the new values, unless the 'AS' clause is used.<br>
 *
 * The command has various functions, that work in different ways: <br>
 * auto(x) - convert x automatically using the best conversion<br>
 * ctime(x) - convert x (epoch time) to a human readable time, timeformat option can be used to change the format<br>
 * dur2sec(x) - convert x (HH:MM:SS) to epoch seconds<br>
 * memk(x) - convert x (positive number with g/m/k) to kilobytes<br>
 * mktime(x) - convert x (human readable time) to epoch. Use timeformat to specify exact format from which to convert<br>
 * mstime(x) - convert x (MM:SS.SSS) to seconds<br>
 * none(x) - ignore x fields in conversion functions<br>
 * num(x) - works like auto(x), but values that cannot be converted are ignored/removed<br>
 * rmcomma(x) - remove commas from x<br>
 * rmunit(x) - removes trailing text from x<br>
 * @author p000043u
 *
 */
public class ConvertTransformation extends DPLParserBaseVisitor<Node> {
    private List<String> traceBuffer = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertTransformation.class);

    private Dataset<Row> ds = null;
    private ProcessingStack processingPipe = null;
    private boolean aggregatesUsed = false;
    private DPLParserCatalystContext catCtx = null;

    public ConvertStep convertStep = null;

    public ConvertTransformation(List<String> buf, ProcessingStack stack, DPLParserCatalystContext catCtx) {
        this.traceBuffer = buf;
        this.processingPipe = stack;
        this.catCtx = catCtx;
    }

    public boolean getAggregatesUsed() {
        return this.aggregatesUsed;
    }

    public void setAggregatesUsed(boolean newValue) {
        this.aggregatesUsed = newValue;
    }

    @Override
    public Node visitConvertTransformation(DPLParser.ConvertTransformationContext ctx) {
        Node rv = convertTransformationEmitCatalyst(ctx);
        return rv;
    }

    /**
     * This function first gets the fields to omit so they can be used in the convert functions,
     * and also timeformat. After this the actual conversion functions will be executed.
     * @param ctx
     * @return
     */
    private Node convertTransformationEmitCatalyst(DPLParser.ConvertTransformationContext ctx) {
        Dataset<Row> ds = null;
        if (!processingPipe.isEmpty()) {
            ds = processingPipe.pop();
        }
        this.convertStep = new ConvertStep(ds);

        Node rv = null;
        String timeformat = "%m/%d/%Y %H:%M:%S"; // default timeformat if no 'timeformat=' provided

        // Which fields to omit even when matches wildcard field
        // also known as the none() command
        // It is gone through here so the list of fields to omit is ready for
        // the other commands.
        List<DPLParser.ConvertMethodNoneContext> noneContextList = ctx.convertMethodNone();
        List<String> listOfFieldsToOmit = new ArrayList<>();
        for (DPLParser.ConvertMethodNoneContext c : noneContextList) {
            listOfFieldsToOmit.add(c.getChild(2).getText());
        }

        this.convertStep.setListOfFieldsToOmit(listOfFieldsToOmit);

        // custom timeformat, if any
        DPLParser.T_convert_timeformatParameterContext timeformatContext = ctx.t_convert_timeformatParameter();
        if (timeformatContext != null) {
            timeformat = ((StringNode) visit(timeformatContext)).toString();
        }

        this.convertStep.setTimeformat(timeformat);

        // Go through all the conversion functions
        AbstractConvertStep.ConvertCommand cmd = null;
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (child instanceof TerminalNode) {
                // skip keyword 'convert'
            }
            else if (child instanceof DPLParser.ConvertMethodAutoContext) {
                // auto(): convert field to numeric with best conversion
                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.AUTO);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);
            }
            else if (child instanceof DPLParser.ConvertMethodNumContext) {
                // num(): convert field to numeric with best conversion
                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.NUM);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);
            }
            else if (child instanceof DPLParser.ConvertMethodMktimeContext) {
                // mktime(): human readable time to epoch
                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.MKTIME);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);
            }
            else if (child instanceof DPLParser.ConvertMethodCtimeContext) {
                // ctime(): epoch to human readable time

                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.CTIME);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);

            }
            else if (child instanceof DPLParser.ConvertMethodDur2secContext) {
                // dur2sec(): Convert a duration format "[D+]HH:MM:SS" to seconds

                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.DUR2SEC);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);

            }
            else if (child instanceof DPLParser.ConvertMethodMemkContext) {
                // memk(): positive number followed by optional k/m/g. default k. output quantity of kilobytes
                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.MEMK);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);

            }
            else if (child instanceof DPLParser.ConvertMethodMstimeContext) {
                // mstime(): mm:ss.sss to milliseconds
                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.MSTIME);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);

            }
            else if (child instanceof DPLParser.ConvertMethodRmcommaContext) {
                // rmcomma(): remove commas from field
                String field = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.RMCOMMA);
                cmd.setFieldParam(field);
                this.convertStep.addCommand(cmd);

            }
            else if (child instanceof DPLParser.ConvertMethodRmunitContext) {
                // rmunit(): remove trailing non-numeric characters (e.g. 100sec -> 100)
                String wcfield = Util.stripQuotes(child.getChild(2).getText());

                cmd = new AbstractConvertStep.ConvertCommand();
                cmd.setCommandType(AbstractConvertStep.ConvertCommand.ConvertCommandType.RMUNIT);
                cmd.setFieldParam(wcfield);
                this.convertStep.addCommand(cmd);

            }

            // AS <new-field-name>
            if (child instanceof DPLParser.T_convert_fieldRenameInstructionContext) {
                if (cmd != null) {
                    cmd.setRenameField(child.getChild(1).getText());
                }
            }
        }

        ds = this.convertStep.get();

        processingPipe.push(ds);
        rv = new CatalystNode(ds);
        return rv;
    }

    @Override
    public Node visitStringType(DPLParser.StringTypeContext ctx) {
        return new StringNode(new Token(Token.Type.STRING, Util.stripQuotes(ctx.getText())));
    }
}
