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
package com.teragrep.pth10.ast.commands.transformstatement;

import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.steps.convert.ConvertCommand;
import com.teragrep.pth10.steps.convert.ConvertStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The 'convert' command is used to convert field values in search results into numerical values.<br>
 * Original values are replaced by the new values, unless the 'AS' clause is used.<br>
 * The command has various functions, that work in different ways: <br>
 * auto(x) - convert x automatically using the best conversion<br>
 * ctime(x) - convert x (epoch time) to a human readable time, timeformat option can be used to change the format<br>
 * dur2sec(x) - convert x (HH:MM:SS) to epoch seconds<br>
 * memk(x) - convert x (positive number with g/m/k) to kilobytes<br>
 * mktime(x) - convert x (human readable time) to epoch. Use timeformat to specify exact format from which to
 * convert<br>
 * mstime(x) - convert x (MM:SS.SSS) to seconds<br>
 * none(x) - ignore x fields in conversion functions<br>
 * num(x) - works like auto(x), but values that cannot be converted are ignored/removed<br>
 * rmcomma(x) - remove commas from x<br>
 * rmunit(x) - removes trailing text from x<br>
 * 
 * @author eemhu
 */
public class ConvertTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertTransformation.class);
    public ConvertStep convertStep = null;

    private ConvertCommand cmd = null;

    public ConvertTransformation() {

    }

    @Override
    public Node visitConvertTransformation(DPLParser.ConvertTransformationContext ctx) {
        return convertTransformationEmitCatalyst(ctx);
    }

    /**
     * Main visiting function for convert command
     * 
     * @param ctx convert command main context
     * @return step node
     */
    private Node convertTransformationEmitCatalyst(DPLParser.ConvertTransformationContext ctx) {
        this.convertStep = new ConvertStep();
        visitChildren(ctx);
        return new StepNode(this.convertStep);
    }

    @Override
    public Node visitConvertMethodAuto(DPLParser.ConvertMethodAutoContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.AUTO, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodCtime(DPLParser.ConvertMethodCtimeContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.CTIME, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodDur2sec(DPLParser.ConvertMethodDur2secContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.DUR2SEC, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodMemk(DPLParser.ConvertMethodMemkContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.MEMK, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodMktime(DPLParser.ConvertMethodMktimeContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.MKTIME, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodMstime(DPLParser.ConvertMethodMstimeContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.MSTIME, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodNone(DPLParser.ConvertMethodNoneContext ctx) {
        // which fields to omit even when matches wildcard field
        // also known as the none() command
        List<String> fieldsToOmit = new ArrayList<>();
        // Go through the fields
        ctx.fieldListType().fieldType().forEach(ftCtx -> fieldsToOmit.add(ftCtx.getText()));

        this.convertStep.setListOfFieldsToOmit(fieldsToOmit);
        return null;
    }

    @Override
    public Node visitConvertMethodNum(DPLParser.ConvertMethodNumContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.NUM, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodRmcomma(DPLParser.ConvertMethodRmcommaContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.RMCOMMA, wcfield);
        return null;
    }

    @Override
    public Node visitConvertMethodRmunit(DPLParser.ConvertMethodRmunitContext ctx) {
        String wcfield = new UnquotedText(new TextString(ctx.fieldListType().fieldType(0).getText())).read();
        buildStep(ConvertCommand.ConvertCommandType.RMUNIT, wcfield);
        return null;
    }

    @Override
    public Node visitT_convert_timeformatParameter(DPLParser.T_convert_timeformatParameterContext ctx) {
        this.convertStep.setTimeformat(new UnquotedText(new TextString(ctx.stringType().getText())).read());
        return null;
    }

    @Override
    public Node visitT_convert_fieldRenameInstruction(DPLParser.T_convert_fieldRenameInstructionContext ctx) {
        String renameAs = ctx.fieldType().getText();

        if (this.cmd != null) {
            this.cmd.setRenameField(renameAs);
        }

        return null;
    }

    /* @Override
    public Node visitStringType(DPLParser.StringTypeContext ctx) {
        return new StringNode(new Token(Token.Type.STRING, new UnquotedText(new TextString(ctx.getText())).read()));
    }
    */
    private void buildStep(ConvertCommand.ConvertCommandType type, String wcfield) {
        this.cmd = new ConvertCommand();
        this.cmd.setCommandType(type);
        this.cmd.setFieldParam(wcfield);
        this.convertStep.addCommand(this.cmd);
    }
}
