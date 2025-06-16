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
import com.teragrep.pth10.ast.bo.Node;
import com.teragrep.pth10.ast.bo.StepNode;
import com.teragrep.pth10.steps.predict.AbstractPredictStep;
import com.teragrep.pth10.steps.predict.PredictStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PredictTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredictTransformation.class);

    public PredictStep predictStep = null;

    public PredictTransformation() {

    }

    @Override
    public Node visitPredictTransformation(DPLParser.PredictTransformationContext ctx) {
        this.predictStep = new PredictStep();

        List<Column> listOfColumnsToPredict = new ArrayList<>();
        AbstractPredictStep.Algorithm algorithm = AbstractPredictStep.Algorithm.LL;
        String correlateField = null;
        String suppressField = null;
        int futureTimespan = 5;
        int holdback = 0;
        int period = 0;
        int upper = 95;
        int lower = 95;
        String upperField = null;
        String lowerField = null;

        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);

            if (child instanceof DPLParser.FieldTypeContext) {
                ParseTree nextChild = ctx.getChild(i + 1);
                // <field> AS <new-name>
                if (nextChild instanceof DPLParser.T_predict_fieldRenameInstructionContext) {
                    listOfColumnsToPredict
                            .add(functions.col(new UnquotedText(new TextString(child.getText())).read()).as(new UnquotedText(new TextString(nextChild.getChild(1).getText())).read()));
                    i++; // Skip next child, as it would be the same fieldRenameInstruction again
                }
                // <field>
                else {
                    listOfColumnsToPredict.add(functions.col(new UnquotedText(new TextString(child.getText())).read()));
                }
            }
            else if (child instanceof DPLParser.T_predict_pdAlgoOptionParameterContext) {
                TerminalNode algoType = (TerminalNode) child.getChild(1);
                switch (algoType.getSymbol().getType()) {
                    case DPLLexer.COMMAND_PREDICT_ALGORITHM_MODE_Ll:
                        algorithm = AbstractPredictStep.Algorithm.LL;
                        break;
                    case DPLLexer.COMMAND_PREDICT_ALGORITHM_MODE_Llt:
                        algorithm = AbstractPredictStep.Algorithm.LLT;
                        break;
                    case DPLLexer.COMMAND_PREDICT_ALGORITHM_MODE_Llp:
                        algorithm = AbstractPredictStep.Algorithm.LLP;
                        break;
                    case DPLLexer.COMMAND_PREDICT_ALGORITHM_MODE_Llp5:
                        algorithm = AbstractPredictStep.Algorithm.LLP5;
                        break;
                    case DPLLexer.COMMAND_PREDICT_ALGORITHM_MODE_Llb:
                        algorithm = AbstractPredictStep.Algorithm.LLB;
                        break;
                    case DPLLexer.COMMAND_PREDICT_ALGORITHM_MODE_BILL:
                        algorithm = AbstractPredictStep.Algorithm.BILL;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid algorithm type provided");
                }
            }
            else if (child instanceof DPLParser.T_predict_pdCorrelateOptionParameterContext) {
                DPLParser.FieldTypeContext ftCtx = ((DPLParser.T_predict_pdCorrelateOptionParameterContext) child)
                        .fieldType();
                correlateField = new UnquotedText(new TextString(ftCtx.getText())).read();
            }
            else if (child instanceof DPLParser.T_predict_pdFutureTimespanOptionParameterContext) {
                DPLParser.NumberTypeContext ntCtx = ((DPLParser.T_predict_pdFutureTimespanOptionParameterContext) child)
                        .numberType();
                futureTimespan = Integer.parseInt(ntCtx.getText());
            }
            else if (child instanceof DPLParser.T_predict_pdHoldbackOptionParameterContext) {
                DPLParser.NumberTypeContext ntCtx = ((DPLParser.T_predict_pdHoldbackOptionParameterContext) child)
                        .numberType();
                holdback = Integer.parseInt(ntCtx.getText());
            }
            else if (child instanceof DPLParser.T_predict_pdPeriodOptionParameterContext) {
                DPLParser.NumberTypeContext ntCtx = ((DPLParser.T_predict_pdPeriodOptionParameterContext) child)
                        .numberType();
                period = Integer.parseInt(ntCtx.getText());
            }
            else if (child instanceof DPLParser.T_predict_pdUpperOptionParameterContext) {
                DPLParser.T_predict_pdUpperOptionParameterContext uopCtx = (DPLParser.T_predict_pdUpperOptionParameterContext) child;
                upper = Integer.parseInt(uopCtx.integerType().getText());
                upperField = new UnquotedText(new TextString(uopCtx.fieldType().getText())).read();
            }
            else if (child instanceof DPLParser.T_predict_pdLowerOptionParameterContext) {
                DPLParser.T_predict_pdLowerOptionParameterContext lopCtx = (DPLParser.T_predict_pdLowerOptionParameterContext) child;
                lower = Integer.parseInt(lopCtx.integerType().getText());
                lowerField = new UnquotedText(new TextString(lopCtx.fieldType().getText())).read();
            }
            else if (child instanceof DPLParser.T_predict_pdSuppressOptionParameterContext) {
                suppressField = new UnquotedText(
                        new TextString(
                                ((DPLParser.T_predict_pdSuppressOptionParameterContext) child).fieldType().getText()
                        )
                ).read();
            }
            else if (child instanceof TerminalNode) {
                // skip TerminalNode
            }
            else {
                throw new IllegalStateException("Unexpected parameter encountered: " + child.getClass());
            }
        }

        this.predictStep.setListOfColumnsToPredict(listOfColumnsToPredict);
        this.predictStep.setAlgorithm(algorithm);
        this.predictStep.setUpperField(upperField);
        this.predictStep.setLowerField(lowerField);
        this.predictStep.setHoldback(holdback);
        this.predictStep.setLower(lower);
        this.predictStep.setPeriod(period);
        this.predictStep.setUpper(upper);
        this.predictStep.setCorrelateField(correlateField);
        this.predictStep.setSuppressField(suppressField);
        this.predictStep.setFutureTimespan(futureTimespan);

        return new StepNode(predictStep);
    }

}
