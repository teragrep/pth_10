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

import com.teragrep.pth10.ast.*;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.steps.join.JoinStep;
import com.teragrep.pth10.steps.subsearch.SubsearchStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Base transformation class for the join command. Allows the user to join two searches (their result sets) together
 */
public class JoinTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformation.class);
    private final DPLParserCatalystContext catCtx;
    private final DPLParserCatalystVisitor catVisitor;
    private String pathForSubsearchSave = "/tmp/pth_10/join";
    public JoinStep joinStep = null;

    public JoinTransformation(DPLParserCatalystVisitor catVisitor, DPLParserCatalystContext catCtx) {
        this.catVisitor = catVisitor;
        this.catCtx = catCtx;
    }

    /**
     * {@literal <left-dataset> | join left=L right=R where L.pid = R.pid [subsearch]}
     * <hr>
     * Command examples:<br>
     * <code>... | join product_id [search vendors]</code> combine results from main search with result from a subsearch
     * "search vendors". Sets are joined on the product_id field.
     * <hr>
     * <code>... | join product_id max=0 [search vendors] </code> returns all subsearch rows; by default only first one
     * is returned.
     * <hr>
     * <code>... | join left=L right=R where L.product.id=
     * 			R.product_id [search vendors]		</code> combine results from a search with the vendors dataset.
     * <hr>
     * <code>... | join left=L right=R where L.product_id=
     * 			R.pid [search vendors]				</code> different field names
     * <hr>
     * A maximum of 50,000 rows in the right-side dataset can be joined with the left-side dataset
     * <pre>COMMAND_MODE_JOIN (t_join_joinOptionsParameter)*? fieldListType? t_join_unnamedDatasetParameter</pre>
     */
    @Override
    public Node visitJoinTransformation(DPLParser.JoinTransformationContext ctx) {
        LOGGER.info("visitJoinTransformation incoming: text=<{}>", ctx.getText());
        return joinTransformationEmitCatalyst(ctx);
    }

    /**
     * Gets the join options from the command, performs a subsearch - which is saved to HDFS, and performs the
     * stream-static join.
     * 
     * @param ctx
     * @return
     */
    private Node joinTransformationEmitCatalyst(DPLParser.JoinTransformationContext ctx) {
        this.joinStep = new JoinStep();

        Dataset<Row> subSearchDs = null; // Contains the subsearch result dataframe (right side)
        List<String> listOfFields = null; // Contains names of all the fields as strings (Java List)

        // Variables used for all the different join options / parameters
        String joinMode = "inner";
        Boolean usetime = false;
        Boolean earlier = true;
        Boolean overwrite = true;
        Integer max = 1;

        // Go through all children
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            LOGGER.debug("Child(<{}>) content: <{}>", i, child.getText());

            if (child instanceof DPLParser.T_join_joinOptionsParameterContext) {
                // Get all the different join options / parameters
                LOGGER.debug("Child(<{}>) is instanceof join options", i);

                for (int j = 0; j < child.getChildCount(); j++) {
                    ParseTree joinOptionsChild = child.getChild(j);

                    if (joinOptionsChild instanceof DPLParser.T_join_typeParameterContext) {
                        StringNode typeParam = (StringNode) visit(joinOptionsChild);
                        joinMode = typeParam.toString();
                    }
                    else if (joinOptionsChild instanceof DPLParser.T_join_usetimeParameterContext) {
                        StringNode usetimeParam = (StringNode) visit(joinOptionsChild);
                        usetime = (Objects.equals(usetimeParam.toString(), "true"));
                    }
                    else if (joinOptionsChild instanceof DPLParser.T_join_earlierParameterContext) {
                        StringNode earlierParam = (StringNode) visit(joinOptionsChild);
                        earlier = (Objects.equals(earlierParam.toString(), "true"));
                    }
                    else if (joinOptionsChild instanceof DPLParser.T_join_overwriteParameterContext) {
                        StringNode overwriteParam = (StringNode) visit(joinOptionsChild);
                        overwrite = (Objects.equals(overwriteParam.toString(), "true"));
                    }
                    else if (joinOptionsChild instanceof DPLParser.T_join_maxParameterContext) {
                        StringNode maxParam = (StringNode) visit(joinOptionsChild);
                        max = Integer.parseInt(maxParam.toString());
                    }
                }
            }
            else if (child instanceof DPLParser.T_join_unnamedDatasetParameterContext) {
                // perform subsearch
                LOGGER.debug("Child <{}> is instanceof dataset parameter", i);
                visit(child);
            }
            else if (child instanceof DPLParser.FieldListTypeContext) {
                LOGGER.debug("Child <{}> is instanceof fieldlist type", i);
                // Visit FieldListType and place fields as Columns in seqOfFields
                StringListNode listOfFieldsNode = (StringListNode) visit(child);

                listOfFields = listOfFieldsNode.asList();
            }
            else if (child instanceof TerminalNode) {
                LOGGER.debug("Child <{}> is instanceof terminalnode", i);
                // should be COMMAND_JOIN_MODE
                // no action needed as it is just a command keyword
            }
            // everything else is invalid and not processed
        }

        LOGGER.debug("--- Join parameters ---");
        LOGGER.debug("join mode= <{}>", joinMode);
        LOGGER.debug("usetime= <{}>", usetime);
        LOGGER.debug("earlier= <{}>", earlier);
        LOGGER.debug("overwrite= <{}>", overwrite);
        LOGGER.debug("max= <{}>", max);
        LOGGER.debug("-----------------------");

        this.pathForSubsearchSave = this.catVisitor.getHdfsPath();

        // step
        this.joinStep.setJoinMode(joinMode);
        this.joinStep.setEarlier(earlier);
        this.joinStep.setMax(max);
        this.joinStep.setOverwrite(overwrite);
        this.joinStep.setListOfFields(listOfFields);
        this.joinStep.setUsetime(usetime);
        this.joinStep.setPathForSubsearchSave(pathForSubsearchSave);
        this.joinStep.setSubSearchDataset(subSearchDs);
        this.joinStep.setCatCtx(catCtx);

        return new StepNode(joinStep);
    }

    @Override
    public Node visitFieldListType(DPLParser.FieldListTypeContext ctx) {
        Node rv = null;

        List<String> fieldList = new ArrayList<>();
        ctx.children.forEach(field -> {
            String fieldName = new UnquotedText(new TextString(field.getText())).read();

            if (!fieldName.equals(",")) {
                fieldList.add(fieldName);
            }

        });

        rv = new StringListNode(fieldList);
        return rv;
    }

    // COMMAND_JOIN_TYPE (COMMAND_JOIN_GET_TYPE_MODE_OUTER|COMMAND_JOIN_GET_TYPE_MODE_LEFT|
    // COMMAND_JOIN_GET_TYPE_MODE_INNER)
    @Override
    public Node visitT_join_typeParameter(DPLParser.T_join_typeParameterContext ctx) {
        Node rv = t_join_typeParameterEmitCatalyst(ctx);
        return rv;
    }

    private Node t_join_typeParameterEmitCatalyst(DPLParser.T_join_typeParameterContext ctx) {
        Node rv = null;

        TerminalNode type = (TerminalNode) ctx.getChild(1);

        rv = new StringNode(new Token(Token.Type.STRING, type.getText()));
        return rv;
    }

    // COMMAND_JOIN_MODE_USETIME booleanType
    @Override
    public Node visitT_join_usetimeParameter(DPLParser.T_join_usetimeParameterContext ctx) {
        Node rv = t_join_usetimeParameterEmitCatalyst(ctx);
        return rv;
    }

    private Node t_join_usetimeParameterEmitCatalyst(DPLParser.T_join_usetimeParameterContext ctx) {
        Node rv = null;
        // COMMAND_JOIN_MODE_USETIME booleanType

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        rv = getBooleanFromTerminalNode(booleanValue);
        return rv;
    }

    // COMMAND_JOIN_MODE_EARLIER booleanType
    @Override
    public Node visitT_join_earlierParameter(DPLParser.T_join_earlierParameterContext ctx) {
        Node rv = t_join_earlierParameterEmitCatalyst(ctx);
        return rv;
    }

    private Node t_join_earlierParameterEmitCatalyst(DPLParser.T_join_earlierParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        rv = getBooleanFromTerminalNode(booleanValue);
        return rv;
    }

    // COMMAND_JOIN_MODE_OVERWRITE booleanType
    @Override
    public Node visitT_join_overwriteParameter(DPLParser.T_join_overwriteParameterContext ctx) {
        Node rv = t_join_overwriteParameterEmitCatalyst(ctx);
        return rv;
    }

    private Node t_join_overwriteParameterEmitCatalyst(DPLParser.T_join_overwriteParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        rv = getBooleanFromTerminalNode(booleanValue);
        return rv;
    }

    // COMMAND_JOIN_MODE_MAX integerType
    @Override
    public Node visitT_join_maxParameter(DPLParser.T_join_maxParameterContext ctx) {
        Node rv = t_join_maxParameterEmitCatalyst(ctx);
        return rv;
    }

    private Node t_join_maxParameterEmitCatalyst(DPLParser.T_join_maxParameterContext ctx) {
        Node rv = null;

        TerminalNode integerValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = integerValue.getText();

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    // subsearchStatement: [ PIPE? subsearchTransformStatement ]
    @Override
    public Node visitT_join_unnamedDatasetParameter(DPLParser.T_join_unnamedDatasetParameterContext ctx) {
        Node rv = t_join_unnamedDatasetParameterEmitCatalyst(ctx);
        return rv;
    }

    private Node t_join_unnamedDatasetParameterEmitCatalyst(DPLParser.T_join_unnamedDatasetParameterContext ctx) {
        LOGGER
                .info(
                        "Visiting unnamedDatasetParameter: text=<{}>, with children=<{}>", ctx.getText(),
                        ctx.getChildCount()
                );

        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            LOGGER.debug("child on unnamedDatasetParam: text=<{}>", child.getText());

            if (child instanceof DPLParser.SubsearchStatementContext) {
                LOGGER.debug("child instanceof SubsearchStmtCtx: text=<{}>", child.getText());
                DPLParserCatalystVisitor ssVisitor = new DPLParserCatalystVisitor(catCtx);
                StepNode ssStepNode = (StepNode) ssVisitor
                        .visitSubsearchStatement(((DPLParser.SubsearchStatementContext) child));
                SubsearchStep ssStep = (SubsearchStep) ssStepNode.get();

                this.joinStep.setSubsearchStep(ssStep);
            }

        }

        return null;
    }

    /**
     * Converts a TerminalNode containing BooleanType into StringNode with content "true" or "false"
     * 
     * @param tn TerminalNode containing a BooleanType
     * @return StringNode with either value "true" or "false"
     */
    private StringNode getBooleanFromTerminalNode(TerminalNode tn) {
        String value = "";

        switch (tn.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        return new StringNode(new Token(Token.Type.STRING, value));
    }
}
