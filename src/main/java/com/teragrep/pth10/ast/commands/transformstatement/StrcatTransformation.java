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

import com.teragrep.pth10.ast.NullValue;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.steps.strcat.StrcatStep;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Base transformation class for the command <code>strcat</code><br>
 * Used to concatenate two or more field values and/or string literals into a destination field
 */
public class StrcatTransformation extends DPLParserBaseVisitor<Node> {

    private final Logger LOGGER = LoggerFactory.getLogger(StrcatTransformation.class);
    public StrcatStep strcatStep = null;
    private final NullValue nullValue;

    public StrcatTransformation(NullValue nullValue) {
        this.nullValue = nullValue;
    }

    /**
     * <pre>
     * -- Command info: --
     *
     * {@literal strcat [allrequired=<bool>] <source-fields> <dest-field>}
     * Concatenates string values from 2 or more fields, combines string values and
     * literals into a new field. The destination field name is specified at the end of
     * the strcat command. allrequired is not a required argument, and it can be omitted.
     *
     * -- Grammar rules: --
     *
     * strcatTransformation
     *  : COMMAND_MODE_STRCAT (t_strcat_allrequiredParameter)? t_strcat_srcfieldsParameter fieldType
     *  ;
     *
     * t_strcat_allrequiredParameter
     *  : COMMAND_STRCAT_MODE_ALLREQUIRED booleanType
     *  ;
     *
     * t_strcat_srcfieldsParameter
     *  : (fieldType | stringType) (fieldType | stringType)+
     *  ;
     *
     * -- SQL: --
     *
     * strcat allRequired=bool field1 field2 ... fieldN destField
     *  to
     * SELECT CONCAT(field1, field2, ..., fieldN) AS destField FROM ˇtemporaryDPLViewˇ
     *  </pre>
     */
    @Override
    public Node visitStrcatTransformation(DPLParser.StrcatTransformationContext ctx) {
        LOGGER.debug(String.format("Child count: %s in StrcatTransformation: %s", ctx.getChildCount(), ctx.getText()));
        return strcatTransformationEmitCatalyst(ctx);
    }

    /**
     * Emit catalyst from strcatTransformation
     * 
     * @param ctx StrcatTransformationContext
     * @return CatalystNode containing resultset
     */
    public Node strcatTransformationEmitCatalyst(DPLParser.StrcatTransformationContext ctx) {
        // syntax: strcat allrequired src-fields dest-field
        // child#	0		1			2			3
        this.strcatStep = new StrcatStep(nullValue);
        visitChildren(ctx);
        return new StepNode(this.strcatStep);
    }

    /**
     * <pre>
     * t_strcat_allrequiredParameter
     *  : COMMAND_STRCAT_MODE_ALLREQUIRED booleanType
     *  ;
     *
     *  If the parameter exists, the second child (child#1) contains the boolean type whether or not all source fields are required
     *  </pre>
     */
    @Override
    public Node visitT_strcat_allrequiredParameter(DPLParser.T_strcat_allrequiredParameterContext ctx) {
        this.strcatStep.setAllRequired(ctx.booleanType().GET_BOOLEAN_TRUE() != null);
        return null;
    }

    /**
     * <pre>
     * 	t_strcat_srcfieldsParameter
     *   : (fieldType | stringType) (fieldType | stringType)+
     *   ;
     *
     *  Contains all the source fields, one or more.
     *   Adds all fields into an array, while stripping quotes from each one of the fields.</pre>
     */
    @Override
    public Node visitT_strcat_srcfieldsParameter(DPLParser.T_strcat_srcfieldsParameterContext ctx) {
        List<String> srcFields = new ArrayList<>();

        ctx.children.forEach(child -> srcFields.add(child.getText()));

        this.strcatStep.setListOfFields(srcFields);
        this.strcatStep.setNumberOfSrcFieldsOriginally(ctx.getChildCount());

        return null;
    }

    @Override
    public Node visitT_strcat_destfieldParameter(DPLParser.T_strcat_destfieldParameterContext ctx) {
        this.strcatStep.setDestField(ctx.fieldType().getText());
        return null;
    }
}
