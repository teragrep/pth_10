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
package com.teragrep.pth10.translationTests;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.commands.transformstatement.SendemailTransformation;
import com.teragrep.pth10.steps.sendemail.SendemailStep;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CharStreams;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.CommonTokenStream;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SendemailTest {

    @Test
    void testSendemailTranslation() {
        String query = " | sendemail to=hello@world sendresults=true inline=true";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        SendemailTransformation ct = new SendemailTransformation(ctx);
        ct.visitSendemailTransformation((DPLParser.SendemailTransformationContext) tree.getChild(1).getChild(0));
        SendemailStep cs = ct.sendemailStep;

        Assertions.assertNotNull(cs.getSendemailResultsProcessor());

        Map<String, String> params = cs.getSendemailResultsProcessor().getParameters();
        Map<String, String> expected = buildParamMap(new Object() {
        }.getClass().getEnclosingMethod().getName());
        Assertions.assertEquals(expected.size(), params.size());

        for (Map.Entry<String, String> ent : params.entrySet()) {
            String currentKey = ent.getKey();
            String currentValue = ent.getValue();

            String expectedValue = expected.get(currentKey);
            Assertions.assertEquals(expectedValue, currentValue);
        }
    }

    @Test
    void testSendemailTranslation2() {
        String query = " | sendemail to=hello@world sendresults=true inline=true use_ssl=true format=raw content_type=plain cc=world@hello";
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        DPLParserCatalystContext ctx = new DPLParserCatalystContext(null);
        ctx.setEarliest("-1w");

        SendemailTransformation ct = new SendemailTransformation(ctx);
        ct.visitSendemailTransformation((DPLParser.SendemailTransformationContext) tree.getChild(1).getChild(0));
        SendemailStep cs = ct.sendemailStep;

        Assertions.assertNotNull(cs.getSendemailResultsProcessor());

        Map<String, String> params = cs.getSendemailResultsProcessor().getParameters();
        Map<String, String> expected = buildParamMap(new Object() {
        }.getClass().getEnclosingMethod().getName());
        Assertions.assertEquals(expected.size(), params.size());

        for (Map.Entry<String, String> ent : params.entrySet()) {
            String currentKey = ent.getKey();
            String currentValue = ent.getValue();

            String expectedValue = expected.get(currentKey);
            Assertions.assertEquals(expectedValue, currentValue);
        }
    }

    /**
     * Build a map of all the parameters in the SendemailProcessor for testing based on the given test method name -
     * these are the expected values.
     * 
     * @param testName name of test method
     * @return map of expected parameter values
     */
    private Map<String, String> buildParamMap(final String testName) {
        final Map<String, String> params = new HashMap<>();
        if (testName.equals("testSendemailTranslation")) {
            params.put("use_tls", "false");
            params.put("server", "localhost");
            params.put("port", "25");
            params.put("use_ssl", "false");
            params.put("username", "");
            params.put("password", "***");
            params.put("fromEmail", "teragrep@localhost.localdomain");
            params.put("toEmails", "hello@world");
            params.put("bccEmails", null);
            params.put("ccEmails", null);
            params.put("subject", null);
            params.put("customMessageContent", null);
            params.put("format", "table");
            params.put("sendResults", "true");
            params.put("inline", "true");
            params.put("sendCsv", "false");
            params.put("sendPdf", "false");
            params.put("customFooterContent", null);
            params.put("paperSize", "letter");
            params.put("paperOrientation", "portrait");
            params.put("content_type", "html");
            params.put("maxinputs", "50000");
            params.put("urlToParagraph", null);
            params.put("smtpDebug", "false");
        }
        else if (testName.equals("testSendemailTranslation2")) {
            // " | sendemail to=hello@world sendresults=true inline=true use_ssl=true format=raw content_type=plain cc=world@hello";
            params.put("use_tls", "false");
            params.put("server", "localhost");
            params.put("port", "25");
            params.put("use_ssl", "true");
            params.put("username", "");
            params.put("password", "***");
            params.put("fromEmail", "teragrep@localhost.localdomain");
            params.put("toEmails", "hello@world");
            params.put("bccEmails", null);
            params.put("ccEmails", "world@hello");
            params.put("subject", null);
            params.put("customMessageContent", null);
            params.put("format", "raw");
            params.put("sendResults", "true");
            params.put("inline", "true");
            params.put("sendCsv", "false");
            params.put("sendPdf", "false");
            params.put("customFooterContent", null);
            params.put("paperSize", "letter");
            params.put("paperOrientation", "portrait");
            params.put("content_type", "plain");
            params.put("maxinputs", "50000");
            params.put("urlToParagraph", null);
            params.put("smtpDebug", "false");
        }
        else {
            Assertions.fail("Invalid test name provided for buildParamMap: " + testName);
        }

        return params;
    }
}
