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

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import com.teragrep.pth10.ast.bo.*;
import com.teragrep.pth10.ast.commands.transformstatement.sendemail.SendemailResultsProcessor;
import com.teragrep.pth10.steps.sendemail.SendemailStep;
import com.typesafe.config.Config;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import com.teragrep.pth_03.antlr.DPLParserBaseVisitor;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.ParseTree;
import com.teragrep.pth_03.shaded.org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base transformation class for the 'sendemail' command <pre>... | sendemail to=...</pre>
 */
public class SendemailTransformation extends DPLParserBaseVisitor<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendemailTransformation.class);
    private DPLParserCatalystContext catCtx = null;
    public SendemailStep sendemailStep = null;

    public SendemailTransformation(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    /**
     * <pre>
     * Command info:
     * Generates email notifications. Email search results to specified email addresses.
     * SMTP server needs to be available to send email.
     * 
     * Syntax:
     * sendemail
     * 	to= list of emails [REQUIRED, other params optional]
     *  cc= list of emails
     *  bcc= list of emails
     *  subject= string
     *  format= csv|table|raw
     *  inline= bool
     *  sendresults= bool
     *  sendpdf= bool
     *  priority= highest|high|normal|low|lowest
     *  server= string
     *  width_sort_columns= bool
     *  graceful= bool
     *  content_type= html | plain
     *  message= string
     *  sendcsv= bool
     *  use_ssl= bool
     *  use_tls= bool
     *  pdfview= string
     *  papersize= letter|legal|ledger|a2|a3|a4|a5
     *  paperorientation= portrait|landscape
     *  maxinputs= int
     *  maxtime= int m|s|h|d
     *  footer= string
     * </pre>
     */
    @Override
    public Node visitSendemailTransformation(DPLParser.SendemailTransformationContext ctx) {
        Node rv = sendemailTransformationEmitCatalyst(ctx);
        return rv;
    }

    /**
     * Sets the parameters based on the given command, and processes the email using the
     * {@link SendemailResultsProcessor}
     * 
     * @param ctx
     * @return
     */
    private Node sendemailTransformationEmitCatalyst(DPLParser.SendemailTransformationContext ctx) {

        // Parameters from given sendemail command
        String toEmails = null, fromEmail = "teragrep@localhost.localdomain", ccEmails = null, bccEmails = null;
        String subject = null, customMessageContent = null, customFooterContent = null;
        boolean graceful = false;
        int priority = 3; // 5-lowest 4-low 3-normal 2-high 1-highest

        boolean sendResults = false; // default false
        boolean inline = false; // default false
        String inlineFormat = "table"; // default "table", can be "raw" or "csv"
        String content_type = "html"; // default "html", can be "plain"
        boolean sendCsv = false, sendPdf = false;
        String pdfView = null;

        String paperSize = "letter"; // for pdfs; default "letter", can be "legal", "ledger" or from "a2" to "a5"
        String paperOrientation = "portrait"; // default "portrait", can be "landscape"

        String server = "localhost";
        int port = 25;
        boolean use_tls = false, use_ssl = false; // default false

        boolean width_sort_columns = true; // only valid for contentType="plain"

        int maxInputs = 50000; // default 50k
        String maxTime = "";

        boolean restrictedMode = false; // issue #231 restricted mode

        boolean smtpDebug = false; // enable smtp debugging

        this.sendemailStep = new SendemailStep();

        // Go through zeppelin config here, so parameters given in command can overwrite these if needed
        // credentials for smtp server
        String username = "";
        String password = "";

        // get smtp username and password from zeppelin config
        Config zplnConfig = catCtx.getConfig();

        final String usernameCfgItem = "dpl.smtp.username";
        final String passwordCfgItem = "dpl.smtp.password";
        final String serverCfgItem = "dpl.smtp.server";
        final String restrictedModeCfgItem = "dpl.pth_10.transform.sendemail.restrictedMode"; // issue #231 restricted mode
        final String globalFromParameterCfgItem = "dpl.pth_10.transform.sendemail.parameter.from"; // issue #232 global config for 'from' parameter
        final String smtpDebugParameterCfgItem = "dpl.smtp.debug"; // config for smtp debug mode
        final String smtpEncryptionParameterCfgItem = "dpl.smtp.encryption"; // config for smtp encryption mode (PLAIN, SSL or TLS)

        if (zplnConfig != null && zplnConfig.hasPath(usernameCfgItem)) {
            username = zplnConfig.getString(usernameCfgItem);
            LOGGER.debug("Sendemail config: username=<[{}]>", username);
        }

        if (zplnConfig != null && zplnConfig.hasPath(passwordCfgItem)) {
            password = zplnConfig.getString(passwordCfgItem);
            LOGGER.debug("Sendemail config: password=<[{}]>", (password != null ? "***" : "null"));
        }

        if (zplnConfig != null && zplnConfig.hasPath(smtpEncryptionParameterCfgItem)) {
            String value = zplnConfig.getString(smtpEncryptionParameterCfgItem);

            if (value.equals("PLAIN")) {
                use_ssl = false;
                use_tls = false;
            }
            else if (value.equals("SSL")) {
                use_ssl = true;
                use_tls = false;
            }
            else if (value.equals("TLS")) {
                use_ssl = false;
                use_tls = true;
            }
            else {
                throw new IllegalArgumentException(
                        "Invalid value for '" + smtpEncryptionParameterCfgItem
                                + "'. It must be 'PLAIN', 'SSL' or 'TLS' instead of '" + value + "'"
                );
            }
        }

        // replace server given in server parameter if present in zeppelin config
        if (zplnConfig != null && zplnConfig.hasPath(serverCfgItem)) {
            String serverString = zplnConfig.getString(serverCfgItem);

            String[] hostAndPort = serverString.split(":");
            // more than one item, means port must be present
            if (hostAndPort.length > 1) {
                server = hostAndPort[0];
                port = Integer.parseInt(hostAndPort[1]);
                LOGGER.debug("Sendemail config: server host=<[{}]> port=<[{}]>", server, port);
            }
            // One item (or less), just server
            else {
                server = hostAndPort[0];
                LOGGER.debug("Sendemail config: server host=<[{}]> (default port <{}>)", server, port);
            }
        }

        // issue #231 and #232 parameters from zeppelin config (restrictedMode and fromEmail)
        if (zplnConfig != null && zplnConfig.hasPath(restrictedModeCfgItem)) {
            restrictedMode = zplnConfig.getBoolean(restrictedModeCfgItem);
            LOGGER.debug("Sendemail config: Restricted config=<[{}]>", restrictedMode);
        }

        if (zplnConfig != null && zplnConfig.hasPath(globalFromParameterCfgItem)) {
            fromEmail = zplnConfig.getString(globalFromParameterCfgItem);
            LOGGER.debug("Sendemail config: Global from parameter=<[{}]>", fromEmail);
        }

        if (zplnConfig != null && zplnConfig.hasPath(smtpDebugParameterCfgItem)) {
            smtpDebug = zplnConfig.getBoolean(smtpDebugParameterCfgItem);
            LOGGER.debug("Sendemail config: SMTP Debug parameter=<[{}]>", smtpDebug);
        }

        boolean hasForbiddenConfig = false; // set to true if other parameters than email subject and to is found

        // Go through all the parameters given in the command using the for loop below.
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);

            if (child instanceof TerminalNode) {
                // COMMAND_MODE_SENDEMAIL
                // skip keyword "sendemail"
                continue;
            }
            else if (child instanceof DPLParser.T_sendemail_toOptionParameterContext) {
                // t_sendemail_toOptionParameter
                toEmails = ((StringNode) visit(child)).toString();
            }
            else if (child instanceof DPLParser.T_sendemail_fromOptionParameterContext) {
                // t_sendemail_fromOptionParameter
                fromEmail = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_ccOptionParameterContext) {
                // t_sendemail_ccOptionParameter
                ccEmails = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_bccOptionParameterContext) {
                // t_sendemail_bccOptionParameter
                bccEmails = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_subjectOptionParameterContext) {
                // t_sendemail_subjectOptionParameter
                subject = ((StringNode) visit(child)).toString();
            }
            else if (child instanceof DPLParser.T_sendemail_messageOptionParameterContext) {
                // T_sendemail_messageOptionParameter
                customMessageContent = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_footerOptionParameterContext) {
                // T_sendemail_footerOptionParameter
                customFooterContent = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_sendresultsOptionParameterContext) {
                // T_sendemail_sendresultsOptionParameter
                sendResults = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_inlineOptionParameterContext) {
                // t_sendemail_inlineOptionParameter
                inline = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_formatOptionParameterContext) {
                // t_sendemail_formatOptionParameter
                inlineFormat = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_sendcsvOptionParameterContext) {
                // t_sendemail_sendcsvOptionParameter
                sendCsv = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_sendpdfOptionParameterContext) {
                // t_sendemail_sendpdfOptionParameter
                sendPdf = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_pdfviewOptionParameterContext) {
                // t_sendemail_pdfviewOptionParameter
                throw new UnsupportedOperationException("Sendemail does not support 'pdfview' parameter yet.");
            }
            else if (child instanceof DPLParser.T_sendemail_paperorientationOptionParameterContext) {
                // t_sendemail_paperorientationOptionParameter
                paperOrientation = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_papersizeOptionParameterContext) {
                // t_sendemail_papersizeOptionParameter
                paperSize = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_priorityOptionParameterContext) {
                // t_sendemail_priorityOptionParameter
                throw new UnsupportedOperationException("Sendemail does not support 'priority' parameter yet.");
            }
            else if (child instanceof DPLParser.T_sendemail_serverOptionParameterContext) {
                // t_sendemail_serverOptionParameter
                // split <host>:<port>
                // if <port> is missing, use default
                String serverString = ((StringNode) visit(child)).toString();

                LOGGER.debug("server string (should be host:port) = <[{}]>", serverString);

                String[] hostAndPort = serverString.split(":");
                // more than one item, means port must be present
                if (hostAndPort.length > 1) {
                    server = hostAndPort[0];
                    port = Integer.parseInt(hostAndPort[1]);
                }
                // One item (or less), just server
                else {
                    server = hostAndPort[0];
                }

                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_gracefulParameterContext) {
                // t_sendemail_gracefulParameter
                graceful = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_contentTypeOptionParameterContext) {
                // T_sendemail_contentTypeOptionParameter
                content_type = ((StringNode) visit(child)).toString();
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_widthSortColumnsOptionParameterContext) {
                // T_sendemail_widthSortColumnsOptionParameter
                throw new UnsupportedOperationException(
                        "Sendemail does not support 'width_sort_columns' parameter yet."
                );
                //widthSortColumns = ((StringNode) visit(child)).toString() == "true";
            }
            else if (child instanceof DPLParser.T_sendemail_useSslOptionParameterContext) {
                // T_sendemail_useSslOptionParameter
                use_ssl = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_useTlsOptionParameterContext) {
                // T_sendemail_useTlsOptionParameter
                use_tls = ((StringNode) visit(child)).toString() == "true";
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_maxinputsParameterContext) {
                // T_sendemail_maxinputsParameter
                maxInputs = Integer.parseInt(((StringNode) visit(child)).toString());
                hasForbiddenConfig = true;
            }
            else if (child instanceof DPLParser.T_sendemail_maxtimeParameterContext) {
                // t_sendemail_maxtimeParameter
                // <integer> m | s | h | d

                throw new UnsupportedOperationException("Sendemail does not support 'maxtime' parameter yet.");
                /*String maxTimeString = ((StringNode) visit(child)).toString();
                LOGGER.info("max time string= {}", maxTimeString);
                Pattern pattern = Pattern.compile("\\d+");
                Matcher matcher = pattern.matcher(maxTimeString);
                if (matcher.find()) {
                	String number = matcher.group();
                	int numberAsInt = Integer.parseInt(number);
                	String timeUnit = maxTimeString.substring(number.length());
                	LOGGER.info("max time = " + numberAsInt + " of unit " + timeUnit);
                	// TODO do something with numberAsInt and timeUnit.
                }
                else {
                	throw new RuntimeException("maxtime argument contained an invalid time argument.\nExpected: <integer> m | s | h | d\nGot: " + maxTimeString);
                }*/
            }
        }

        if (restrictedMode && hasForbiddenConfig) {
            throw new IllegalArgumentException(
                    "Forbidden configuration detected. Please make sure that only the 'to' and 'subject' parameters are used, or switch off restricted mode."
            );
        }

        if (use_tls && use_ssl) {
            throw new IllegalArgumentException(
                    "Both 'use_tls' and 'use_ssl' cannot be used simultaneously. Please enable either 'use_tls' or 'use_ssl', not both."
            );
        }

        // initialize results processor
        final SendemailResultsProcessor resultsProcessor = new SendemailResultsProcessor(
                use_tls,
                server,
                port,
                use_ssl,
                username,
                password,
                fromEmail,
                toEmails,
                ccEmails,
                bccEmails,
                subject,
                customMessageContent,
                inlineFormat,
                sendResults,
                inline,
                sendCsv,
                sendPdf,
                customFooterContent,
                paperSize,
                paperOrientation,
                content_type,
                maxInputs,
                catCtx.getUrl(),
                smtpDebug
        );

        // step
        this.sendemailStep.setSendemailResultsProcessor(resultsProcessor);
        this.sendemailStep.setSendResults(sendResults);

        return new StepNode(sendemailStep);
    }

    // COMMAND_SENDEMAIL_MODE_TO t_sendemail_emailListParameter
    @Override
    public Node visitT_sendemail_toOptionParameter(DPLParser.T_sendemail_toOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return email list
        rv = visit(ctx.getChild(1));

        return rv;
    }

    // COMMAND_SENDEMAIL_MODE_FROM ...
    @Override
    public Node visitT_sendemail_fromOptionParameter(DPLParser.T_sendemail_fromOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return email list
        rv = visit(ctx.getChild(1));

        return rv;
    }

    @Override
    public Node visitT_sendemail_ccOptionParameter(DPLParser.T_sendemail_ccOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return email list
        rv = visit(ctx.getChild(1));

        return rv;
    }

    @Override
    public Node visitT_sendemail_bccOptionParameter(DPLParser.T_sendemail_bccOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return email list
        rv = visit(ctx.getChild(1));

        return rv;
    }

    @Override
    public Node visitT_sendemail_subjectOptionParameter(DPLParser.T_sendemail_subjectOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return subject
        rv = new StringNode(
                new Token(Token.Type.STRING, new UnquotedText(new TextString(ctx.getChild(1).getText())).read())
        );

        return rv;
    }

    @Override
    public Node visitT_sendemail_messageOptionParameter(DPLParser.T_sendemail_messageOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return message
        rv = new StringNode(
                new Token(Token.Type.STRING, new UnquotedText(new TextString(ctx.getChild(1).getText())).read())
        );

        return rv;
    }

    @Override
    public Node visitT_sendemail_footerOptionParameter(DPLParser.T_sendemail_footerOptionParameterContext ctx) {
        Node rv = null;

        // skip keyword and return footer
        rv = new StringNode(
                new Token(Token.Type.STRING, new UnquotedText(new TextString(ctx.getChild(1).getText())).read())
        );

        return rv;
    }

    @Override
    public Node visitT_sendemail_inlineOptionParameter(DPLParser.T_sendemail_inlineOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    @Override
    public Node visitT_sendemail_formatOptionParameter(DPLParser.T_sendemail_formatOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode formatValue = (TerminalNode) ctx.getChild(1);//.getChild(0);
        String value = null;

        switch (formatValue.getSymbol().getType()) {
            case DPLLexer.COMMAND_SENDEMAIL_MODE_FORMAT_MODE_CSV:
                value = "csv";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_FORMAT_MODE_TABLE:
                value = "table";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_FORMAT_MODE_RAW:
                value = "raw";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_sendcsvOptionParameter(DPLParser.T_sendemail_sendcsvOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_sendpdfOptionParameter(DPLParser.T_sendemail_sendpdfOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_pdfviewOptionParameter(DPLParser.T_sendemail_pdfviewOptionParameterContext ctx) {
        Node rv = null;

        rv = new StringNode(new Token(Token.Type.STRING, ctx.getChild(1).getText()));

        return rv;
    }

    @Override
    public Node visitT_sendemail_sendresultsOptionParameter(
            DPLParser.T_sendemail_sendresultsOptionParameterContext ctx
    ) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    @Override
    public Node visitT_sendemail_paperorientationOptionParameter(
            DPLParser.T_sendemail_paperorientationOptionParameterContext ctx
    ) {
        Node rv = null;

        TerminalNode paperOrientationValue = (TerminalNode) ctx.getChild(1);
        String value = null;

        switch (paperOrientationValue.getSymbol().getType()) {
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERORIENTATION_MODE_PORTRAIT:
                value = "portrait";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERORIENTATION_MODE_LANDSCAPE:
                value = "landscape";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    @Override
    public Node visitT_sendemail_papersizeOptionParameter(DPLParser.T_sendemail_papersizeOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode paperSizeValue = (TerminalNode) ctx.getChild(1);
        String value = null;

        switch (paperSizeValue.getSymbol().getType()) {
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_A2:
                value = "a2";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_A3:
                value = "a3";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_A4:
                value = "a4";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_A5:
                value = "a5";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_LEDGER:
                value = "ledger";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_LEGAL:
                value = "legal";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PAPERSIZE_MODE_LETTER:
                value = "letter";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    @Override
    public Node visitT_sendemail_priorityOptionParameter(DPLParser.T_sendemail_priorityOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode priorityValue = (TerminalNode) ctx.getChild(1);
        String value = "";

        switch (priorityValue.getSymbol().getType()) {
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PRIORITY_MODE_LOWEST:
                value = "5";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PRIORITY_MODE_LOW:
                value = "4";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PRIORITY_MODE_NORMAL:
                value = "3";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PRIORITY_MODE_HIGH:
                value = "2";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_PRIORITY_MODE_HIGHEST:
                value = "1";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    @Override
    public Node visitT_sendemail_serverOptionParameter(DPLParser.T_sendemail_serverOptionParameterContext ctx) {
        Node rv = null;

        String server = ctx.getChild(1).getText();

        rv = new StringNode(new Token(Token.Type.STRING, server));
        return rv;
    }

    @Override
    public Node visitT_sendemail_gracefulParameter(DPLParser.T_sendemail_gracefulParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));
        return rv;
    }

    @Override
    public Node visitT_sendemail_contentTypeOptionParameter(
            DPLParser.T_sendemail_contentTypeOptionParameterContext ctx
    ) {
        Node rv = null;

        // content_type is html OR plain

        TerminalNode contentTypeValue = (TerminalNode) ctx.getChild(1);//.getChild(0);
        String value = null;

        switch (contentTypeValue.getSymbol().getType()) {
            case DPLLexer.COMMAND_SENDEMAIL_MODE_CONTENT_TYPE_MODE_HTML:
                value = "html";
                break;
            case DPLLexer.COMMAND_SENDEMAIL_MODE_CONTENT_TYPE_MODE_PLAIN:
                value = "plain";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_widthSortColumnsOptionParameter(
            DPLParser.T_sendemail_widthSortColumnsOptionParameterContext ctx
    ) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_useSslOptionParameter(DPLParser.T_sendemail_useSslOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_useTlsOptionParameter(DPLParser.T_sendemail_useTlsOptionParameterContext ctx) {
        Node rv = null;

        TerminalNode booleanValue = (TerminalNode) ctx.getChild(1).getChild(0);
        String value = null;

        switch (booleanValue.getSymbol().getType()) {
            case DPLLexer.GET_BOOLEAN_TRUE:
                value = "true";
                break;
            case DPLLexer.GET_BOOLEAN_FALSE:
                value = "false";
                break;
        }

        rv = new StringNode(new Token(Token.Type.STRING, value));

        return rv;
    }

    @Override
    public Node visitT_sendemail_maxinputsParameter(DPLParser.T_sendemail_maxinputsParameterContext ctx) {
        Node rv = null;

        rv = new StringNode(new Token(Token.Type.STRING, ctx.getChild(1).getText()));

        return rv;
    }

    @Override
    public Node visitT_sendemail_maxtimeParameter(DPLParser.T_sendemail_maxtimeParameterContext ctx) {
        Node rv = null;

        rv = new StringNode(new Token(Token.Type.STRING, ctx.getChild(1).getText()));

        return rv;
    }

    // stringType (COMMA stringType)*?
    @Override
    public Node visitT_sendemail_emailListParameter(DPLParser.T_sendemail_emailListParameterContext ctx) {
        return new StringNode(new Token(Token.Type.STRING, new UnquotedText(new TextString(ctx.getText())).read()));
    }

}
