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
package com.teragrep.pth10.ast.commands.transformstatement.sendemail;

import be.quodlibet.boxable.BaseTable;
import be.quodlibet.boxable.datatable.DataTable;
import com.google.common.annotations.VisibleForTesting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * A class that processes a list of rows into an email. Used for the sendemail command.
 */
public class SendemailResultsProcessor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendemailResultsProcessor.class);

    private static final long serialVersionUID = 1L;
    private static List<Row> listOfRows = new ArrayList<>();
    private static int maxCount = 50000;
    private static int count = 0;

    // from constructor
    private boolean use_tls = false;
    private String server = "localhost";
    private int port = 25;
    private boolean use_ssl = false;
    //DPLParserCatalystContext catCtx = null;
    private String fromEmail = null;
    private String toEmails = null;
    private String ccEmails = null;
    private String bccEmails = null;
    private String subject = null;
    private String customMessageContent = null;

    private String format = "csv";
    private boolean sendResults = false;
    private boolean inline = false;
    private boolean sendCsv = false;
    private boolean sendPdf = false;
    private String customFooterContent = null;
    private String paperSize = null;
    private String paperOrientation = null;
    private String content_type = null;

    private static PDPage page1 = new PDPage();
    private static PDDocument pdfDoc = new PDDocument();
    private static MimeBodyPart dataBodyPart = new MimeBodyPart();
    private static MimeBodyPart attachmentBodyPart = new MimeBodyPart();

    private static String username = "";
    private static String password = "";

    private String urlToParagraph = null;

    private boolean smtpDebug = false; // enable debugging for email sending

    // Was the email processor called already? (make sure that email is only sent once if sendresults=false)
    private boolean isCalledBefore = false;

    /**
     * Construct the SendemailResultsProcessor with given parameters.
     * 
     * @param use_tls              Use TLS or not
     * @param server               server host name (e.g. localhost)
     * @param port                 server port number (e.g. 25)
     * @param use_ssl              Use SSL or not
     * @param username             SMTP username
     * @param password             SMTP password
     * @param fromEmail            Send from email
     * @param toEmails             Send to emails (separate with comma)
     * @param ccEmails             cc emails (separate with comma)
     * @param bccEmails            bcc emails (separate with comma)
     * @param subject              custom subject if needed
     * @param customMessageContent custom message if needed
     * @param format               format of inline results (csv, raw, table)
     * @param sendResults          whether or not include results in email
     * @param inline               whether or not include inline results in email
     * @param sendCsv              send csv attachment?
     * @param sendPdf              send pdf attachment?
     * @param customFooterContent  custom footer if needed
     * @param paperSize            custom paperSize if needed (e.g. "a4")
     * @param paperOrientation     custom paper orientation (e.g. "landscape" or "portrait")
     * @param content_type         "plain" text or "html"
     * @param maxInputs            maximum batch size to send at a time
     * @param smtpDebug            enable additional SMTP debug logging
     * @param urlToParagraph       URL address to paragraph containing the results
     */
    public SendemailResultsProcessor(
            boolean use_tls,
            String server,
            int port,
            boolean use_ssl,
            String username,
            String password,
            String fromEmail,
            String toEmails,
            String ccEmails,
            String bccEmails,
            String subject,
            String customMessageContent,
            String format,
            boolean sendResults,
            boolean inline,
            boolean sendCsv,
            boolean sendPdf,
            String customFooterContent,
            String paperSize,
            String paperOrientation,
            String content_type,
            int maxInputs,
            String urlToParagraph,
            boolean smtpDebug
    ) {
        super();

        this.use_tls = use_tls;
        this.server = server;
        this.port = port;
        this.use_ssl = use_ssl;

        SendemailResultsProcessor.username = username;
        SendemailResultsProcessor.password = password;

        this.fromEmail = fromEmail;
        this.toEmails = toEmails;
        this.ccEmails = ccEmails;
        this.bccEmails = bccEmails;
        this.subject = subject;
        this.customMessageContent = customMessageContent;

        this.format = format;
        this.sendResults = sendResults;
        this.inline = inline;
        this.sendCsv = sendCsv;
        this.sendPdf = sendPdf;
        this.customFooterContent = customFooterContent;
        this.paperSize = paperSize;
        this.paperOrientation = paperOrientation;
        this.content_type = content_type;

        SendemailResultsProcessor.maxCount = maxInputs; // set to other than 50k

        this.urlToParagraph = urlToParagraph;
        this.smtpDebug = smtpDebug;
    }

    /**
     * Used for testing translation; not to be used outside of testing and/or debugging. Returns all parameters in a Map
     * 
     * @return map of parameters, where key= variable name, value= the value of that variable
     */
    @VisibleForTesting
    public Map<String, String> getParameters() {
        Map<String, String> params = new HashMap<>();

        params.put("use_tls", String.valueOf(use_tls));
        params.put("server", server);
        params.put("port", String.valueOf(port));
        params.put("use_ssl", String.valueOf(use_ssl));
        params.put("username", username);
        params.put("password", password != null ? "***" : "NULL");
        params.put("fromEmail", fromEmail);
        params.put("toEmails", toEmails);
        params.put("bccEmails", bccEmails);
        params.put("ccEmails", ccEmails);
        params.put("subject", subject);
        params.put("customMessageContent", customMessageContent);
        params.put("format", format);
        params.put("sendResults", String.valueOf(sendResults));
        params.put("inline", String.valueOf(inline));
        params.put("sendCsv", String.valueOf(sendCsv));
        params.put("sendPdf", String.valueOf(sendPdf));
        params.put("customFooterContent", customFooterContent);
        params.put("paperSize", paperSize);
        params.put("paperOrientation", paperOrientation);
        params.put("content_type", content_type);
        params.put("maxinputs", String.valueOf(maxCount));
        params.put("urlToParagraph", urlToParagraph);
        params.put("smtpDebug", String.valueOf(smtpDebug));

        return params;
    }

    /**
     * Has the call() function of this object been used even once?
     * 
     * @return true or false
     */
    public boolean getIsCalledBefore() {
        return this.isCalledBefore;
    }

    /**
     * Builds email on maxInputs batches using a list of rows given
     * 
     * @param rows List of rows
     * @throws Exception Any exception that occurred during building and sending the email
     */
    public void call(List<Row> rows) throws Exception {
        this.isCalledBefore = true;
        // Size of list of rows (collected dataframe)
        int size = rows.size();
        // rows needed for current maxInputs email batch
        int remainingToBeAdded = maxCount - count;

        // none remaining, build email
        if (remainingToBeAdded == 0) {
            buildEmail(listOfRows);
            listOfRows.clear();
            count = 0;
        }
        // Needs more than currently in list of rows
        else if (remainingToBeAdded > size) {
            listOfRows.addAll(rows);
            count += size;
        }
        // Current email batch needs less than in list of rows,
        // call this function again with surplus rows
        else { // remaining != 0, remaining < size
            listOfRows.addAll(rows.subList(0, remainingToBeAdded));
            buildEmail(listOfRows);
            listOfRows.clear();
            count = 0;

            List<Row> toAdd = rows.subList(remainingToBeAdded, rows.size());
            this.call(toAdd);
        }
    }

    /**
     * Send email without rows (sendresults=false)
     * 
     * @throws Exception Any exception that occurred during building and sending the email
     */
    public void call() throws Exception {
        this.isCalledBefore = true;
        buildEmail(null);
    }

    /**
     * flushes the remaining rows that were not processed as they did not reach the target count
     * 
     * @throws Exception Any error that occurred during the flush()
     */
    public void flush() throws Exception {
        LOGGER.info("Flushing email processor!");
        if (listOfRows != null && listOfRows.size() > 0) {
            buildEmail(listOfRows);
            listOfRows.clear();
            count = 0;
        }
    }

    /**
     * Builds the email from current content of listOfRows
     * 
     * @throws MessagingException
     */
    private void buildEmail(List<Row> listOfRows) throws MessagingException {
        LOGGER.info("Building email!");

        // Properties instance used for setting up email parameters
        Properties emailProp = new Properties();
        emailProp.put("mail.smtp.auth", use_ssl || use_tls); // use auth if either ssl or tls is used
        emailProp.put("mail.smtp.starttls.enable", use_tls);
        emailProp.put("mail.smtp.host", server);
        emailProp.put("mail.smtp.port", port);
        emailProp.put("mail.smtp.ssl.enable", use_ssl);

        LOGGER.info("Sendemail properties: <{}>", emailProp.entrySet());

        // user and pass from zeppelin config
        final Session session = Session.getInstance(emailProp, new Authenticator() {

            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        // Set debugging, if given in zeppelin config
        session.setDebug(this.smtpDebug);

        // Start building the email message
        try {
            final Message message = new MimeMessage(session);

            // from, to, cc and bcc emails
            if (fromEmail != null) {
                message.setFrom(new InternetAddress(fromEmail));
            }

            if (toEmails != null) {
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmails));
            }
            else {
                throw new IllegalArgumentException(
                        "Sendemail command did not contain a required parameter: 'to=<email_list>' !"
                );
            }

            if (ccEmails != null) {
                message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(ccEmails));
            }

            if (bccEmails != null) {
                message.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(bccEmails));
            }

            // set subject if given in command, otherwise defaults to "Teragrep Results"
            message.setSubject(subject != null ? subject : "Teragrep Results");

            // line break based on content_type
            String lineBreak = "\n";
            if (this.content_type.equals("html")) {
                lineBreak = "<br>";
            }

            // Message depends on given parameters
            // sendResults=false -> Search complete
            // sendResults=true, inline=true, and no attachments -> Search results.
            // sendResults=true with attachments -> Search results attached.
            // 
            // Apply custom message if it was given in the command
            String messageContent = "";
            if (customMessageContent == null) {
                if (!sendResults) {
                    messageContent = "Search complete.";
                }
                else if (sendResults && inline && (!sendPdf || !sendCsv)) {
                    messageContent = "Search results.";
                }
                else if (sendResults && (sendPdf || sendCsv)) {
                    messageContent = "Search results attached.";
                }
            }
            else {
                messageContent = customMessageContent;
            }

            // Add url to paragraph
            if (this.urlToParagraph != null) {
                if (this.content_type.equals("html")) {
                    messageContent += String
                            .format("<br><a href=\"%s\">View results in Teragrep</a>", this.urlToParagraph);
                }
                else {
                    // plain
                    messageContent += String.format("\n%s", this.urlToParagraph);
                }

            }

            // Footer (again, custom footer will be applied if it was given in the command)
            MimeBodyPart footerBodyPart = new MimeBodyPart();
            String footerContent = "This email was generated via the sendemail command. Not the correct recipient? Contact your Teragrep administrator."
                    + lineBreak + "Teragrep - Know Everything";
            if (customFooterContent != null) {
                footerContent = customFooterContent;
            }
            footerBodyPart
                    .setContent(footerContent, content_type.equals("html") ? "text/html; charset=utf-8" : "text/plain; charset=utf-8");

            // Full email will be assembled to MimeMultipart
            final Multipart multipart = new MimeMultipart();

            // Set messageContent to MimeBodyPart
            MimeBodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart
                    .setContent(messageContent, content_type.equals("html") ? "text/html; charset=utf-8" : "text/plain; charset=utf-8");

            // Add message bodypart to MimeMultipart
            multipart.addBodyPart(messageBodyPart);

            // pdf paper settings
            // Object hierarchy for pdf document
            // pdfDoc (main document) <- page1 (page) <- table (outside table) <- t (internal data)
            pdfDoc = new PDDocument();
            page1 = null;

            // set paper size
            float paperWidth = 0f, paperHeight = 0f;

            switch (paperSize) {
                case "legal":
                    paperWidth = PDRectangle.LEGAL.getWidth();
                    paperHeight = PDRectangle.LEGAL.getHeight();
                    break;
                case "a2":
                    paperWidth = PDRectangle.A2.getWidth();
                    paperHeight = PDRectangle.A2.getHeight();
                    break;
                case "a3":
                case "ledger":
                    // ledger is equivalent to A3
                    paperWidth = PDRectangle.A3.getWidth();
                    paperHeight = PDRectangle.A3.getHeight();
                    break;
                case "a4":
                    paperWidth = PDRectangle.A4.getWidth();
                    paperHeight = PDRectangle.A4.getHeight();
                    break;
                case "a5":
                    paperWidth = PDRectangle.A5.getWidth();
                    paperHeight = PDRectangle.A5.getHeight();
                    break;
                case "letter":
                default:
                    // letter is default
                    paperWidth = PDRectangle.LETTER.getWidth();
                    paperHeight = PDRectangle.LETTER.getHeight();
                    break;
            }

            // Create page with set paper orientation and size
            if (paperOrientation.equals("landscape")) {
                page1 = new PDPage(new PDRectangle(paperHeight, paperWidth));
            }
            else {
                // default (portrait)
                page1 = new PDPage(new PDRectangle(paperWidth, paperHeight));
            }

            String dataContent = null;

            // Get search results if sendResults = true
            dataBodyPart = new MimeBodyPart();

            // DatasetToTextBuilder init
            DatasetToTextBuilder txtBuilder = new DatasetToTextBuilder(format, lineBreak);
            if (sendResults && inline) {
                dataContent = txtBuilder.build(listOfRows);
                // set dataContent to dataBodyPart
                dataBodyPart
                        .setContent(dataContent, content_type.equals("html") ? "text/html; charset=utf-8" : "text/plain; charset=utf-8");
            }

            // sendCsv and sendPdf
            if (sendResults && sendCsv) {
                if (!inline || format != "csv" || !lineBreak.equals("\n")) {
                    // dataContent is something other than csv
                    // re-generate csv version
                    txtBuilder = new DatasetToTextBuilder("csv", "\n");
                    dataContent = txtBuilder.build(listOfRows);
                }

                byte[] fileBase64ByteArray;
                try {
                    fileBase64ByteArray = java.util.Base64.getEncoder().encode(dataContent.getBytes("UTF-8"));
                    // headers for attachment
                    InternetHeaders fileHeaders = new InternetHeaders();
                    fileHeaders.setHeader("Content-Type", "text/csv; name=\"results.csv\"");
                    fileHeaders.setHeader("Content-Transfer-Encoding", "base64");
                    fileHeaders.setHeader("Content-Disposition", "attachment; filename=\"results.csv\"");
                    // body part for attachment
                    attachmentBodyPart = new MimeBodyPart(fileHeaders, fileBase64ByteArray);
                    attachmentBodyPart.setFileName("results.csv");

                    multipart.addBodyPart(attachmentBodyPart); // attachment

                }
                catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
            else if (sendResults && sendPdf) {
                if (!inline || format != "csv" || !lineBreak.equals("\n")) {
                    // dataContent is something other than csv
                    // re-generate csv version
                    // csv is used to generate table to pdf
                    txtBuilder = new DatasetToTextBuilder("csv", "\n");
                    dataContent = txtBuilder.build(listOfRows);
                }

                // Generate content
                try {
                    // add page to main document and initialize contentStream
                    pdfDoc.addPage(page1);
                    PDPageContentStream contentStream = new PDPageContentStream(pdfDoc, page1);

                    // Setup contentStream for text
                    contentStream.beginText();
                    contentStream.setFont(PDType1Font.TIMES_ROMAN, 16);
                    contentStream.setLeading(14.5f);

                    // table settings
                    float margin = 50;
                    float yStartNewPage = page1.getMediaBox().getHeight() - (2 * margin);
                    float tableWidth = page1.getMediaBox().getWidth() - (2 * margin);
                    boolean drawContent = true;
                    float yStart = yStartNewPage;
                    float bottomMargin = 70;
                    float yPosition = page1.getMediaBox().getHeight() - 100;

                    // Generate BaseTable and DataTable
                    BaseTable table = new BaseTable(
                            yPosition,
                            yStartNewPage,
                            bottomMargin,
                            tableWidth,
                            margin,
                            pdfDoc,
                            page1,
                            true,
                            drawContent
                    );
                    DataTable t = new DataTable(table, page1);
                    t.addCsvToTable(dataContent, DataTable.HASHEADER, ',');

                    // write text to contentStream (title)
                    contentStream.newLineAtOffset(25, page1.getMediaBox().getHeight() - 50);
                    contentStream.showText("Teragrep Results");
                    contentStream.endText();

                    // draw table and close stream
                    table.draw();
                    contentStream.close();

                    // output pdf to stream
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

                    //pdfDoc.save(new File("/tmp/pth10_" + UUID.randomUUID() + ".pdf")); // comment out - file system save for debugging
                    pdfDoc.save(outputStream);
                    pdfDoc.close();

                    // stream to base64 and attach to email with given headers
                    byte[] fileBase64ByteArray = java.util.Base64.getEncoder().encode(outputStream.toByteArray());

                    // headers for attachment
                    InternetHeaders fileHeaders = new InternetHeaders();
                    fileHeaders.setHeader("Content-Type", "application/pdf; name=\"results.pdf\"");
                    fileHeaders.setHeader("Content-Transfer-Encoding", "base64");
                    fileHeaders.setHeader("Content-Disposition", "attachment; filename=\"results.pdf\"");

                    // body part for attachment
                    MimeBodyPart attachmentBodyPart = new MimeBodyPart(fileHeaders, fileBase64ByteArray);
                    attachmentBodyPart.setFileName("results.pdf");

                    multipart.addBodyPart(attachmentBodyPart); // attachment

                }
                catch (IOException e) {
                    LOGGER.error("sendPdf IOException: <{}>", e.getMessage());
                    e.printStackTrace();
                }

            }

            // add inline results to multipart if inline=true
            if (sendResults && inline) {
                if (dataBodyPart != null) {
                    multipart.addBodyPart(dataBodyPart);
                }
            }

            // Set multipart as the content for message and send
            multipart.addBodyPart(footerBodyPart); // add footer
            message.setContent(multipart);
            Transport.send(message);

        }
        catch (MessagingException me) {
            LOGGER.error("An error occurred trying to send email using the sendemail command. Details:");
            me.printStackTrace();

            // FIXME: Implement: Throw an exception if not in graceful mode
            //if (!graceful) {
            throw new RuntimeException("Error sending email using sendemail command! Details: " + me.getMessage());
            //}
        }
    }

}
