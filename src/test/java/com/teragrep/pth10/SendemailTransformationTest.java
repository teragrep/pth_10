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
package com.teragrep.pth10;


import com.icegreen.greenmail.junit5.GreenMailExtension;
import com.icegreen.greenmail.util.ServerSetup;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


/**
 * @author eemhu
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SendemailTransformationTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(SendemailTransformationTest.class);

	private final String testFile = "src/test/resources/sendemailTransformationTest_data*.json"; // * to make the path into a directory path
	private final StructType testSchema = new StructType(
			new StructField[] {
					new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
					new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
					new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
					new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
			}
	);

	private StreamingTestUtil streamingTestUtil;

	@org.junit.jupiter.api.BeforeAll
	void setEnv() throws IOException {
		this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
		this.streamingTestUtil.setEnv();
	}
	
	@org.junit.jupiter.api.BeforeEach
	void setUp() {
		this.streamingTestUtil.setUp();

		DPLParserCatalystContext ctx = this.streamingTestUtil.getCtx();
		DPLParserCatalystVisitor visitor = this.streamingTestUtil.getCatalystVisitor();

		// set path for join cmd
		visitor.setHdfsPath("/tmp/pth_10/" + UUID.randomUUID());

		// set paragraph url
		ctx.setBaseUrl("http://teragrep.test");
		ctx.setNotebookUrl("NoteBookID");
		ctx.setParagraphUrl("ParaGraphID");

		greenMail.start();
	}
	
	@org.junit.jupiter.api.AfterEach
	void tearDown() {
		greenMail.stop();
		this.streamingTestUtil.tearDown();
	}

	@RegisterExtension
	static GreenMailExtension greenMail = new GreenMailExtension(new ServerSetup(2525, "localhost", "smtp"));
	
	
	// ----------------------------------------
	// Tests
	// ----------------------------------------
		
	// basic email without results, no aggregations
	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	public void sendemail_test_1() {
		// Perform DPL query with streaming data
		streamingTestUtil.performDPLTest(
				"index=index_A | sendemail to=exa@mple.test from=from@example.test cc=cc@example.test server=localhost:2525",
				testFile,
				ds -> {
					assertEquals("[_time, id, _raw, index, sourcetype, host, source, partition, offset]", Arrays.toString(ds.columns()),
							"Batch handler dataset contained an unexpected column arrangement !");
				}
				);

		// Get message
		MimeMessage msg = greenMail.getReceivedMessages()[0];
		String msgStr = assertDoesNotThrow(() -> msgToString(msg));

		// Get toEmails and subject.
		String[] toEmails = assertDoesNotThrow(() -> msg.getHeader("to"));
		String subject = assertDoesNotThrow(() -> msg.getHeader("subject")[0]);
		String cc = assertDoesNotThrow(() -> msg.getHeader("cc")[0]);
		String from = assertDoesNotThrow(() -> msg.getHeader("from")[0]);

		// Assertions
		assertTrue(msgStr.contains("Search complete."));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("cc@example.test", cc);
		assertEquals("from@example.test", from);
		assertEquals("Teragrep Results", subject);
	}
	
	// basic email with two preceding eval commands
	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	public void sendemail_test_2() {
		// Perform DPL query with streaming data
		streamingTestUtil.performDPLTest(
				"index=index_A | eval extraField=null() | eval oneMoreField=true() | sendemail to=\"exa@mple.test\" server=localhost:2525",
				testFile,
				ds -> {
					assertEquals("[_time, id, _raw, index, sourcetype, host, source, partition, offset, extraField, oneMoreField]",
							Arrays.toString(ds.columns()),
							"Batch handler dataset contained an unexpected column arrangement !");
				}
				);
		
		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = assertDoesNotThrow(() -> msgToString(msg));
		
		// Get toEmails and subject.
		String[] toEmails = assertDoesNotThrow(() -> msg.getHeader("to"));
		String subject = assertDoesNotThrow(() -> msg.getHeader("subject")[0]);
		
		// Assertions
		assertTrue(msgStr.contains("Search complete."));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Teragrep Results", subject);
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	public void sendemail_test_3() {
		// Perform DPL query with streaming data
		streamingTestUtil.performDPLTest(
				"index=index_A | chart avg(offset) as avgo | chart avg(avgo) as resultssss | sendemail to=\"exa@mple.test\" sendresults=true inline=true sendpdf=true format=csv server=localhost:2525 ",
				testFile,
				ds -> {

				}
				);

		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = assertDoesNotThrow(() -> msgToString(msg));

		// Get toEmails and subject.
		String[] toEmails = assertDoesNotThrow(() -> msg.getHeader("to"));
		String subject = assertDoesNotThrow(() -> msg.getHeader("subject")[0]);

		// Assertions
		assertTrue(msgStr.contains("Search results."));

		// if message contains the column headers like this it will contain the csv too
		assertTrue(msgStr.contains("result"));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Teragrep Results", subject);
	}

	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	public void sendemail_test_4() {
		// Perform DPL query with streaming data
		streamingTestUtil.performDPLTest(
				"index=index_A | sendemail to=\"exa@mple.test\" subject=\"Custom subject\" sendresults=true inline=true format=csv server=localhost:2525",
				testFile,
				ds -> {
					assertEquals("[_time, id, _raw, index, sourcetype, host, source, partition, offset]",
							Arrays.toString(ds.columns()),
							"Batch handler dataset contained an unexpected column arrangement !");
				}
				);
		
		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = assertDoesNotThrow(() -> msgToString(msg));
		
		// Get toEmails and subject.;
		String[] toEmails = assertDoesNotThrow(() -> msg.getHeader("to"));
		String subject = assertDoesNotThrow(() -> msg.getHeader("subject")[0]);

		// Assertions
		assertTrue(msgStr.contains("Search results."));
		
		// if message contains the column headers like this it will contain the csv too
		assertTrue(msgStr.contains("_time,id,_raw,index,sourcetype,host,source,partition,offset"));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Custom subject", subject);
	}
	
	// pipe where after stats, then send email
	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	public void sendemail_test_5() {
		// Perform DPL query with streaming data
		streamingTestUtil.performDPLTest(
				"index=index_A | stats avg(offset) as avgo count(offset) as co | where co > 1 | sendemail to=\"exa@mple.test\" server=localhost:2525",
				testFile,
				ds -> {
					assertEquals("[avgo, co]", Arrays.toString(ds.columns()),
							"Batch handler dataset contained an unexpected column arrangement !");
				}
				);

		// Get message
		MimeMessage msg = greenMail.getReceivedMessagesForDomain("exa@mple.test")[0];
		String msgStr = assertDoesNotThrow(() -> msgToString(msg));
		
		// Get toEmails and subject.
		String[] toEmails = assertDoesNotThrow(() -> msg.getHeader("to"));
		String subject = assertDoesNotThrow(() -> msg.getHeader("subject")[0]);
		
		// Assertions
		assertTrue(msgStr.contains("Search complete."));
		assertEquals(1, toEmails.length);
		assertEquals("exa@mple.test", toEmails[0]);
		assertEquals("Teragrep Results", subject);
	}

	// empty resultset must not send email
	@Test
	@DisabledIfSystemProperty(named="skipSparkTest", matches="true")
	public void sendemailTestEmptyResultset() {
		// Perform DPL query with streaming data
		streamingTestUtil.performDPLTest(
				"index=index_A" +
						"|chart count(_raw) as craw" +
						"|where craw < 0 " + // filter out all
						"|sendemail to=\"1@example.com\" server=localhost:2525",
				testFile,
				ds -> {
					// returns empty dataframe, but has column names present
					assertEquals("[craw]", Arrays.toString(ds.columns()),
							"Batch handler dataset contained an unexpected column arrangement !");
				}
		);

		// must not send any message
		assertEquals(0, greenMail.getReceivedMessagesForDomain("1@example.com").length);
	}
	
	// ----------------------------------------
	// Helper methods
	// ----------------------------------------

	private String msgToString(MimeMessage mimeMsg) throws MessagingException {
		String text = new BufferedReader(
				new InputStreamReader(mimeMsg.getRawInputStream(), StandardCharsets.UTF_8))
				.lines()
				.collect(Collectors.joining("\n"));

		return text;
	}
	
}