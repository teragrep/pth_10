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

import com.teragrep.pth10.ast.time.RelativeTimeParser;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests time-related aspects of the project,
 * such as TimeStatement.
 * Uses streaming datasets
 *
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EarliestLatestTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarliestLatestTest.class);

    // Use this file for dataset initialization
    private final String testFile = "src/test/resources/earliestLatestTest_data*.json"; // * to make the path into a directory path
    private final String epochTestFile = "src/test/resources/earliestLatestTest_epoch_data*.json";
    private StreamingTestUtil streamingTestUtil;

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }


    // ----------------------------------------
    // Tests
    // ----------------------------------------

    // FIXME: earliest-latest defaulting removed and default tests set to Disabled to fix issue #351

	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest1() {
        String query = "index=strawberry earliest=-10y OR index=seagull";
        this.streamingTestUtil.performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"),
                res -> {
                    List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("strawberry"));
                    assertFalse(indexAsList.contains("seagull"));
                });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest2( ) {
        String query = "index=strawberry OR index=seagull | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertTrue(indexAsList.contains("strawberry"));
            assertTrue(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest3( ) {
        String query = "index=strawberry OR index=seagull earliest=-10y | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertTrue(indexAsList.contains("strawberry"));
            assertTrue(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest4( ) {
        String query = "earliest=-10y index=strawberry OR index=seagull | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertTrue(indexAsList.contains("strawberry"));
            assertTrue(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest5( ) {
        String query = "earliest=-10y index=strawberry OR (index=seagull latest=-10y) | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertTrue(indexAsList.contains("strawberry"));
            assertFalse(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest6( ) {
        String query = "earliest=-10y index=strawberry OR index=seagull latest=-10y | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertFalse(indexAsList.contains("strawberry"));
            assertFalse(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest7( ) {
        String query = "earliest=-10y index=strawberry OR index=seagull latest=-1y | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertTrue(indexAsList.contains("strawberry"));
            assertTrue(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest8( ) {
        String query = "earliest=-20y index=strawberry OR index=seagull earliest=-1d | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertFalse(indexAsList.contains("strawberry"));
            assertFalse(indexAsList.contains("seagull"));
        });
    }

    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest9( ) {
        String query = "earliest=-20y index=strawberry OR index=seagull earliest=now | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertFalse(indexAsList.contains("strawberry"));
            assertFalse(indexAsList.contains("seagull"));
        });
    }
    
	@Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void earliestLatestTest10( ) {
        String query = "earliest=-20y index=strawberry OR index=seagull latest=now | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile,setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertTrue(indexAsList.contains("strawberry"));
            assertTrue(indexAsList.contains("seagull"));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultFormatTest() { // MM/dd/yyyy:HH:mm:ss 2013-07-15 10:01:50
        String query = "(index=strawberry OR index=seagull) AND earliest=03/15/2014:00:00:00";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertEquals("2014-04-15 08:23:17", time.get(0));
            assertEquals("2014-03-15 21:54:14", time.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultFormatInvalidInputTest() { // MM/dd/yyyy:HH:mm:ss 2013-07-15 10:01:50
        String query = "(index=strawberry OR index=seagull) AND earliest=31/31/2014:00:00:00";
        RuntimeException sqe = this.streamingTestUtil.performThrowingDPLTest(RuntimeException.class, query, this.testFile, res -> {});
        assertEquals("TimeQualifier conversion error: <31/31/2014:00:00:00> can't be parsed.", sqe.getMessage());
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void ISO8601ZonedFormatTest() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry OR index=seagull) AND earliest=2014-03-15T00:00:00+03:00";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertEquals("2014-04-15 08:23:17", time.get(0));
            assertEquals("2014-03-15 21:54:14", time.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void ISO8601WithoutZoneFormatTest() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry OR index=seagull) AND earliest=2014-03-15T00:00:00";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertEquals("2014-04-15 08:23:17", time.get(0));
            assertEquals("2014-03-15 21:54:14", time.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void OverflowTest( ) {
        String query = "index=strawberry latest=-3644444444444444d";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());
            assertTrue(time.isEmpty());
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void OverflowTest2( ) {
        String query = "index=strawberry earliest=-1000y@y latest=+3644444444444444d";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            String maxTime = res.agg(functions.max("_time")).first().getString(0);
            String minTime = res.agg(functions.min("_time")).first().getString(0);

            assertEquals("2014-03-15 21:54:14", maxTime);
            assertEquals("2013-07-15 10:01:50", minTime);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void TimeformatTest1( ) {
        String query = "index=strawberry timeformat=%s earliest=0";
        this.streamingTestUtil.performDPLTest(query, this.epochTestFile, res -> {
            // epoch test data contains values from 1970-01-01 till 2050-03-15
            String maxTime = res.agg(functions.max("_time")).first().getString(0);
            String minTime = res.agg(functions.min("_time")).first().getString(0);

            assertEquals("2050-03-15 21:54:14", maxTime);
            assertEquals("1970-01-01 00:00:00", minTime);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void TimeformatTest2( ) {
        String query = "index=strawberry timeformat=\"%Y-%m-%d-%H-%M-%S\" earliest=2030-01-01-00-00-00 latest=2040-01-01-00-00-00";
        this.streamingTestUtil.performDPLTest(query, this.epochTestFile, res -> {
            // epoch test data contains values from 1970-01-01 till 2050-03-15
            String maxTime = res.agg(functions.max("_time")).first().getString(0);
            String minTime = res.agg(functions.min("_time")).first().getString(0);

            assertEquals("2030-01-14 00:56:08", maxTime);
            assertEquals("2030-01-14 00:56:08", minTime);
        });
    }

    @Test
    public void RelativeTimestampSecondsTest() {
        // Initial values
        // Various possible unit strings
        final List<String> units = Arrays.asList("s", "sec", "secs", "second", "seconds");

        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        final Instant i = ts.toInstant();

        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = i.plus(amount, ChronoUnit.SECONDS).getEpochSecond();

        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100sec etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampMinutesTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        // Various possible unit strings
        final List<String> units = Arrays.asList("m", "min", "minute", "minutes");
        final Instant i = ts.toInstant();

        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = i.plus(amount, ChronoUnit.MINUTES).getEpochSecond();


        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100min etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampHoursTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        // Various possible unit strings
        final List<String> units = Arrays.asList("h", "hr", "hrs", "hour", "hours");
        final Instant i = ts.toInstant();

        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = i.plus(amount, ChronoUnit.HOURS).getEpochSecond();


        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100hour etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampDaysTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        // Various possible unit strings
        final List<String> units = Arrays.asList("d", "day", "days");
        final Instant i = ts.toInstant();

        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = i.plus(amount, ChronoUnit.DAYS).getEpochSecond();


        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampWeeksTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        // Various possible unit strings
        final List<String> units = Arrays.asList("w", "week", "weeks");
        final LocalDateTime now = ts.toLocalDateTime();

        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = now.plusWeeks(amount).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();


        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100min etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampMonthsTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        // Various possible unit strings
        final List<String> units = Arrays.asList("mon", "month", "months");
        final LocalDateTime now = ts.toLocalDateTime();

        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = now.plusMonths(amount).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();

        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100min etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampYearsTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        // Various possible unit strings
        final List<String> units = Arrays.asList("y", "yr", "yrs", "year", "years");
        final LocalDateTime now = ts.toLocalDateTime();

        // Amount to add
        final long amount = 100;
        // Expected result
        final long expected = now.plusYears(amount).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();


        RelativeTimeParser rtParser = new RelativeTimeParser();
        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100min etc.
            assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
        });
    }

    @Test
    public void RelativeTimestampOver9999YearTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        final String relativeTimestamp = "+10000y";

        // Expected result
        final long expected = Timestamp.valueOf("9999-10-10 15:15:30.00").toInstant().getEpochSecond();

        RelativeTimeParser rtParser = new RelativeTimeParser();
        assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
    }

    @Test
    public void RelativeTimestampLessThan1000YearTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");
        final String relativeTimestamp = "-1100y";
        final LocalDateTime now = ts.toLocalDateTime();

        // Expected result
        final long expected = now.minusYears(1010).atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();

        RelativeTimeParser rtParser = new RelativeTimeParser();
        assertEquals(expected, rtParser.parse(relativeTimestamp).calculate(ts));
    }

    @Test
    public void RelativeTimestampInvalidUnitTest() {
        // Initial values
        final String relativeTimestamp = "xyz";

        RelativeTimeParser rtParser = new RelativeTimeParser();
        assertThrows(RuntimeException.class, () -> {
            rtParser.parse(relativeTimestamp);
        }, "Relative timestamp contained an invalid time unit");
    }

    @Test
    public void RelativeTimestampOverflowPositiveTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");

        // Amount to add
        final long v = Long.MAX_VALUE;
        final long expected = Instant.ofEpochMilli(v).atZone(ZoneId.systemDefault()).withYear(9999).toInstant().getEpochSecond();

        // positive overflow epoch should be long max value
        RelativeTimeParser rtParser = new RelativeTimeParser();
        long result = rtParser.parse("+" + v + "h").calculate(ts);
        assertEquals(expected, result);
    }

    @Test
    public void RelativeTimestampOverflowNegativeTest() {
        // Initial values
        final Timestamp ts = Timestamp.valueOf("2010-10-10 15:15:30.00");

        // Amount to add
        final long v = Long.MIN_VALUE;

        // negative overflow epoch should be epoch=0
        RelativeTimeParser rtParser = new RelativeTimeParser();
        long result = rtParser.parse(v + "h").calculate(ts);
        assertEquals(0, result);
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest1( ) { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry OR index=seagull)";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(-1L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^strawberry AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^seagull AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        assertEquals(expSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest2() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry earliest=2014-03-15T21:54:14+02:00) OR index=seagull)";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(-1L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1394913254\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1394913254, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^seagull AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        assertEquals(expSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest3() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry earliest=2014-03-15T21:54:14+02:00) OR (index=seagull earliest=2014-04-15T08:23:17+02:00))";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1678713103L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

            assertEquals(2, time.size());
            assertEquals("2014-04-15 08:23:17", time.get(0));
            assertEquals("2014-03-15 21:54:14", time.get(1));
        });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1678713103\"/></AND><earliest operation=\"GE\" value=\"1394913254\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><latest operation=\"LE\" value=\"1678713103\"/></AND><earliest operation=\"GE\" value=\"1397539397\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1678713103, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1394913254, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^seagull AND (_time <= from_unixtime(1678713103, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1397539397, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        assertEquals(expSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest4( ) {
        String query = "(earliest=1900-01-01T00:00:00Z latest=1960-01-01T00:00:00Z) OR ((index=seagull earliest=1970-01-01T00:00:00Z latest=2100-01-01T00:00:00Z)) OR index=strawberry earliest=1950-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1678711652L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedXml = "<AND>" +
                "<OR>" +
                "<OR>" +
                "<AND>" +
                "<earliest operation=\"GE\" value=\"-2208994789\"/>" +
                "<latest operation=\"LE\" value=\"-315626400\"/>" +
                "</AND>" +
                "<AND>" +
                "<AND>" +
                "<comparisonstatement field=\"index\" operation=\"=\" value=\"seagull\"/>" +
                "<earliest operation=\"GE\" value=\"-7200\"/>" +
                "</AND>" +
                "<latest operation=\"LE\" value=\"4102437600\"/>" +
                "</AND>" +
                "</OR>" +
                "<AND>" +
                "<comparisonstatement field=\"index\" operation=\"=\" value=\"strawberry\"/>" +
                "<latest operation=\"LE\" value=\"1678711652\"/>" +
                "</AND>" +
                "</OR>" +
                "<earliest operation=\"GE\" value=\"-631159200\"/>" +
                "</AND>";
        final String expectedSpark = "(((((_time >= from_unixtime(-2208994789, yyyy-MM-dd HH:mm:ss)) AND (_time <= from_unixtime(-315626400, yyyy-MM-dd HH:mm:ss)))" +
                " OR ((index RLIKE ^seagull$ AND (_time >= from_unixtime(-7200, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(4102437600, yyyy-MM-dd HH:mm:ss))))" +
                " OR (index RLIKE ^strawberry$ AND (_time <= from_unixtime(1678711652, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(-631159200, yyyy-MM-dd HH:mm:ss)))";

        assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest5( ) {
        String query = "index=strawberry OR index=seagull earliest=2020-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedXml = "<AND><OR><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND></OR><earliest operation=\"GE\" value=\"1577829600\"/></AND>";
        final String expectedSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) OR (index RLIKE (?i)^seagull AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(1577829600, yyyy-MM-dd HH:mm:ss)))";

        assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest6( ) {
        String query = "index=seagull earliest=1970-01-01T00:00:00.000+02:00 OR " +
                "index=strawberry earliest=2010-12-31T00:00:00.000+02:00";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedXml = "<AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND><AND><OR><earliest operation=\"GE\" value=\"-7200\"/><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND></OR><earliest operation=\"GE\" value=\"1293746400\"/></AND></AND>";
        final String expectedSpark = "((index RLIKE (?i)^seagull AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (((_time >= from_unixtime(-7200, yyyy-MM-dd HH:mm:ss)) OR (index RLIKE ^strawberry$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(1293746400, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
    }


    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest7( ) {
        String query = "(index=strawberry earliest=2019-01-01T00:00:00Z) AND (index=seagull) earliest=2009-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final String expectedXml = "<AND><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1546293600\"/></AND><AND><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1230760800\"/></AND></AND>";
            final String expectedSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1546293600, yyyy-MM-dd HH:mm:ss))) AND ((index RLIKE ^seagull$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1230760800, yyyy-MM-dd HH:mm:ss))))";

            assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
            assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        });
    }

    @Test
    @Disabled(value="Should be changed to a dataframe test")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void defaultEarliestLatestTest8( ) {
        String query = "(index=strawberry earliest=2019-01-01T00:00:00Z) AND (index=seagull) earliest=2009-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final String expectedXml = "<AND><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1546293600\"/></AND><AND><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1230760800\"/></AND></AND>";
            final String expectedSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1546293600, yyyy-MM-dd HH:mm:ss))) AND ((index RLIKE ^seagull$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1230760800, yyyy-MM-dd HH:mm:ss))))";

            assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
            assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void searchIssue383Test( ) {
        // test issue 383
        // Case: can't match XYZ="yes asd" _raw column, except by omitting double quotes entirely
        String query = " index=abc earliest=\"01/01/2022:00:00:00\" latest=\"01/02/2022:00:00:00\" \"XYZ=\\\"yes asd\\\"\" ";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final String expectedSpark = "(RLIKE(index, (?i)^abc$) AND (((_time >= from_unixtime(1640988000, yyyy-MM-dd HH:mm:ss)) AND (_time < from_unixtime(1641074400, yyyy-MM-dd HH:mm:ss))) AND RLIKE(_raw, (?i)^.*\\QXYZ=\"yes asd\"\\E.*)))";
            assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void searchIssue383_2Test( ) {
        // test issue 383
        // Case: can't match XYZ="yes asd" _raw column, except by omitting double quotes entirely
        String query = " index=abc \"XYZ=\\\"yes asd\\\"\" ";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedSpark = "(RLIKE(index, (?i)^abc$) AND RLIKE(_raw, (?i)^.*\\QXYZ=\"yes asd\"\\E.*))";
        assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value="Broken on pth-03")
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void searchIssue384Test( ) {
        // test issue 384
        // FIXME: Parser error: line 1:35 token recognition error at: '"[17/Aug/2023:08:03:55.441546368 +0300] conn='
        // |makeresultscount=1|eval_raw=917818
        String query = " | makeresults count=1 | eval _raw=\"[10/Jan/2020:05:03:55.441546368 +0300] xyz=654321 ab=2 DEF pid=\\\"1.23.456.7.899999.1.2.34\\\" key=\\\"random-words-here\\\"\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            Row r = res.select("_raw").first();
            String s = r.getAs(0).toString();

            assertEquals("[17/Aug/2023:08:03:55.441546368 +0300] conn=917818 op=5 EXT oid=\"2.16.840.1.113730.3.5.12\" name=\"replication-multimaster-extop\"", s);
        });
    }

    @Disabled(value="Broken on pth-03") /* FIXME: Parser can't handle = symbol inside quotes */
    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void searchIssue384_2Test( ) {
        // test issue 384
        // works with escaping '=' symbols
        String query = " | makeresults count=1 | eval _raw=\"[10/Jan/2020:05:03:55.441546368 +0300] xyz\\=654321 ab\\=2 DEF pid\\=\\\"1.23.456.7.899999.1.2.34\\\" key\\=\\\"random-words-here\\\"\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            Row r = res.select("_raw").first();
            String s = r.getAs(0).toString();

            assertEquals("[17/Aug/2023:08:03:55.441546368 +0300] conn=917818 op=5 EXT oid=\"2.16.840.1.113730.3.5.12\" name=\"replication-multimaster-extop\"", s);
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void searchIssue382Test( ) {
        // test issue 382
        // case: index=* earliest=x latest=y abcdef and index=*abc* earliest=x latest=y abcdef match differently (data/no data)
        this.streamingTestUtil.performDPLTest("index=*g*", this.testFile,res -> {
            if (res.count() == 0){
                fail("(index=*g*) Expected result rows, instead got 0");
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(named="skipSparkTest", matches="true")
    public void searchIssue382Test2( ) {
        // test issue 382
        // index=* case
        this.streamingTestUtil.performDPLTest("index=*", this.testFile, res -> {
            if (res.count() == 0) {
                fail("(index=*) Expected result rows, instead got 0");
            }
        });
    }

    // Util function: Set _time column in data to be the same as if $date is current time
    private Function<Dataset<Row>, Dataset<Row>> setTimeDifferenceToSameAsDate(final String date) {
        return rowDataset -> {
            // Calculate the time difference between time in data and intended time.
            // Use that difference to update the test data, so the tests will function the same regardless
            // of the current time.

            // Current time as unix epoch
            Column currentTimeAsUnixEpoch = functions.unix_timestamp(functions.current_timestamp());
            // Intended time as unix epoch
            Column defaultTimeAsUnixEpoch = functions.unix_timestamp(functions.lit(date));
            // Time in data as unix epoch
            Column timeInDataAsUnixEpoch = functions.unix_timestamp(functions.col("_time"));

            // Calculate new _time column using the time difference between intended time and old _time column
            Column defaultTimeMinusTimeInData = defaultTimeAsUnixEpoch.minus(timeInDataAsUnixEpoch);
            Column currentTimeMinusTimeDifference = currentTimeAsUnixEpoch.minus(defaultTimeMinusTimeInData);
            Column newTimeColumn = currentTimeMinusTimeDifference.cast(DataTypes.TimestampType);

            return rowDataset.withColumn("_time", newTimeColumn);
        };
    }
}
