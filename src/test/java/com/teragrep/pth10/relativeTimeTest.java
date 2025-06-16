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
package com.teragrep.pth10;

import com.teragrep.pth10.ast.DefaultTimeFormat;
import com.teragrep.pth10.ast.time.RelativeTimeParser;
import com.teragrep.pth10.ast.time.RelativeTimestamp;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class relativeTimeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(relativeTimeTest.class);
    private TimeZone originalTimeZone = null;

    // use this file to initialize the streaming dataset
    String testFile = "src/test/resources/xmlWalkerTestDataStreaming";
    private StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        // set default timezone
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));

        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    @AfterAll
    void recoverTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseEpochTimeformatTest() {
        // unix epoch format
        String q = "index=kafka_topic timeformat=%s earliest=1587032680 latest=1587021942";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            long latestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");

            String regex = "^.*_time >= from_unixtime\\(1587032680.*_time < from_unixtime\\(" + latestEpoch + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseDefaultTimeformatTest() {
        // default but given manually
        String q = "index=kafka_topic timeformat=%m/%d/%Y:%H:%M:%S earliest=\"04/16/2020:10:24:40\" latest=\"04/16/2020:10:25:42\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            long latestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");

            String regex = "^.*_time >= from_unixtime\\(1587021880.*_time < from_unixtime\\(" + latestEpoch + ".*$";
            LOGGER.info("Complex timeformat<{}>", q);
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseCustomTimeFormatTest() {
        // custom format SS-MM-HH YY-DD-MM
        String q = "index=kafka_topic timeformat=\"%S-%M-%H %Y-%d-%m\" earliest=\"40-24-10 2020-16-04\" latest=\"42-25-10 2020-16-04\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            long latestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");

            String regex = "^.*_time >= from_unixtime\\(1587021880.*_time < from_unixtime\\(" + latestEpoch + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseCustomComplexTimeformatTest() {
        // earliest custom format ISO8601 + HH:MM:SS , latest default
        String q = "index=kafka_topic timeformat=\"%F %T\" earliest=\"2020-04-16 10:24:40\" latest=\"2020-04-16 10:25:42\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            long latestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");

            String regex = "^.*_time >= from_unixtime\\(1587021880.*_time < from_unixtime\\(" + latestEpoch + ".*$";
            LOGGER.info("Complex timeformat<{}>", q);
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseCustomTimeformatWithMonthNameTest() {
        // earliest custom format 16 Apr 2020 10.24.40 AM (dd MMM y hh.mm.ss a) , latest default
        String q = "index=kafka_topic timeformat=\"%d %b %Y %I.%M.%S %p\" earliest=\"16 Apr 2020 10.24.40 AM\" latest=\"16 Apr 2020 10.25.42 AM\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            long latestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");

            String regex = "^.*_time >= from_unixtime\\(1587021880.*_time < from_unixtime\\(" + latestEpoch + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseStarttimeuTest() {
        String q = "index=cinnamon starttimeu=1587032680";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String regex = "^.*_time >= from_unixtime\\(1587032680.*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseEndtimeuTest() {
        String q = "index=cinnamon endtimeu=1587032680";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String regex = "^.*_time < from_unixtime\\(1587032680.*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampEarliestRelativeTest() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant t1 = timestamp.toInstant();
        RelativeTimeParser rtParser = new RelativeTimeParser();

        // Using instant-method
        // -1 h
        Instant exp = t1.plus(-1, ChronoUnit.HOURS);
        LocalDateTime etime = LocalDateTime.ofInstant(exp, ZoneOffset.UTC);
        RelativeTimestamp rtTimestamp = rtParser.parse("-1h");
        long rtEpoch = rtTimestamp.calculate(timestamp).getEpochSecond();
        Assertions
                .assertEquals(etime.getHour(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtEpoch), ZoneOffset.UTC).getHour());

        // -3 min
        exp = t1.plus(-3, ChronoUnit.MINUTES);
        etime = LocalDateTime.ofInstant(exp, ZoneOffset.UTC);
        rtTimestamp = rtParser.parse("-3m");
        rtEpoch = rtTimestamp.calculate(timestamp).getEpochSecond();
        Assertions
                .assertEquals(etime.getMinute(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtEpoch), ZoneOffset.UTC).getMinute());
        // Using localDateTime-method
        // -1 week
        LocalDateTime dt = timestamp.toLocalDateTime();
        LocalDateTime et = dt.minusWeeks(1);
        rtTimestamp = rtParser.parse("-1w");
        rtEpoch = rtTimestamp.calculate(timestamp).getEpochSecond();
        Assertions
                .assertEquals(et.getDayOfWeek(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtEpoch), ZoneOffset.UTC).getDayOfWeek());
        // -3 month
        dt = timestamp.toLocalDateTime();
        et = dt.minusMonths(3);
        rtTimestamp = rtParser.parse("-3mon");
        rtEpoch = rtTimestamp.calculate(timestamp).getEpochSecond();
        Assertions
                .assertEquals(et.getMonth(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtEpoch), ZoneOffset.UTC).getMonth());

        // -7 year
        dt = timestamp.toLocalDateTime();
        et = dt.minusYears(7);
        rtTimestamp = rtParser.parse("-7y");
        rtEpoch = rtTimestamp.calculate(timestamp).getEpochSecond();
        Assertions
                .assertEquals(et.getYear(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtEpoch), ZoneOffset.UTC).getYear());
    }

    // test snap-to-time "@d"
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampSnapToTimeRelativeTest() {
        // Test for snap-to-time functionality
        // for example "-@d" would snap back to midnight of set day
        long epochSeconds = 1643881600; // Thursday, February 3, 2022 09:46:40 UTC
        Timestamp timestamp = new Timestamp(epochSeconds * 1000L);
        RelativeTimeParser rtParser = new RelativeTimeParser();

        LocalDateTime dt = timestamp.toLocalDateTime();
        LocalDateTime et = dt.minusHours(9);
        et = et.minusMinutes(46);
        et = et.minusSeconds(40); // Thu Feb 3, 2022 00:00 UTC

        RelativeTimestamp rtTimestamp = rtParser.parse("@d");
        long rtEpoch = rtTimestamp.calculate(timestamp).getEpochSecond();
        Assertions
                .assertEquals(et.getDayOfWeek(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtEpoch), ZoneOffset.systemDefault()).getDayOfWeek());
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestRelativeTest() {
        String q = "index=cinnamon latest=-3h ";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Instant now = timestamp.toInstant().plus(-3, ChronoUnit.HOURS);

            String expected = String.valueOf(now.getEpochSecond()).substring(0, 7); // don't check the seconds within a minute, as the query takes some time and might be a few seconds off
            String regex = "^.*_time < from_unixtime\\(" + expected + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestRelativeTestWithPlus() {
        String q = "index=cinnamon latest=+3h ";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Instant now = timestamp.toInstant().plus(3, ChronoUnit.HOURS);

            String expected = String.valueOf(now.getEpochSecond()).substring(0, 7); // don't check the seconds within a minute, as the query takes some time and might be a few seconds off
            String regex = "^.*_time < from_unixtime\\(" + expected + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestRelativeTestWithoutSign() {
        String q = "index=cinnamon latest=3h ";
        String expected = "TimeQualifier conversion error: <3h> can't be parsed.";

        RuntimeException exception = this.streamingTestUtil
                .performThrowingDPLTest(RuntimeException.class, q, this.testFile, res -> {
                });

        Assertions.assertEquals(expected, exception.getMessage());
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestRelativeSnapTest() {
        String q = "index=cinnamon latest=@d ";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ZonedDateTime now = timestamp.toInstant().atZone(ZoneId.systemDefault());
            now = now.truncatedTo(ChronoUnit.DAYS); // snap to start of day
            long expected = now.toInstant().getEpochSecond(); // transform to Instant and get epoch

            String regex = "^.*_time < from_unixtime\\(" + expected + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestRelativeSnapWithOffsetTest() {
        String q = "index=cinnamon latest=@d+3h ";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ZonedDateTime now = timestamp.toInstant().atZone(ZoneId.systemDefault());
            now = now.truncatedTo(ChronoUnit.DAYS); // snap to start of day
            now = now.plus(3, ChronoUnit.HOURS); // add three hours
            long expected = now.toInstant().getEpochSecond(); // transform to Instant and get epoch

            String regex = "^.*_time < from_unixtime\\(" + expected + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestRelativeNowTest() {
        String q = "index=cinnamon latest=now ";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Instant now = timestamp.toInstant();

            String expected = String.valueOf(now.getEpochSecond()).substring(0, 7); // don't check the seconds within a minute, as the query takes some time and might be a few seconds off
            String regex = "^.*_time < from_unixtime\\(" + expected + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampEarliestRelativeSnapToDayLatestNow() {
        // pth10 ticket #24 query: 'index=... sourcetype=... earliest=@d latest=now'
        String q = "index=cinnamon earliest=@d latest=now";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ZonedDateTime now = timestamp.toInstant().atZone(ZoneId.systemDefault());
            long expectedEarliest = now.truncatedTo(ChronoUnit.DAYS).toInstant().getEpochSecond();
            long expectedLatest = now.toEpochSecond();
            String expectedLatestString = String.valueOf(expectedLatest).substring(0, 7); // don't check last 2 numbers as the query takes some time and the "now" is different
            String regex = "^.*_time >= from_unixtime\\(" + expectedEarliest + ".*_time < from_unixtime\\("
                    + expectedLatestString + ".*$";
            ;
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    // should throw an exception
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampRelativeInvalidSnapToTimeUnitTest() {
        // pth10 ticket #66 query: 'index=... sourcetype=... earliest=@-5h latest=@-3h'
        String query = "index=cinnamon earliest=\"@-5h\" latest=\"@-3h\"";
        String expected = "TimeQualifier conversion error: <@-5h> can't be parsed.";

        RuntimeException exception = this.streamingTestUtil
                .performThrowingDPLTest(RuntimeException.class, query, this.testFile, res -> {
                });

        Assertions.assertEquals(expected, exception.getMessage());
    }

    // should throw an exception
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampRelativeInvalidTimeUnitQueryTest() {
        String q = "index=cinnamon earliest=-5x latest=-7z";
        String e = "Relative timestamp contained an invalid time unit";

        Throwable exception = this.streamingTestUtil
                .performThrowingDPLTest(RuntimeException.class, q, this.testFile, res -> {
                });

        Assertions.assertEquals(e, exception.getMessage());
    }

    // test with quotes
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampRelativeWithQuotesTest() {
        String q = "index=cinnamon earliest=\"-3h@h\" latest=\"-1h@h\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Instant now = timestamp.toInstant();
            Instant earliest = now.minus(3L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS);
            Instant latest = now.minus(1L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS);
            String regex = "^.*_time >= from_unixtime\\(" + earliest.getEpochSecond() + ".*_time < from_unixtime\\("
                    + latest.getEpochSecond() + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    // test with -h
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampRelativeWithoutExplicitAmountOfTimeTest() {
        String q = "index=cinnamon earliest=\"-h\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Instant now = timestamp.toInstant();
            long earliestEpoch = now.minus(1L, ChronoUnit.HOURS).getEpochSecond();
            String earliestString = String.valueOf(earliestEpoch).substring(0, 7); // don't check last 2 indexes as the query takes some time and the "now" is different
            String regex = "^.*_time >= from_unixtime\\(" + earliestString + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    // test with relative timestamp, snap to time and offset
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampRelativeComplexTest() {
        String q = "index=cinnamon earliest=\"-3h@d+1d\"";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ZonedDateTime now = timestamp.toInstant().atZone(ZoneId.systemDefault());
            now = now.minus(3L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.DAYS).plus(1L, ChronoUnit.DAYS);
            long expected = now.toInstant().getEpochSecond();

            String regex = "^.*_time >= from_unixtime\\(" + expected + ".*$";
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampEarliestTest() {
        String q;
        // earliest
        q = "index=cinnamon earliest=\"04/16/2020:10:25:40\"";
        long earliestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:40");
        String regex = "^.*_time >= from_unixtime\\(" + earliestEpoch + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampLatestTest() {
        String q;
        // latest
        q = "index=cinnamon latest=\"04/16/2020:10:25:40\"";
        long latestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:40");
        String regex = ".*_time < from_unixtime\\(" + latestEpoch + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampEarliestLatestTest() {
        String q;
        // earliest, latest
        q = "index=cinnamon earliest=\"04/16/2020:10:25:40\" latest=\"04/16/2020:10:25:42\"";
        long earliestEpoch2 = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:40");
        long latestEpoch2 = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");
        String regex = "^.*_time >= from_unixtime\\(" + earliestEpoch2 + ".*_time < from_unixtime\\(" + latestEpoch2
                + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void parseTimestampIndexEarliestLatestTest() {
        // _index_earliest, _index_latest
        String q = "index=cinnamon _index_earliest=\"04/16/2020:10:25:40\" _index_latest=\"04/16/2020:10:25:42\"";
        long indexEarliestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:40");
        long indexLatestEpoch = new DefaultTimeFormat().getEpoch("04/16/2020:10:25:42");
        String regex = "^.*_time >= from_unixtime\\(" + indexEarliestEpoch + ".*_time < from_unixtime\\("
                + indexLatestEpoch + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void streamListTest() {
        String q = "index = memory earliest=\"05/08/2019:09:10:40\" latest=\"05/10/2022:09:11:40\" host=\"sc-99-99-14-25\" OR host=\"sc-99-99-14-20\" sourcetype=\"log:f17:0\" Latitude";
        long earliestEpoch = new DefaultTimeFormat().getEpoch("05/08/2019:09:10:40");
        long latestEpoch = new DefaultTimeFormat().getEpoch("05/10/2022:09:11:40");
        String regex = "^.*_time >= from_unixtime\\(" + earliestEpoch + ".*_time < from_unixtime\\(" + latestEpoch
                + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void streamList1Test() {
        String q = "index = memory-test earliest=\"05/08/2019:09:10:40\" latest=\"05/10/2022:09:11:40\" host=\"sc-99-99-14-25\" OR host=\"sc-99-99-14-20\" sourcetype=\"log:f17:0\" Latitude";
        long earliestEpoch2 = new DefaultTimeFormat().getEpoch("05/08/2019:09:10:40");
        long latestEpoch2 = new DefaultTimeFormat().getEpoch("05/10/2022:09:11:40");
        String regex = "^.*_time >= from_unixtime\\(" + earliestEpoch2 + ".*_time < from_unixtime\\(" + latestEpoch2
                + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void streamList2Test() {
        String q = "index = memory-test/yyy earliest=\"05/08/2019:09:10:40\" latest=\"05/10/2022:09:11:40\" host=\"sc-99-99-14-25\" OR host=\"sc-99-99-14-20\" sourcetype=\"log:f17:0\" Latitude";
        long earliestEpoch3 = new DefaultTimeFormat().getEpoch("05/08/2019:09:10:40");
        long latestEpoch3 = new DefaultTimeFormat().getEpoch("05/10/2022:09:11:40");
        String regex = "^.*_time >= from_unixtime\\(" + earliestEpoch3 + ".*_time < from_unixtime\\(" + latestEpoch3
                + ".*$";

        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            String result = this.streamingTestUtil.getCtx().getSparkQuery();
            Assertions.assertTrue(result.matches(regex));
        });
    }
}
