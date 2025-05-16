/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
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

import com.teragrep.pth10.ast.time.RelativeTimestamp;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tests time-related aspects of the project, such as TimeStatement. Uses streaming datasets
 *
 * @author eemhu
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EarliestLatestTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EarliestLatestTest.class);

    // Use this file for dataset initialization
    private final String testFile = "src/test/resources/earliestLatestTest_data*.jsonl"; // * to make the path into a directory path
    private final String epochTestFile = "src/test/resources/earliestLatestTest_epoch_data*.jsonl";
    private StreamingTestUtil streamingTestUtil;
    private final ZoneId utcZone = ZoneId.of("UTC");

    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(testSchema);
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    // FIXME: earliest-latest defaulting removed and default tests set to Disabled to fix issue #351

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeInTheMiddleTest() {
        String query = "index=strawberry earliest=-10y OR index=seagull";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertTrue(indexAsList.contains("strawberry"));
                    Assertions.assertFalse(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestLatestNotSpecifiedTest() {
        String query = "index=strawberry OR index=seagull | stats count(_raw) by index";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> indexAsList = res
                    .select("index")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertTrue(indexAsList.contains("strawberry"));
            Assertions.assertTrue(indexAsList.contains("seagull"));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeInTheEndTest() {
        String query = "index=strawberry OR index=seagull earliest=-10y | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertTrue(indexAsList.contains("strawberry"));
                    Assertions.assertTrue(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeAtStartOfQueryTest() {
        String query = "earliest=-10y index=strawberry OR index=seagull | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertTrue(indexAsList.contains("strawberry"));
                    Assertions.assertTrue(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void SimilarEarliestLatestRelativeTimeWithBracketsTest() {
        String query = "earliest=-10y index=strawberry OR (index=seagull latest=-10y) | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertTrue(indexAsList.contains("strawberry"));
                    Assertions.assertFalse(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void similarEarliestLatestRelativeTimeTest() {
        String query = "earliest=-10y index=strawberry OR index=seagull latest=-10y | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertFalse(indexAsList.contains("strawberry"));
                    Assertions.assertFalse(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeAtStartAndLatestAtEndOfMainSearchTest() {
        String query = "earliest=-10y index=strawberry OR index=seagull latest=-1y | stats count(_raw) by index";
        streamingTestUtil.performDPLTest(query, testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
            List<String> indexAsList = res
                    .select("index")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertTrue(indexAsList.contains("strawberry"));
            Assertions.assertTrue(indexAsList.contains("seagull"));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeAtStartAndEndOfMainSearchTest() {
        String query = "earliest=-20y index=strawberry OR index=seagull earliest=-1d | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertFalse(indexAsList.contains("strawberry"));
                    Assertions.assertFalse(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeAtStartAndEarliestNowAtEndOfMainSearchTest() {
        String query = "earliest=-20y index=strawberry OR index=seagull earliest=now | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertFalse(indexAsList.contains("strawberry"));
                    Assertions.assertFalse(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void earliestRelativeTimeAtStartAndLatestNowAtEndOfMainSearchTest() {
        String query = "earliest=-20y index=strawberry OR index=seagull latest=now | stats count(_raw) by index";
        this.streamingTestUtil
                .performDPLTest(query, this.testFile, setTimeDifferenceToSameAsDate("2023-01-01 12:00:00"), res -> {
                    List<String> indexAsList = res
                            .select("index")
                            .collectAsList()
                            .stream()
                            .map(r -> r.getAs(0).toString())
                            .collect(Collectors.toList());

                    Assertions.assertTrue(indexAsList.contains("strawberry"));
                    Assertions.assertTrue(indexAsList.contains("seagull"));
                });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultFormatTest() { // MM/dd/yyyy:HH:mm:ss 2013-07-15 10:01:50
        String query = "(index=strawberry OR index=seagull) AND earliest=03/15/2014:00:00:00";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res
                    .select("_time")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            // Daylight savings happens between these two timestamps, which explains the +1 hour difference.
            Assertions.assertEquals("2014-04-15 09:23:17.0", time.get(0));
            Assertions.assertEquals("2014-03-15 21:54:14.0", time.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultFormatInvalidInputTest() { // MM/dd/yyyy:HH:mm:ss 2013-07-15 10:01:50
        String query = "(index=strawberry OR index=seagull) AND earliest=31/31/2014:00:00:00";
        RuntimeException sqe = this.streamingTestUtil
                .performThrowingDPLTest(RuntimeException.class, query, this.testFile, res -> {
                });
        Assertions
                .assertEquals(
                        "TimeQualifier conversion error: <31/31/2014:00:00:00> can't be parsed using default formats.",
                        sqe.getMessage()
                );
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void ISO8601ZonedFormatTest() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry OR index=seagull) AND earliest=2014-03-15T00:00:00+03:00";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res
                    .select("_time")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals("2014-04-15 09:23:17.0", time.get(0));
            Assertions.assertEquals("2014-03-15 21:54:14.0", time.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void ISO8601WithoutZoneFormatTest() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry OR index=seagull) AND earliest=2014-03-15T00:00:00";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res
                    .select("_time")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals("2014-04-15 09:23:17.0", time.get(0));
            Assertions.assertEquals("2014-03-15 21:54:14.0", time.get(1));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void OverflowTest() {
        String query = "index=strawberry latest=-3644444444444444d";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res
                    .select("_time")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            Assertions.assertTrue(time.isEmpty());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void OverflowTest2() {
        String query = "index=strawberry earliest=-1000y@y latest=+3644444444444444d";
        streamingTestUtil.performDPLTest(query, testFile, res -> {
            String maxTime = res.agg(functions.max("_time")).first().get(0).toString();
            String minTime = res.agg(functions.min("_time")).first().get(0).toString();
            Assertions.assertEquals("2014-03-15 21:54:14.0", maxTime);
            Assertions.assertEquals("2013-07-15 11:01:50.0", minTime);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void epochTimeformatTest() {
        String query = "index=strawberry timeformat=%s earliest=-10000";
        this.streamingTestUtil.performDPLTest(query, this.epochTestFile, res -> {
            // epoch test data contains values from 1970-01-01 till 2050-03-15
            String maxTime = res.agg(functions.max("_time")).first().get(0).toString();
            String minTime = res.agg(functions.min("_time")).first().get(0).toString();

            Assertions.assertEquals("2050-03-15 21:54:14.0", maxTime);
            Assertions.assertEquals("1970-01-01 00:00:00.0", minTime);
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void customTimeformatTest() {
        String query = "index=strawberry timeformat=\"%Y-%m-%d-%H-%M-%S\" earliest=2030-01-01-00-00-00 latest=2040-01-01-00-00-00";
        this.streamingTestUtil.performDPLTest(query, this.epochTestFile, res -> {
            // epoch test data contains values from 1970-01-01 till 2050-03-15
            String maxTime = res.agg(functions.max("_time")).first().get(0).toString();
            String minTime = res.agg(functions.min("_time")).first().get(0).toString();

            Assertions.assertEquals("2030-01-14 00:56:08.0", maxTime);
            Assertions.assertEquals("2030-01-14 00:56:08.0", minTime);
        });
    }

    @Test
    public void RelativeTimestampSecondsTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("s", "sec", "secs", "second", "seconds");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusSeconds(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampMinutesTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("m", "min", "minute", "minutes");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusMinutes(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampHoursTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("h", "hr", "hrs", "hour", "hours");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusHours(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampDaysTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("d", "day", "days");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusDays(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampWeeksTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("w", "week", "weeks");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusWeeks(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampMonthsTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("mon", "month", "months");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusMonths(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampYearsTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        // Various possible unit strings
        final List<String> units = Arrays.asList("y", "yr", "yrs", "year", "years");
        // Amount to add
        final int amount = 100;
        // Expected result
        final long expected = startTime.plusYears(100).toEpochSecond();

        units.forEach(unit -> {
            String relativeTimestamp = "+" + amount + unit; //+100d etc.
            ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
            Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
        });
    }

    @Test
    public void RelativeTimestampOver9999YearTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = "+10000y";

        // Expected result
        final long expected = startTime.withYear(9999).toEpochSecond();

        ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void RelativeTimestampLessThan1000YearTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = "-1100y";

        // Expected result
        final long expected = startTime.withYear(1000).toEpochSecond();

        ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void RelativeTimestampInvalidUnitTest() {
        // Initial values
        final String relativeTimestamp = "xyz";
        final RelativeTimestamp timestamp = new RelativeTimestamp(relativeTimestamp, ZonedDateTime.now());
        Assertions
                .assertThrows(
                        RuntimeException.class, timestamp::zonedDateTime,
                        "Relative timestamp contained an invalid time unit"
                );
    }

    @Test
    public void RelativeTimestampOverflowPositiveTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = String.format("+%sy", Long.MAX_VALUE);

        // Expected result
        final long expected = startTime.withYear(9999).toEpochSecond();

        ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    public void RelativeTimestampOverflowNegativeTest() {
        // Initial values
        final ZonedDateTime startTime = ZonedDateTime.of(2010, 10, 10, 15, 15, 30, 0, utcZone);
        final String relativeTimestamp = String.format("%sy", Long.MIN_VALUE);

        // Expected result
        final long expected = startTime.withYear(1000).toEpochSecond();

        ZonedDateTime zonedDateTime = new RelativeTimestamp(relativeTimestamp, startTime).zonedDateTime();
        Assertions.assertEquals(expected, zonedDateTime.toEpochSecond());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest1() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry OR index=seagull)";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(-1L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^strawberry AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^seagull AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))))";

        Assertions.assertEquals(expXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        Assertions.assertEquals(expSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest2() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry earliest=2014-03-15T21:54:14+02:00) OR index=seagull)";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(-1L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1394913254\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1394913254, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^seagull AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))))";

        Assertions.assertEquals(expXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        Assertions.assertEquals(expSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest3() { // '2011-12-03T10:15:30+01:00'
        String query = "(index=strawberry earliest=2014-03-15T21:54:14+02:00) OR (index=seagull earliest=2014-04-15T08:23:17+02:00))";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1678713103L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            List<String> time = res
                    .select("_time")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());

            Assertions.assertEquals(2, time.size());
            Assertions.assertEquals("2014-04-15 08:23:17", time.get(0));
            Assertions.assertEquals("2014-03-15 21:54:14", time.get(1));
        });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1678713103\"/></AND><earliest operation=\"GE\" value=\"1394913254\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><latest operation=\"LE\" value=\"1678713103\"/></AND><earliest operation=\"GE\" value=\"1397539397\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1678713103, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1394913254, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^seagull AND (_time <= from_unixtime(1678713103, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1397539397, yyyy-MM-dd HH:mm:ss))))";

        Assertions.assertEquals(expXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        Assertions.assertEquals(expSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest4() {
        String query = "(earliest=1900-01-01T00:00:00Z latest=1960-01-01T00:00:00Z) OR ((index=seagull earliest=1970-01-01T00:00:00Z latest=2100-01-01T00:00:00Z)) OR index=strawberry earliest=1950-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1678711652L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedXml = "<AND>" + "<OR>" + "<OR>" + "<AND>"
                + "<earliest operation=\"GE\" value=\"-2208994789\"/>"
                + "<latest operation=\"LE\" value=\"-315626400\"/>" + "</AND>" + "<AND>" + "<AND>"
                + "<comparisonstatement field=\"index\" operation=\"=\" value=\"seagull\"/>"
                + "<earliest operation=\"GE\" value=\"-7200\"/>" + "</AND>"
                + "<latest operation=\"LE\" value=\"4102437600\"/>" + "</AND>" + "</OR>" + "<AND>"
                + "<comparisonstatement field=\"index\" operation=\"=\" value=\"strawberry\"/>"
                + "<latest operation=\"LE\" value=\"1678711652\"/>" + "</AND>" + "</OR>"
                + "<earliest operation=\"GE\" value=\"-631159200\"/>" + "</AND>";
        final String expectedSpark = "(((((_time >= from_unixtime(-2208994789, yyyy-MM-dd HH:mm:ss)) AND (_time <= from_unixtime(-315626400, yyyy-MM-dd HH:mm:ss)))"
                + " OR ((index RLIKE ^seagull$ AND (_time >= from_unixtime(-7200, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(4102437600, yyyy-MM-dd HH:mm:ss))))"
                + " OR (index RLIKE ^strawberry$ AND (_time <= from_unixtime(1678711652, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(-631159200, yyyy-MM-dd HH:mm:ss)))";

        Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        Assertions.assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest5() {
        String query = "index=strawberry OR index=seagull earliest=2020-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedXml = "<AND><OR><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND></OR><earliest operation=\"GE\" value=\"1577829600\"/></AND>";
        final String expectedSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) OR (index RLIKE (?i)^seagull AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(1577829600, yyyy-MM-dd HH:mm:ss)))";

        Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        Assertions.assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest6() {
        String query = "index=seagull earliest=1970-01-01T00:00:00.000+02:00 OR "
                + "index=strawberry earliest=2010-12-31T00:00:00.000+02:00";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedXml = "<AND><AND><index operation=\"EQUALS\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND><AND><OR><earliest operation=\"GE\" value=\"-7200\"/><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND></OR><earliest operation=\"GE\" value=\"1293746400\"/></AND></AND>";
        final String expectedSpark = "((index RLIKE (?i)^seagull AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (((_time >= from_unixtime(-7200, yyyy-MM-dd HH:mm:ss)) OR (index RLIKE ^strawberry$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(1293746400, yyyy-MM-dd HH:mm:ss))))";

        Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        Assertions.assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest7() {
        String query = "(index=strawberry earliest=2019-01-01T00:00:00Z) AND (index=seagull) earliest=2009-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final String expectedXml = "<AND><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1546293600\"/></AND><AND><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1230760800\"/></AND></AND>";
            final String expectedSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1546293600, yyyy-MM-dd HH:mm:ss))) AND ((index RLIKE ^seagull$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1230760800, yyyy-MM-dd HH:mm:ss))))";

            Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
            Assertions.assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        });
    }

    @Test
    @Disabled(value = "Will be looked at in issue #295")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void defaultEarliestLatestTest8() {
        String query = "(index=strawberry earliest=2019-01-01T00:00:00Z) AND (index=seagull) earliest=2009-01-01T00:00:00Z";
        this.streamingTestUtil.getCtx().setDplDefaultEarliest(0L);
        this.streamingTestUtil.getCtx().setDplDefaultLatest(1L);

        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final String expectedXml = "<AND><AND><AND><index operation=\"EQUALS\" value=\"strawberry\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1546293600\"/></AND><AND><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"seagull\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1230760800\"/></AND></AND>";
            final String expectedSpark = "(((index RLIKE (?i)^strawberry AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1546293600, yyyy-MM-dd HH:mm:ss))) AND ((index RLIKE ^seagull$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1230760800, yyyy-MM-dd HH:mm:ss))))";

            Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
            Assertions.assertEquals(expectedXml, this.streamingTestUtil.getCtx().getArchiveQuery());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchIssue383Test() {
        // test issue 383
        // Case: can't match XYZ="yes asd" _raw column, except by omitting double quotes entirely
        String query = " index=abc earliest=\"01/01/2022:00:00:00\" latest=\"01/02/2022:00:00:00\" \"XYZ=\\\"yes asd\\\"\" ";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            final String expectedSpark = "(RLIKE(index, (?i)^abc$) AND (((_time >= from_unixtime(1640988000, yyyy-MM-dd HH:mm:ss)) AND (_time < from_unixtime(1641074400, yyyy-MM-dd HH:mm:ss))) AND RLIKE(_raw, (?i)^.*\\QXYZ=\"yes asd\"\\E.*)))";
            Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchIssue383_2Test() {
        // test issue 383
        // Case: can't match XYZ="yes asd" _raw column, except by omitting double quotes entirely
        String query = " index=abc \"XYZ=\\\"yes asd\\\"\" ";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {

        });

        final String expectedSpark = "(RLIKE(index, (?i)^abc$) AND RLIKE(_raw, (?i)^.*\\QXYZ=\"yes asd\"\\E.*))";
        Assertions.assertEquals(expectedSpark, this.streamingTestUtil.getCtx().getSparkQuery());
    }

    @Test
    @Disabled(value = "Broken on pth-03")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchIssue384Test() {
        // test issue 384
        // FIXME: Parser error: line 1:35 token recognition error at: '"[17/Aug/2023:08:03:55.441546368 +0300] conn='
        // |makeresultscount=1|eval_raw=917818
        String query = " | makeresults count=1 | eval _raw=\"[10/Jan/2020:05:03:55.441546368 +0300] xyz=654321 ab=2 DEF pid=\\\"1.23.456.7.899999.1.2.34\\\" key=\\\"random-words-here\\\"\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            Row r = res.select("_raw").first();
            String s = r.getAs(0).toString();

            Assertions
                    .assertEquals(
                            "[17/Aug/2023:08:03:55.441546368 +0300] conn=917818 op=5 EXT oid=\"2.16.840.1.113730.3.5.12\" name=\"replication-multimaster-extop\"",
                            s
                    );
        });
    }

    @Disabled(value = "Broken on pth-03") /* FIXME: Parser can't handle = symbol inside quotes */
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchIssue384_2Test() {
        // test issue 384
        // works with escaping '=' symbols
        String query = " | makeresults count=1 | eval _raw=\"[10/Jan/2020:05:03:55.441546368 +0300] xyz\\=654321 ab\\=2 DEF pid\\=\\\"1.23.456.7.899999.1.2.34\\\" key\\=\\\"random-words-here\\\"\"";
        this.streamingTestUtil.performDPLTest(query, this.testFile, res -> {
            Row r = res.select("_raw").first();
            String s = r.getAs(0).toString();

            Assertions
                    .assertEquals(
                            "[17/Aug/2023:08:03:55.441546368 +0300] conn=917818 op=5 EXT oid=\"2.16.840.1.113730.3.5.12\" name=\"replication-multimaster-extop\"",
                            s
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchIssue382Test() {
        // test issue 382
        // case: index=* earliest=x latest=y abcdef and index=*abc* earliest=x latest=y abcdef match differently (data/no data)
        this.streamingTestUtil.performDPLTest("index=*g*", this.testFile, res -> {
            if (res.count() == 0) {
                Assertions.fail("(index=*g*) Expected result rows, instead got 0");
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void searchIssue382Test2() {
        // test issue 382
        // index=* case
        this.streamingTestUtil.performDPLTest("index=*", this.testFile, res -> {
            if (res.count() == 0) {
                Assertions.fail("(index=*) Expected result rows, instead got 0");
            }
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void testFractionsOfSecondsInLatestTimeQualifier() {
        this.streamingTestUtil
                .performDPLTest(
                        "index=* earliest=2013-07-15T10:01:50.000+02:00 latest=2014-04-15T08:23:17.001+02:00",
                        this.testFile, res -> {
                            Row r = res.select("_time").agg(functions.min("_time"), functions.max("_time")).first();
                            Assertions.assertEquals("2013-07-15 11:01:50.0", r.get(0).toString());
                            Assertions.assertEquals("2014-04-15 09:23:17.0", r.get(1).toString());
                        }
                );
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
