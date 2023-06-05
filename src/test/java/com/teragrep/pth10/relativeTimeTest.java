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

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static com.teragrep.pth10.ast.TimestampToEpochConversion.unixEpochFromString;
import static com.teragrep.pth10.ast.Util.relativeTimeModifier;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class relativeTimeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(relativeTimeTest.class);
	private static TimeZone originalTimeZone = null;
	
	@BeforeAll
	static void setTimeZone() {
		originalTimeZone = TimeZone.getDefault();
		TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
	}
	
	@BeforeEach
	void enforceTimeZone() {
		TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
	}
	
	@AfterAll
	static void recoverTimeZone() {
		TimeZone.setDefault(originalTimeZone);
	}

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimeformatTest() throws Exception {
        // earliest unix epoch format, latest default
        String q = "index=kafka_topic timeformat=%s earliest=1587032680 latest=\"04/16/2020:10:25:42\"";

        long latestEpoch = unixEpochFromString("04/16/2020:10:25:42", null);

        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _time >= from_unixtime(1587032680) AND _time <= from_unixtime(" + latestEpoch + ")";
        LOGGER.info("Complex timeformat<" + q + ">");
        String result = utils.getQueryAnalysis(q);
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimeformat_example_default_format_Test() throws Exception {
        // earliest default but given manually, latest default
        String q = "index=kafka_topic timeformat=%m/%d/%Y:%H:%M:%S earliest=\"04/16/2020:10:24:40\" latest=\"04/16/2020:10:25:42\"";

        long latestEpoch = unixEpochFromString("04/16/2020:10:25:42", null);

        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _time >= from_unixtime(1587021880) AND _time <= from_unixtime(" + latestEpoch + ")";
        LOGGER.info("Complex timeformat<" + q + ">");
        String result = utils.getQueryAnalysis(q);
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimeformat_custom_format1_Test() throws Exception {
        // earliest custom format SS-MM-HH YY-DD-MM, latest default
        String q = "index=kafka_topic timeformat=\"%S-%M-%H %Y-%d-%m\" earliest=\"40-24-10 2020-16-04\" latest=\"04/16/2020:10:25:42\"";

        long latestEpoch = unixEpochFromString("04/16/2020:10:25:42", null);

        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _time >= from_unixtime(1587021880) AND _time <= from_unixtime(" + latestEpoch + ")";
        LOGGER.info("Complex timeformat<" + q + ">");
        String result = utils.getQueryAnalysis(q);
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimeformat_custom_format2_Test() throws Exception {
        // earliest custom format ISO8601 + HH:MM:SS , latest default
        String q = "index=kafka_topic timeformat=\"%F %T\" earliest=\"2020-04-16 10:24:40\" latest=\"04/16/2020:10:25:42\"";

        long latestEpoch = unixEpochFromString("04/16/2020:10:25:42", null);

        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _time >= from_unixtime(1587021880) AND _time <= from_unixtime(" + latestEpoch + ")";
        LOGGER.info("Complex timeformat<" + q + ">");
        String result = utils.getQueryAnalysis(q);
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimeformat_custom_format3_Test() throws Exception {
        // earliest custom format 16 Apr 2020 10.24.40 AM (dd MMM y hh.mm.ss a) , latest default
        String q = "index=kafka_topic timeformat=\"%d %b %Y %I.%M.%S %p\" earliest=\"16 Apr 2020 10.24.40 AM\" latest=\"04/16/2020:10:25:42\"";

        long latestEpoch = unixEpochFromString("04/16/2020:10:25:42", null);

        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"kafka_topic\" AND _time >= from_unixtime(1587021880) AND _time <= from_unixtime(" + latestEpoch + ")";
        LOGGER.info("Complex timeformat<" + q + ">");
        String result = utils.getQueryAnalysis(q);
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampEuTest() throws Exception {
        String q = "index=voyager starttimeu=1587032680";
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= 1587032680";
        String result = null;
        assertEquals(e, result, q);
        q = "index=voyager endtimeu=1587032680";
        e = "SELECT * FROM `temporaryDPLView` WHERE LIKE = \"voyager\" AND _time <= from_unixtime(1587032680)";
        result = null;
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampEarliestRelativeTest() throws Exception {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant t1 = timestamp.toInstant();
        // Using instant-method
        // -1 h
        Instant exp = t1.plus(-1, ChronoUnit.HOURS);
        LocalDateTime etime = LocalDateTime.ofInstant(exp, ZoneOffset.UTC);
        long rtm = relativeTimeModifier(timestamp, "-1h");
        assertEquals(etime.getHour(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtm), ZoneOffset.UTC).getHour());

        // -3 min
        exp = t1.plus(-3, ChronoUnit.MINUTES);
        etime = LocalDateTime.ofInstant(exp, ZoneOffset.UTC);
        rtm = relativeTimeModifier(timestamp, "-3m");
        assertEquals(etime.getMinute(),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(rtm), ZoneOffset.UTC).getMinute());
        // Using localDateTime-method
        // -1 week
        LocalDateTime dt = timestamp.toLocalDateTime();
        LocalDateTime et = dt.minusWeeks(1);
        rtm = relativeTimeModifier(timestamp, "-1w");
        assertEquals(et.getDayOfWeek(),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(rtm), ZoneOffset.UTC).getDayOfWeek());
        // -3 month
        dt = timestamp.toLocalDateTime();
        et = dt.minusMonths(3);
        rtm = relativeTimeModifier(timestamp, "-3mon");
        assertEquals(et.getMonth(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtm), ZoneOffset.UTC).getMonth());

        // -7 year
        dt = timestamp.toLocalDateTime();
        et = dt.minusYears(7);
        rtm = relativeTimeModifier(timestamp, "-7y");
        assertEquals(et.getYear(), LocalDateTime.ofInstant(Instant.ofEpochSecond(rtm), ZoneOffset.UTC).getYear());
    }
    
    // test snap-to-time "@d"
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampSnapToTimeRelativeTest() throws Exception {
    	// Test for snap-to-time functionality
    	// for example "-@d" would snap back to midnight of set day
    	long epochSeconds = 1643881600; // Thursday, February 3, 2022 09:46:40 UTC
    	Timestamp timestamp = new Timestamp(epochSeconds*1000L);
    	
    	LocalDateTime dt = timestamp.toLocalDateTime();
        LocalDateTime et = dt.minusHours(9);
        et = et.minusMinutes(46);
        et = et.minusSeconds(40); // Thu Feb 3, 2022 00:00 UTC
        
        long rtm = relativeTimeModifier(timestamp, "@d");
        assertEquals(et.getDayOfWeek(),
                LocalDateTime.ofInstant(Instant.ofEpochSecond(rtm), ZoneOffset.UTC).getDayOfWeek());
    	
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampLatestRelativeTest() throws Exception {
        String q = "index=voyager latest=-3h ";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant().plus(-3, ChronoUnit.HOURS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time <= from_unixtime(" + String.valueOf(now.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampLatestRelativeSnapTest() throws Exception {
        String q = "index=voyager latest=@d ";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant().truncatedTo(ChronoUnit.DAYS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time <= from_unixtime(" + String.valueOf(now.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampLatestRelativeSnapWithOffsetTest() throws Exception {
        String q = "index=voyager latest=@d+3h ";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant().truncatedTo(ChronoUnit.DAYS);
        now = now.plus(3, ChronoUnit.HOURS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time <= from_unixtime(" + String.valueOf(now.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampLatestRelativeNowTest() throws Exception {
        String q = "index=voyager latest=now ";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant();
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time <= from_unixtime(" + String.valueOf(now.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampRelativeTicket24QueryTest() throws Exception {
    	// pth10 ticket #24 query: 'index=... sourcetype=... earliest=@d latest=now'
        String q = "index=voyager earliest=@d latest=now";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant();
        Instant nowTimeSnapped = now.truncatedTo(ChronoUnit.DAYS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + String.valueOf(nowTimeSnapped.getEpochSecond()) +") AND _time <= from_unixtime(" + String.valueOf(now.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    // should throw an exception
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampRelativeTicket66QueryTest() throws Exception {
    	// pth10 ticket #66 query: 'index=... sourcetype=... earliest=@-5h latest=@-3h'
        String q = "index=voyager earliest=\"@-5h\" latest=\"@-3h\"";
        String e = "TimeQualifier conversion error: <@-5h> can't be parsed.";
        
        Throwable exception = Assertions.assertThrows(RuntimeException.class, () -> utils.getQueryAnalysis(q));
        
        assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");
    }
    
    // should throw an exception
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampRelativeInvalidTimeUnitQueryTest() throws Exception {
        String q = "index=voyager earliest=5x latest=7z";
        String e = "Relative timestamp contained an invalid time unit";
        
        Throwable exception = Assertions.assertThrows(RuntimeException.class, () -> utils.getQueryAnalysis(q));
        
        assertEquals(e, exception.getMessage());
        throw new UnsupportedOperationException("Implement");
    }
    
    // test with quotes
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampRelativeWithQuotesTest() throws Exception {
        String q = "index=voyager earliest=\"-3h@h\" latest=\"-1h@h\"";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant();
        Instant earliest = now.minus(3L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS);
        Instant latest = now.minus(1L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + String.valueOf(earliest.getEpochSecond()) +") AND _time <= from_unixtime(" + String.valueOf(latest.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    // test with -h
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampRelativeWithoutExplicitAmountOfTimeTest() throws Exception {
        String q = "index=voyager earliest=\"-h\"";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant();
        Instant earliest = now.minus(1L, ChronoUnit.HOURS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + String.valueOf(earliest.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    // test with relative timestamp, snap to time and offset
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampRelativeComplexTest() throws Exception {
        String q = "index=voyager earliest=\"-3h@d+1d\"";
        String result = utils.getQueryAnalysis(q);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Instant now = timestamp.toInstant();
        Instant earliest = now.minus(3L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.DAYS).plus(1L, ChronoUnit.DAYS);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + String.valueOf(earliest.getEpochSecond()) + ")";
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }
    
    
    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampEarliestTest() throws Exception {
        String q, e, result;
        // earliest
        q = "index=voyager earliest=\"04/16/2020:10:25:40\"";
        long earliestEpoch = unixEpochFromString("04/16/2020:10:25:40", null);
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + earliestEpoch + ")";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampLatestTest() throws Exception {
        String q, e, result;
        // latest
        q = "index=voyager latest=\"04/16/2020:10:25:40\"";
        long latestEpoch = unixEpochFromString("04/16/2020:10:25:40", null);
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time <= from_unixtime(" + latestEpoch + ")";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampEarliestLatestTest() throws Exception {
        String q, e, result;
        // earliest, latest
        q = "index=voyager earliest=\"04/16/2020:10:25:40\" latest=\"04/16/2020:10:25:42\"";
        long earliestEpoch2 = unixEpochFromString("04/16/2020:10:25:40", null);
        long latestEpoch2 = unixEpochFromString("04/16/2020:10:25:42", null);
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + earliestEpoch2 + ") AND _time <= from_unixtime(" + latestEpoch2 + ")";
        //LOGGER.info("Complex timeformat<" + q + ">");
        result = utils.getQueryAnalysis(q);
        //LOGGER.info("Spark SQL=<" + result + ">");
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void parseTimestampIndexEarliestLatestTest() throws Exception {
        String q, e, result;
        // _index_earliest, _index_latest
        q = "index=voyager _index_earliest=\"04/16/2020:10:25:40\" _index_latest=\"04/16/2020:10:25:42\"";
        long indexEarliestEpoch = unixEpochFromString("04/16/2020:10:25:40", null);
        long indexLatestEpoch = unixEpochFromString("04/16/2020:10:25:42", null);
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"voyager\" AND _time >= from_unixtime(" + indexEarliestEpoch + ") AND _time <= from_unixtime(" + indexLatestEpoch + ")";
        result = utils.getQueryAnalysis(q);
        assertEquals(e, result, q);
    }


    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void streamListTest() throws Exception {
        String q = "index = memory earliest=\"05/08/2019:09:10:40\" latest=\"05/10/2022:09:11:40\" host=\"sc-99-99-14-25\" OR host=\"sc-99-99-14-20\" sourcetype=\"log:f17:0\" Latitude";
        long earliestEpoch = unixEpochFromString("05/08/2019:09:10:40", null);
        long latestEpoch = unixEpochFromString("05/10/2022:09:11:40", null);
        String e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory\" AND _time >= from_unixtime(" + earliestEpoch + ") AND _time <= from_unixtime(" + latestEpoch + ") AND host LIKE \"sc-99-99-14-25\" OR host LIKE \"sc-99-99-14-20\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
        String result = utils.getQueryAnalysis(q);
        //LOGGER.info("DPL      =<" + q + ">");
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void streamList1Test() throws Exception {
        String q, e, result;
        q = "index = memory-test earliest=\"05/08/2019:09:10:40\" latest=\"05/10/2022:09:11:40\" host=\"sc-99-99-14-25\" OR host=\"sc-99-99-14-20\" sourcetype=\"log:f17:0\" Latitude";
        long earliestEpoch2 = unixEpochFromString("05/08/2019:09:10:40", null);
        long latestEpoch2 = unixEpochFromString("05/10/2022:09:11:40", null);
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory-test\" AND _time >= from_unixtime(" + earliestEpoch2 + ") AND _time <= from_unixtime(" + latestEpoch2 + ") AND host LIKE \"sc-99-99-14-25\" OR host LIKE \"sc-99-99-14-20\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
        result = utils.getQueryAnalysis(q);
        //LOGGER.info("DPL      =<" + q + ">");
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }

    @Disabled
	@Test // disabled on 2022-05-16 TODO Convert to dataframe test
    public void streamList2Test() throws Exception {
        String q, e, result;
        q = "index = memory-test/yyy earliest=\"05/08/2019:09:10:40\" latest=\"05/10/2022:09:11:40\" host=\"sc-99-99-14-25\" OR host=\"sc-99-99-14-20\" sourcetype=\"log:f17:0\" Latitude";
        long earliestEpoch3 = unixEpochFromString("05/08/2019:09:10:40", null);
        long latestEpoch3 = unixEpochFromString("05/10/2022:09:11:40", null);
        e = "SELECT * FROM `temporaryDPLView` WHERE index LIKE \"memory-test/yyy\" AND _time >= from_unixtime(" + earliestEpoch3 + ") AND _time <= from_unixtime(" + latestEpoch3 + ") AND host LIKE \"sc-99-99-14-25\" OR host LIKE \"sc-99-99-14-20\" AND sourcetype LIKE \"log:f17:0\" AND _raw LIKE '%Latitude%'";
        result = utils.getQueryAnalysis(q);
        //LOGGER.info("DPL      =<" + q + ">");
        //utils.printDebug(e,result);
        assertEquals(e, result, q);
    }

}