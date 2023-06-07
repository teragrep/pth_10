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

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.ast.DPLParserCatalystVisitor;
import com.teragrep.pth10.ast.TreeUtils;
import com.teragrep.pth10.ast.bo.CatalystNode;
import com.teragrep.pth_03.antlr.DPLLexer;
import com.teragrep.pth_03.antlr.DPLParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the new ProcessingStack implementation
 * Uses streaming datasets
 * @author p000043u
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EarliestLatestTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarliestLatestTest.class);

    DPLParserCatalystContext ctx = null;
    DPLParserCatalystVisitor visitor = null;

    SparkSession sparkSession = null;
    SQLContext sqlContext = null;

    ExpressionEncoder<Row> encoder = null;
    MemoryStream<Row> rowMemoryStream = null;
    Dataset<Row> rowDataset = null;

    private static final StructType testSchema = new StructType(
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

    @org.junit.jupiter.api.BeforeAll
    void setEnv() {

    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .config("checkpointLocation","/tmp/pth_10/test/earliestlatest/checkpoints/" + UUID.randomUUID() + "/")
                .getOrCreate();

        sqlContext = sparkSession.sqlContext();
        ctx = new DPLParserCatalystContext(sparkSession);

        //sparkSession.sparkContext().setLogLevel("ERROR");

        encoder = RowEncoder.apply(testSchema);
        rowMemoryStream =
                new MemoryStream<>(1, sqlContext, encoder);

        // Create a spark structured streaming dataset and start writing the stream
        rowDataset = rowMemoryStream.toDS();
        ctx.setDs(rowDataset);	// for stream ds
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        visitor = null;
    }


    // ----------------------------------------
    // Tests
    // ----------------------------------------

    /*
        Generated data:
        +-------------------+---+---------+--------+----------+------------+-------+---------+------+
        |_time              |id |_raw     |index   |sourcetype|host        |source |partition|offset|
        +-------------------+---+---------+--------+----------+------------+-------+---------+------+
        |2013-07-15 10:01:50|1  |data data|haproxy |stream1   |webserver.example.com|127.0.0.0|0        |1     |
        |2013-07-15 10:01:50|2  |data data|rsyslogd|stream2   |webserver.example.com|127.1.1.1|0        |2     |
        |2014-07-15 10:01:50|3  |data data|haproxy |stream1   |webserver.example.com|127.2.2.2|0        |3     |
        |2014-07-15 10:01:50|4  |data data|rsyslogd|stream2   |webserver.example.com|127.3.3.3|0        |4     |
        |2015-07-15 10:01:50|5  |data data|haproxy |stream1   |webserver.example.com|127.4.4.4|0        |5     |
        |2015-07-15 10:01:50|6  |data data|rsyslogd|stream2   |webserver.example.com|127.5.5.5|0        |6     |
        |2016-07-14 10:01:50|7  |data data|haproxy |stream1   |webserver.example.com|127.6.6.6|0        |7     |
        |2016-07-14 10:01:50|8  |data data|rsyslogd|stream2   |webserver.example.com|127.7.7.7|0        |8     |
        |2017-07-14 10:01:50|9  |data data|haproxy |stream1   |webserver.example.com|127.8.8.8|0        |9     |
        |2017-07-14 10:01:50|10 |data data|rsyslogd|stream2   |webserver.example.com|127.9.9.9|0        |10    |
        +-------------------+---+---------+--------+----------+------------+-------+---------+------+
     */

    // Remove '' to run the tests.
    // TODO Make sure the assertions are correct.
    // FIXME: earliest-latest defaulting removed to fix issue #351
    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest1() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "index=haproxy earliest=-10y OR index=rsyslogd",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertFalse(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest2() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "index=haproxy OR index=rsyslogd | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertTrue(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest3() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "index=haproxy OR index=rsyslogd earliest=-10y | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertTrue(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest4() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-10y index=haproxy OR index=rsyslogd | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertTrue(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest5() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-10y index=haproxy OR (index=rsyslogd latest=-10y) | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertFalse(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest6() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-10y index=haproxy OR index=rsyslogd latest=-10y | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertFalse(indexAsList.contains("haproxy"));
                    assertFalse(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest7() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-10y index=haproxy OR index=rsyslogd latest=-1y | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertTrue(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest8() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-20y index=haproxy OR index=rsyslogd earliest=-1d | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertFalse(indexAsList.contains("haproxy"));
                    assertFalse(indexAsList.contains("rsyslogd"));
                }
        );
    }

    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest9() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-20y index=haproxy OR index=rsyslogd earliest=now | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertFalse(indexAsList.contains("haproxy"));
                    assertFalse(indexAsList.contains("rsyslogd"));
                }
        );
    }
    
	@Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void earliestLatestTest10() throws StreamingQueryException, InterruptedException {
        performStreamingDPLTest(
                "earliest=-20y index=haproxy OR index=rsyslogd latest=now | stats count(_raw) by index",
                null, //"[_time, _time2]",
                ds -> {
                    List<String> indexAsList = ds.select("index").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertTrue(indexAsList.contains("haproxy"));
                    assertTrue(indexAsList.contains("rsyslogd"));
                }
        );
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultFormatTest() throws StreamingQueryException, InterruptedException{ // MM/dd/yyyy:HH:mm:ss
        performStreamingDPLTest("(index=haproxy OR index=rsyslogd) AND earliest=03/15/2014:00:00:00",
                null,
                ds -> {
                    List<String> time = ds.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals("2014-04-15 08:23:17.0", time.get(0));
                    assertEquals("2014-03-15 21:54:14.0", time.get(1));

                });
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void ISO8601FormatTest() throws StreamingQueryException, InterruptedException{ // '2011-12-03T10:15:30+01:00'
        performStreamingDPLTest("(index=haproxy OR index=rsyslogd) AND earliest=2014-03-15T00:00:00+03:00",
                null,
                ds -> {
                    List<String> time = ds.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals("2014-04-15 08:23:17.0", time.get(0));
                    assertEquals("2014-03-15 21:54:14.0", time.get(1));

                });
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest1() throws StreamingQueryException, InterruptedException{ // '2011-12-03T10:15:30+01:00'
        ctx.setDplDefaultEarliest(-1L);
        ctx.setDplDefaultLatest(1L);
        performStreamingDPLTest("(index=haproxy OR index=rsyslogd)",
                null,
                ds -> {
                    // empty
                });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"haproxy\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"rsyslogd\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^haproxy AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^rsyslogd AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))))";



        assertEquals(expXml, ctx.getArchiveQuery());
        assertEquals(expSpark, ctx.getSparkQuery());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest2() throws StreamingQueryException, InterruptedException{ // '2011-12-03T10:15:30+01:00'
        ctx.setDplDefaultEarliest(-1L);
        ctx.setDplDefaultLatest(1L);
        performStreamingDPLTest("(index=haproxy earliest=2014-03-15T21:54:14+02:00) OR index=rsyslogd)",
                null,
                ds -> {
                    // empty
                });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"haproxy\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1394913254\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"rsyslogd\"/><earliest operation=\"GE\" value=\"-1\"/></AND><latest operation=\"LE\" value=\"1\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^haproxy AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1394913254, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^rsyslogd AND (_time >= from_unixtime(-1, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expXml, ctx.getArchiveQuery());
        assertEquals(expSpark, ctx.getSparkQuery());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest3() throws StreamingQueryException, InterruptedException{ // '2011-12-03T10:15:30+01:00'
        ctx.setDplDefaultEarliest(0L);
        ctx.setDplDefaultLatest(1678713103L);
        performStreamingDPLTest("(index=haproxy earliest=2014-03-15T21:54:14+02:00) OR (index=rsyslogd earliest=2014-04-15T08:23:17+02:00))",
                null,
                ds -> {
                    List<String> time = ds.select("_time").collectAsList().stream().map(r -> r.getAs(0).toString()).collect(Collectors.toList());

                    assertEquals(2, time.size());
                    assertEquals("2014-04-15 08:23:17.0", time.get(0));
                    assertEquals("2014-03-15 21:54:14.0", time.get(1));

                });

        final String expXml = "<OR><AND><AND><index operation=\"EQUALS\" value=\"haproxy\"/><latest operation=\"LE\" value=\"1678713103\"/></AND><earliest operation=\"GE\" value=\"1394913254\"/></AND><AND><AND><index operation=\"EQUALS\" value=\"rsyslogd\"/><latest operation=\"LE\" value=\"1678713103\"/></AND><earliest operation=\"GE\" value=\"1397539397\"/></AND></OR>";
        final String expSpark = "(((index RLIKE (?i)^haproxy AND (_time <= from_unixtime(1678713103, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1394913254, yyyy-MM-dd HH:mm:ss))) OR ((index RLIKE (?i)^rsyslogd AND (_time <= from_unixtime(1678713103, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1397539397, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expXml, ctx.getArchiveQuery());
        assertEquals(expSpark, ctx.getSparkQuery());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
   public void defaultEarliestLatestTest4() throws StreamingQueryException, InterruptedException {
        // set defaults
        ctx.setDplDefaultEarliest(0L);
        ctx.setDplDefaultLatest(1678711652L);

        // query, assertions
        performStreamingDPLTest("(earliest=1900-01-01T00:00:00Z latest=1960-01-01T00:00:00Z) OR ((index=rsyslogd earliest=1970-01-01T00:00:00Z latest=2100-01-01T00:00:00Z)) OR index=haproxy earliest=1950-01-01T00:00:00Z",
                null,
                ds -> {
                    //empty
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
                        "<comparisonstatement field=\"index\" operation=\"=\" value=\"rsyslogd\"/>" +
                         "<earliest operation=\"GE\" value=\"-7200\"/>" +
                     "</AND>" +
                     "<latest operation=\"LE\" value=\"4102437600\"/>" +
                    "</AND>" +
                    "</OR>" +
                    "<AND>" +
                     "<comparisonstatement field=\"index\" operation=\"=\" value=\"haproxy\"/>" +
                     "<latest operation=\"LE\" value=\"1678711652\"/>" +
                    "</AND>" +
                "</OR>" +
                "<earliest operation=\"GE\" value=\"-631159200\"/>" +
                "</AND>";
        final String expectedSpark = "(((((_time >= from_unixtime(-2208994789, yyyy-MM-dd HH:mm:ss)) AND (_time <= from_unixtime(-315626400, yyyy-MM-dd HH:mm:ss)))" +
                " OR ((index RLIKE ^rsyslogd$ AND (_time >= from_unixtime(-7200, yyyy-MM-dd HH:mm:ss))) AND (_time <= from_unixtime(4102437600, yyyy-MM-dd HH:mm:ss))))" +
                " OR (index RLIKE ^haproxy$ AND (_time <= from_unixtime(1678711652, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(-631159200, yyyy-MM-dd HH:mm:ss)))";

        assertEquals(expectedSpark, this.visitor.getCatalystContext().getSparkQuery());
        assertEquals(expectedXml, this.visitor.getCatalystContext().getArchiveQuery());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest5() throws StreamingQueryException, InterruptedException {
        // set defaults
        ctx.setDplDefaultEarliest(0L);
        ctx.setDplDefaultLatest(1L);

        // query, assertions
        performStreamingDPLTest("index=haproxy OR index=rsyslogd earliest=2020-01-01T00:00:00Z",
                null,
                ds -> {
                    //empty
                });


        final String expectedXml = "<AND><OR><AND><index operation=\"EQUALS\" value=\"haproxy\"/><latest operation=\"LE\" value=\"1\"/></AND><AND><index operation=\"EQUALS\" value=\"rsyslogd\"/><latest operation=\"LE\" value=\"1\"/></AND></OR><earliest operation=\"GE\" value=\"1577829600\"/></AND>";
        final String expectedSpark = "(((index RLIKE (?i)^haproxy AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) OR (index RLIKE (?i)^rsyslogd AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(1577829600, yyyy-MM-dd HH:mm:ss)))";

        assertEquals(expectedSpark, this.visitor.getCatalystContext().getSparkQuery());
        assertEquals(expectedXml, this.visitor.getCatalystContext().getArchiveQuery());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest6() throws StreamingQueryException, InterruptedException {
        // set defaults
        ctx.setDplDefaultEarliest(0L);
        ctx.setDplDefaultLatest(1L);

        // query, assertions
        performStreamingDPLTest("index=rsyslogd earliest=1970-01-01T00:00:00.000+02:00 OR " +
                        "index=haproxy earliest=2010-12-31T00:00:00.000+02:00",
                null,
                ds -> {
                    //empty
                });

       final String expectedXml = "<AND><AND><index operation=\"EQUALS\" value=\"rsyslogd\"/><latest operation=\"LE\" value=\"1\"/></AND><AND><OR><earliest operation=\"GE\" value=\"-7200\"/><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"haproxy\"/><latest operation=\"LE\" value=\"1\"/></AND></OR><earliest operation=\"GE\" value=\"1293746400\"/></AND></AND>";
       final String expectedSpark = "((index RLIKE (?i)^rsyslogd AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (((_time >= from_unixtime(-7200, yyyy-MM-dd HH:mm:ss)) OR (index RLIKE ^haproxy$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss)))) AND (_time >= from_unixtime(1293746400, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expectedSpark, this.visitor.getCatalystContext().getSparkQuery());
        assertEquals(expectedXml, this.visitor.getCatalystContext().getArchiveQuery());
    }


    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest7() throws StreamingQueryException, InterruptedException {
        // set defaults
        ctx.setDplDefaultEarliest(0L);
        ctx.setDplDefaultLatest(1L);

        // query, assertions
        performStreamingDPLTest("(index=haproxy earliest=2019-01-01T00:00:00Z) AND (index=rsyslogd) earliest=2009-01-01T00:00:00Z",
                null,
                ds -> {
                    //empty
                });

        final String expectedXml = "<AND><AND><AND><index operation=\"EQUALS\" value=\"haproxy\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1546293600\"/></AND><AND><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"rsyslogd\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1230760800\"/></AND></AND>";
        final String expectedSpark = "(((index RLIKE (?i)^haproxy AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1546293600, yyyy-MM-dd HH:mm:ss))) AND ((index RLIKE ^rsyslogd$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1230760800, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expectedSpark, this.visitor.getCatalystContext().getSparkQuery());
        assertEquals(expectedXml, this.visitor.getCatalystContext().getArchiveQuery());
    }

    @Test
    @EnabledIfSystemProperty(named="runSparkTest", matches="true")
    public void defaultEarliestLatestTest8() throws StreamingQueryException, InterruptedException {
        // set defaults
        ctx.setDplDefaultEarliest(0L);
        ctx.setDplDefaultLatest(1L);

        // query, assertions
        performStreamingDPLTest("(index=haproxy earliest=2019-01-01T00:00:00Z) AND (index=rsyslogd) earliest=2009-01-01T00:00:00Z",
                null,
                ds -> {
                    //empty
                });

        final String expectedXml = "<AND><AND><AND><index operation=\"EQUALS\" value=\"haproxy\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1546293600\"/></AND><AND><AND><comparisonstatement field=\"index\" operation=\"=\" value=\"rsyslogd\"/><latest operation=\"LE\" value=\"1\"/></AND><earliest operation=\"GE\" value=\"1230760800\"/></AND></AND>";
        final String expectedSpark = "(((index RLIKE (?i)^haproxy AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1546293600, yyyy-MM-dd HH:mm:ss))) AND ((index RLIKE ^rsyslogd$ AND (_time <= from_unixtime(1, yyyy-MM-dd HH:mm:ss))) AND (_time >= from_unixtime(1230760800, yyyy-MM-dd HH:mm:ss))))";

        assertEquals(expectedSpark, this.visitor.getCatalystContext().getSparkQuery());
        assertEquals(expectedXml, this.visitor.getCatalystContext().getArchiveQuery());
    }



    // ----------------------------------------
    // Helper methods
    // ----------------------------------------


    private void performStreamingDPLTest(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) throws StreamingQueryException, InterruptedException {

        // the names of the queries for source and result
        final String nameOfSourceStream = "pth10_earliestlatest_test_src";

        // start streams for rowDataset (source)
        StreamingQuery sourceStreamingQuery = startStream(rowDataset, "append", nameOfSourceStream);
        sourceStreamingQuery.processAllAvailable();


        // listener for source stream, this allows stopping the stream when all processing is done
        // without the use of thread.sleep()
        sparkSession.streams().addListener(new StreamingQueryListener() {
            int noProgress = 0;
            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
                LOGGER.info("Query started: " + queryStarted.id());
            }
            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                LOGGER.info("Query terminated: " + queryTerminated.id());
            }
            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
                Double progress = queryProgress.progress().processedRowsPerSecond();
                String nameOfStream = queryProgress.progress().name();

                LOGGER.info("Processed rows/sec: " + progress);
                // Check if progress has stopped (progress becomes NaN at the end)
                // and end the query if that is the case
                if (Double.isNaN(progress) && nameOfStream == nameOfSourceStream) {
                    noProgress++;
                    if (noProgress > 1) {
                        LOGGER.info("No progress on source stream");

                        sourceStreamingQuery.processAllAvailable();
                        sourceStreamingQuery.stop();
                        sparkSession.streams().removeListener(this);
                    }

                }

            }
        }); // end listener


        // Keep adding data to stream until enough runs are done
        long run = 0L, counter = 0L, id = 0L;
        long maxCounter = 10L, maxRuns = 1L; /* You can use these two variables to customize the amount of data */
        long timeEpoch = 1_373_882_510L;
        final long monthEpoch = 2_629_743L;
        boolean evenRun = false;
        while (sourceStreamingQuery.isActive()) {

            if (run < maxRuns) {
                rowMemoryStream.addData(
                        makeRows(
                                Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochSecond(timeEpoch), ZoneOffset.UTC)),    // _time
                                ++id, 					                            // id
                                "data data", 			                            // _raw
                                (evenRun ? "rsyslogd" : "haproxy"), 		        // index
                                "stream" + (counter % 2 == 0 ? 1 : 2), 				// sourcetype
                                "webserver.example.com", 		                            // host
                                counter + "." + counter + "." + counter + "." + counter,	// source
                                String.valueOf(run), 	                            // partition
                                ++counter, 				                            // offset
                                1 //500 				                            // make n amount of rows
                        )
                );
                timeEpoch += monthEpoch; // add 1 month
                evenRun = !evenRun;
            }

            // Run $run times, each with $counter makeRows()
            if (counter == maxCounter) {
                run++;
                counter = 0;
            }

        }

        sourceStreamingQuery.awaitTermination(); // wait for query to be over

        // Got streamed data
        boolean truncateFields = false;
        int numRowsToPreview = 25;

        // Print previews of dataframes
        LOGGER.info(" -- Source data -- ");
        Dataset<Row> df = sqlContext.sql("SELECT * FROM " + nameOfSourceStream);
        df.show(numRowsToPreview, truncateFields);

        // Start performing the dpl query
        performDPLQuery(query, expectedColumns, assertConsumer);

    }

    // Starts the stream for streaming dataframe rowDataset in outputMode and sets the queryName
    private StreamingQuery startStream(Dataset<Row> rowDataset, String outputMode, String queryName) {
        return rowDataset
                .writeStream()
                .outputMode(outputMode)
                .format("memory")
                .queryName(queryName)
                .start();
    }

    // Performs given DPL query and returns result dataset<row>
    private Dataset<Row> performDPLQuery(String query, String expectedColumns, Consumer<Dataset<Row>> assertConsumer) {
        LOGGER.info("-> Got DPL query: " + query);

        ctx.setEarliest("-1Y");

        // Visit the parse tree
        visitor = new DPLParserCatalystVisitor(ctx);
        CharStream inputStream = CharStreams.fromString(query);
        DPLLexer lexer = new DPLLexer(inputStream);
        DPLParser parser = new DPLParser(new CommonTokenStream(lexer));
        ParseTree tree = parser.root();

        System.out.println("Parse tree for this query --- " + query);
        System.out.println(TreeUtils.toPrettyTree(tree, Arrays.asList(parser.getRuleNames())));

        // set path for join cmd
        visitor.setHdfsPath("/tmp/pth_10/" + UUID.randomUUID());

        // Set consumer for testing
        visitor.setConsumer(ds -> {
            LOGGER.info("Batch handler consumer called for ds with schema: " + ds.schema());
            ds.show(50, false);
            if (expectedColumns != null) assertEquals(expectedColumns, Arrays.toString(ds.columns()), "Batch handler dataset contained an unexpected column arrangement !");
            if (assertConsumer != null) assertConsumer.accept(ds); // more assertions, if any
        });

        assertTrue(visitor.getConsumer() != null, "Consumer was not properly registered to visitor !");
        CatalystNode n = (CatalystNode) visitor.visit(tree);
        DataStreamWriter<Row> dsw = n.getDataStreamWriter();

        assertTrue(dsw != null || !n.getDataset().isStreaming(), "DataStreamWriter was not returned from visitor !");
        if (dsw != null) {
            // process forEachBatch
            StreamingQuery sq = dsw.start();
            sq.processAllAvailable();
        }

        return n.getDataset();
    }

    // Make rows of given amount
    private Seq<Row> makeRows(Timestamp _time, Long id, String _raw, String index, String sourcetype, String host, String source, String partition, Long offset, long amount) {
        ArrayList<Row> rowArrayList = new ArrayList<>();
        Row row = RowFactory.create(_time, id, _raw, index, sourcetype, host, source, partition, offset);

        while (amount > 0) {
            rowArrayList.add(row);
            amount--;
        }

        Seq<Row> rowSeq = JavaConverters.asScalaIteratorConverter(rowArrayList.iterator()).asScala().toSeq();
        return rowSeq;
    }

    // generate number between a - b
    private int rng(int a, int b) {
        return new Random().nextInt(b-a) + a;
    }

}
