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

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TeragrepKafkaTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepKafkaTest.class);

    private final String testFile = "src/test/resources/IplocationTransformationTest_data*.jsonl"; // * to make the path into a directory path
    private final StructType testSchema = new StructType(new StructField[] {
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, true, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
    });

    private StreamingTestUtil streamingTestUtil;

    // automatically started and stopped via the annotation
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .registerListener(new PlainListener().onPorts(42649));

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil(this.testSchema);
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();

        // Create config for kafka
        HashMap<String, Object> map = new HashMap<>();
        map
                .put(
                        "dpl.pth_10.transform.teragrep.kafka.save.bootstrap.servers",
                        sharedKafkaTestResource.getKafkaConnectString().split("//")[1]
                );
        map.put("dpl.pth_10.transform.teragrep.kafka.save.security.protocol", "PLAINTEXT");
        map.put("dpl.pth_10.transform.teragrep.kafka.save.sasl.mechanism", "SASL_PLAINTEXT");
        map.put("fs.s3a.access.key", "empty");
        map.put("fs.s3a.secret.key", "empty");
        map.put("fs.s3a.endpoint", "empty");
        map.put("dpl.pth_06.enabled", false);

        // Add config to catalystContext
        Config c = ConfigFactory.parseMap(map);
        this.streamingTestUtil.getCtx().setConfig(c);
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    // test teragrep exec kafka save
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void teragrepKafkaSaveTest() {
        String topic = "topic1";
        String query = "index=index_A | teragrep exec kafka save " + topic;

        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            LOGGER.info("Consumer dataset : <{}>", ds.schema());

            // Create kafka consumer
            try (
                    final KafkaConsumer<String, String> kafkaConsumer = sharedKafkaTestResource
                            .getKafkaTestUtils()
                            .getKafkaConsumer(StringDeserializer.class, StringDeserializer.class)
            ) {

                final List<TopicPartition> topicPartitionList = new ArrayList<>();
                for (final PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
                    topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
                kafkaConsumer.assign(topicPartitionList);
                kafkaConsumer.seekToBeginning(topicPartitionList);

                // Pull records from kafka, keep polling until we get nothing back
                ConsumerRecords<String, String> records;
                int i = 0;
                do {
                    records = kafkaConsumer.poll(2000L);

                    for (ConsumerRecord<String, String> record : records) {
                        // Assert that there are correct values in kafka
                        Assertions
                                .assertTrue(
                                        record.value().contains("\"source\":\"" + "127." + i + "." + i + "." + i + "\"")
                                );
                        i++;
                    }
                }
                while (!records.isEmpty());

                // rows of data saved to kafka
                Assertions.assertEquals(5, i);
            }

            // test the returned dataset
            List<String> offsets = ds
                    .select("offset")
                    .orderBy("id")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> actualOffsets = Arrays.asList("1", "2", "3", "4", "5");
            Assertions.assertEquals(actualOffsets, offsets);
        });
    }

    // test with an aggregation before the command
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void teragrepKafkaSaveTest_aggregation() {
        String topic = "topic2";
        String query = "index=index_A | stats count by offset | teragrep exec kafka save " + topic;

        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            LOGGER.info("Consumer dataset : <{}>", ds.schema());

            // make sure to flush rest of the data after an aggregation
            this.streamingTestUtil.getCtx().flush();

            // Create kafka consumer
            try (
                    final KafkaConsumer<String, String> kafkaConsumer = sharedKafkaTestResource
                            .getKafkaTestUtils()
                            .getKafkaConsumer(StringDeserializer.class, StringDeserializer.class)
            ) {

                final List<TopicPartition> topicPartitionList = new ArrayList<>();
                for (final PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
                    topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
                kafkaConsumer.assign(topicPartitionList);
                kafkaConsumer.seekToBeginning(topicPartitionList);

                // Pull records from kafka, keep polling until we get nothing back
                ConsumerRecords<String, String> records;
                int i = 0;
                do {
                    records = kafkaConsumer.poll(2000L);

                    for (ConsumerRecord<String, String> record : records) {
                        // Assert that there are correct values in kafka (all offsets)
                        for (int j = 1; j < 6; j++) {
                            Assertions.assertTrue(record.value().contains("\"offset\":" + j));
                        }

                        i++;
                    }
                }
                while (!records.isEmpty());

                // rows of data saved to kafka (aggregation should result into one row of data containing an array)
                Assertions.assertEquals(1, i);
            }

            // test the returned dataset
            List<String> offsets = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> actualOffsets = Arrays.asList("1", "2", "3", "4", "5");
            Assertions.assertEquals(actualOffsets, offsets);
        });
    }

    // test with two aggregations before the command
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void teragrepKafkaSaveTest_twoAggregations() {
        String topic = "topic3";
        String query = "index=index_A | stats count by offset | stats count by offset | teragrep exec kafka save "
                + topic;

        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            LOGGER.info("Consumer dataset : <{}>", ds.schema());
            // make sure to flush rest of the data after two aggregations
            this.streamingTestUtil.getCtx().flush();

            // Create kafka consumer
            try (
                    final KafkaConsumer<String, String> kafkaConsumer = sharedKafkaTestResource
                            .getKafkaTestUtils()
                            .getKafkaConsumer(StringDeserializer.class, StringDeserializer.class)
            ) {

                final List<TopicPartition> topicPartitionList = new ArrayList<>();
                for (final PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
                    topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
                kafkaConsumer.assign(topicPartitionList);
                kafkaConsumer.seekToBeginning(topicPartitionList);

                // Pull records from kafka, keep polling until we get nothing back
                ConsumerRecords<String, String> records;
                int i = 0;
                do {
                    records = kafkaConsumer.poll(2000L);

                    for (ConsumerRecord<String, String> record : records) {
                        // Assert that there are correct values in kafka (all offsets)
                        for (int j = 1; j < 6; j++) {
                            Assertions.assertTrue(record.value().contains("\"offset\":" + j));
                        }
                        i++;
                    }
                }
                while (!records.isEmpty());

                // rows of data saved to kafka (aggregation should result into one row of data containing an array)
                Assertions.assertEquals(1, i);
            }

            // test the returned dataset
            List<String> offsets = ds
                    .select("offset")
                    .orderBy("offset")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> actualOffsets = Arrays.asList("1", "2", "3", "4", "5");
            Assertions.assertEquals(actualOffsets, offsets);
        });
    }

    // test with a "sequential_only" before the command
    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void teragrepKafkaSaveTest_sequential() {
        String topic = "topic4";
        String query = "index=index_A | sort num(offset) | teragrep exec kafka save " + topic;

        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            LOGGER.info("Consumer dataset : <{}>", ds.schema());
            // make sure to flush rest of the data after a sequential_only command
            this.streamingTestUtil.getCtx().flush();

            // Create kafka consumer
            try (
                    final KafkaConsumer<String, String> kafkaConsumer = sharedKafkaTestResource
                            .getKafkaTestUtils()
                            .getKafkaConsumer(StringDeserializer.class, StringDeserializer.class)
            ) {

                final List<TopicPartition> topicPartitionList = new ArrayList<>();
                for (final PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
                    topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
                kafkaConsumer.assign(topicPartitionList);
                kafkaConsumer.seekToBeginning(topicPartitionList);

                // Pull records from kafka, keep polling until we get nothing back
                ConsumerRecords<String, String> records;
                int i = 0;
                do {
                    records = kafkaConsumer.poll(2000L);

                    for (ConsumerRecord<String, String> record : records) {
                        // Assert that there are correct values in kafka (test the source column)
                        Assertions
                                .assertTrue(
                                        record.value().contains("\"source\":\"" + "127." + i + "." + i + "." + i + "\"")
                                );
                        i++;
                    }
                }
                while (!records.isEmpty());

                // rows of data saved to kafka
                Assertions.assertEquals(1, i);
            }

            // test the returned dataset
            List<String> offsets = ds
                    .select("offset")
                    .orderBy("id")
                    .collectAsList()
                    .stream()
                    .map(r -> r.getAs(0).toString())
                    .collect(Collectors.toList());
            List<String> actualOffsets = Arrays.asList("1", "2", "3", "4", "5");
            Assertions.assertEquals(actualOffsets, offsets);
        });
    }
}
