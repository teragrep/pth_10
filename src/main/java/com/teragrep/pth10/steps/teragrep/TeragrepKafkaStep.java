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
package com.teragrep.pth10.steps.teragrep;

import com.google.gson.Gson;
import com.teragrep.functions.dpf_02.AbstractStep;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.datasources.S3CredentialWallet;
import com.teragrep.pth10.steps.Flushable;
import com.typesafe.config.Config;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * teragrep exec kafka save: Saves the input dataset to kafka with the specified topic
 */
public final class TeragrepKafkaStep extends AbstractStep implements Flushable {

    private final String hdfsPath;
    private final DPLParserCatalystContext catCtx;
    private final Config zeppelinConfig;
    public String kafkaTopic;

    private DataFrameWriter<Row> dfKafkaWriter;
    private boolean inSequentialMode;

    private final static String FALLBACK_S3_IDENTITY_CONFIG_ITEM = "fs.s3a.access.key";
    private final static String FALLBACK_S3_CREDENTIAL_CONFIG_ITEM = "fs.s3a.secret.key";
    private final static String S3_CREDENTIAL_ENDPOINT_CONFIG_ITEM = "fs.s3a.endpoint";
    private final static String KAFKA_BOOTSTRAP_SERVERS_CONFIG_ITEM = "dpl.pth_10.transform.teragrep.kafka.save.bootstrap.servers";
    private final static String KAFKA_SASL_MECHANISM_CONFIG_ITEM = "dpl.pth_10.transform.teragrep.kafka.save.sasl.mechanism";
    private final static String KAFKA_SECURITY_PROTOCOL_CONFIG_ITEM = "dpl.pth_10.transform.teragrep.kafka.save.security.protocol";
    private final static String DEFAULT_KAFKA_TOPIC_TEMPLATE = "teragrep.%s.%s";

    public TeragrepKafkaStep(
            String hdfsPath,
            DPLParserCatalystContext catCtx,
            Config zeppelinConfig,
            String kafkaTopic
    ) {
        this.hdfsPath = hdfsPath;
        this.catCtx = catCtx;
        this.zeppelinConfig = zeppelinConfig;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void flush() {
        if (this.inSequentialMode && this.aggregatesUsedBefore) {
            // When in sequential mode and aggregates are used, the writer is in complete output mode, meaning that the
            // last version of the whole dataset has to be saved to kafka now.
            try {
                this.dfKafkaWriter.save();
            }
            catch (Exception e) {
                throw new RuntimeException("Error saving dataframe: " + e);
            }
        }
    }

    @Override
    public Dataset<Row> get(Dataset<Row> dataset) throws StreamingQueryException {
        final SparkSession ss = catCtx.getSparkSession();
        final SparkContext sc = ss.sparkContext();
        final S3CredentialWallet wallet = new S3CredentialWallet(sc);
        String identity = wallet.getIdentity();
        String credential = wallet.getCredential();

        if (identity == null || credential == null) {
            // fallback credentials from zeppelin config

            if (zeppelinConfig.hasPath(FALLBACK_S3_IDENTITY_CONFIG_ITEM)) {
                identity = zeppelinConfig.getString(FALLBACK_S3_IDENTITY_CONFIG_ITEM);
                if (identity == null || identity.equals("")) {
                    // exists, but null or empty string
                    throw new RuntimeException("Identity was null");
                }
            }
            else {
                // no config item
                throw new RuntimeException("Missing configuration item: '" + FALLBACK_S3_IDENTITY_CONFIG_ITEM + "'.");
            }

            if (zeppelinConfig.hasPath(FALLBACK_S3_CREDENTIAL_CONFIG_ITEM)) {
                credential = zeppelinConfig.getString(FALLBACK_S3_CREDENTIAL_CONFIG_ITEM);
                if (credential == null || credential.equals("")) {
                    // exists, but null or empty string
                    throw new RuntimeException("Credential was null");
                }
            }
            else {
                // no config item
                throw new RuntimeException("Missing configuration item: '" + FALLBACK_S3_CREDENTIAL_CONFIG_ITEM + "'.");
            }
        }
        else {
            // ignore anything after '@' char in username
            identity = identity.split("@")[0];
        }

        // set jaas config string based on identity & credential
        final String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + identity + "\" password=\"" + credential + "\";";

        if (kafkaTopic == null || kafkaTopic.equals("")) {
            // default kafka topic if not one specified by user
            // format is "teragrep.applicationId.paragraphId"
            kafkaTopic = String.format(DEFAULT_KAFKA_TOPIC_TEMPLATE, sc.applicationId(), catCtx.getParagraphUrl());
        }

        // name used to identify the kafka save streaming query
        final String id = UUID.randomUUID().toString();
        final String kafkaQueryName = "tg-kafka-save-" + id;
        Dataset<Row> toKafkaDs = dataset;

        // add run identifiers to kafka save data
        toKafkaDs = toKafkaDs
                .withColumn("kafkaSave_runTime", functions.current_timestamp())
                .withColumn("kafkaSave_appId", functions.lit(sc.applicationId()));

        if (toKafkaDs.isStreaming()) { // parallel mode
            // Convert each row to a single "value" column as JSON
            toKafkaDs = toKafkaDs
                    .map(
                            (MapFunction<Row, Row>) r -> {
                                // get column names as a scala sequence
                                Seq<String> seqOfColumnNames = JavaConversions
                                        .asScalaBuffer(Arrays.asList(r.schema().fieldNames()));

                                // Get values for each of the columns as a map, and create json out of the map
                                // getValuesMap() returns a Scala map; mapAsJavaMap converts it to a java map which Gson processes correctly
                                String json = new Gson()
                                        .toJson(JavaConversions.mapAsJavaMap(r.getValuesMap(seqOfColumnNames)));

                                // Return final row
                                return RowFactory.create(json);
                            }, RowEncoder.apply(new StructType(new StructField[] {
                                    new StructField("value", DataTypes.StringType, true, new MetadataBuilder().build())
                            }))
                    );
        }
        else { // sequential mode, all as one event
            List<String> jsonList = toKafkaDs.toJSON().collectAsList();
            final String json;

            if (jsonList.size() == 1) {
                json = jsonList.get(0);
            }
            else {
                json = Arrays.toString(jsonList.toArray());
            }

            toKafkaDs = catCtx
                    .getSparkSession()
                    .createDataFrame(Collections.singletonList(RowFactory.create(json)), new StructType(new StructField[] {
                            new StructField("value", DataTypes.StringType, true, new MetadataBuilder().build())
                    }));
        }

        String kafkaBootstrapServers;
        String kafkaSaslMechanism;
        String kafkaSecurityProtocol;
        String s3Endpoint;

        // check config items for nulls
        if (zeppelinConfig.hasPath(KAFKA_BOOTSTRAP_SERVERS_CONFIG_ITEM)) {
            kafkaBootstrapServers = zeppelinConfig.getString(KAFKA_BOOTSTRAP_SERVERS_CONFIG_ITEM);
            if (kafkaBootstrapServers == null || kafkaBootstrapServers.equals("")) {
                // exists, but null or empty string
                throw new RuntimeException("Kafka save bootstrap servers config not properly set.");
            }
        }
        else {
            // config item does not exist at all
            throw new RuntimeException("Missing configuration item: '" + KAFKA_BOOTSTRAP_SERVERS_CONFIG_ITEM + "'.");
        }

        if (zeppelinConfig.hasPath(KAFKA_SASL_MECHANISM_CONFIG_ITEM)) {
            kafkaSaslMechanism = zeppelinConfig.getString(KAFKA_SASL_MECHANISM_CONFIG_ITEM);
            if (kafkaSaslMechanism == null || kafkaSaslMechanism.equals("")) {
                // exists, but null or empty string
                throw new RuntimeException("Kafka save sasl mechanism config not properly set.");
            }
        }
        else {
            // config item does not exist at all
            throw new RuntimeException("Missing configuration item: '" + KAFKA_SASL_MECHANISM_CONFIG_ITEM + "'.");
        }

        if (zeppelinConfig.hasPath(KAFKA_SECURITY_PROTOCOL_CONFIG_ITEM)) {
            kafkaSecurityProtocol = zeppelinConfig.getString(KAFKA_SECURITY_PROTOCOL_CONFIG_ITEM);
            if (kafkaSecurityProtocol == null || kafkaSecurityProtocol.equals("")) {
                // exists, but null or empty string
                throw new RuntimeException("Kafka save security protocol config not properly set.");
            }
        }
        else {
            // config item does not exist at all
            throw new RuntimeException("Missing configuration item: '" + KAFKA_SECURITY_PROTOCOL_CONFIG_ITEM + "'.");
        }

        if (zeppelinConfig.hasPath(S3_CREDENTIAL_ENDPOINT_CONFIG_ITEM)) {
            s3Endpoint = zeppelinConfig.getString(S3_CREDENTIAL_ENDPOINT_CONFIG_ITEM);
            if (s3Endpoint == null || s3Endpoint.equals("")) {
                // exists, but null or empty string
                throw new RuntimeException("S3 endpoint config not properly set.");
            }
        }
        else {
            // config item does not exist at all
            throw new RuntimeException("Missing configuration item: '" + S3_CREDENTIAL_ENDPOINT_CONFIG_ITEM + "'.");
        }

        if (!toKafkaDs.isStreaming()) {
            // in sequential mode
            this.inSequentialMode = true;
            this.dfKafkaWriter = toKafkaDs
                    .write()
                    .format("kafka")
                    .option("checkpointLocation", hdfsPath + kafkaQueryName)
                    .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                    .option("topic", kafkaTopic)
                    .option("S3endPoint", s3Endpoint)
                    .option("S3identity", identity)
                    .option("S3credential", credential)
                    .option("kafka.enabled", "true")
                    .option("kafka.sasl.jaas.config", jaasConfig)
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("kafka.sasl.mechanism", kafkaSaslMechanism)
                    .option("kafka.security.protocol", kafkaSecurityProtocol);

            if (!this.aggregatesUsedBefore) {
                this.dfKafkaWriter.mode(SaveMode.Append);
                this.dfKafkaWriter.save();
            }
        }
        else {
            // configure DataStreamWriter for kafka save, and start it with listener to check for completion
            DataStreamWriter<Row> kafkaWriter = toKafkaDs
                    .writeStream()
                    .format("kafka")
                    .option("checkpointLocation", hdfsPath + kafkaQueryName)
                    .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                    .option("topic", kafkaTopic)
                    .option("S3endPoint", s3Endpoint)
                    .option("S3identity", identity)
                    .option("S3credential", credential)
                    .option("kafka.enabled", "true")
                    .option("kafka.sasl.jaas.config", jaasConfig)
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("kafka.sasl.mechanism", kafkaSaslMechanism)
                    .option("kafka.security.protocol", kafkaSecurityProtocol);

            if (catCtx.getStepList().getAggregateCount() > 0) {
                kafkaWriter.outputMode(OutputMode.Complete());
            }
            else {
                // should always be this, since aggregates should be in sequential
                kafkaWriter.outputMode(OutputMode.Append());
            }

            StreamingQuery kafkaQuery = catCtx
                    .getInternalStreamingQueryListener()
                    .registerQuery(kafkaQueryName, kafkaWriter);

            // Await for the listener to send the stop signal
            kafkaQuery.awaitTermination();

        }
        return dataset;
    }
}
