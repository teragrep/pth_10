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

package com.teragrep.pth10.datasources;

import com.teragrep.jue_01.GlobToRegEx;
import com.teragrep.pth06.ArchiveSourceProvider;
import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.typesafe.config.Config;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * DPL Datasource, used for archive and kafka queries
 */
public class DPLDatasource {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLDatasource.class);

    private final Config config;
    private final DPLParserCatalystContext catCtx;
    private final SparkSession sparkSession;

    public DPLDatasource(Config config, SparkSession session) {
        this.config = config;
        this.sparkSession = session;
        this.catCtx = null;
    }

    public DPLDatasource(DPLParserCatalystContext catCtx) {
        this.config = catCtx.getConfig();
        this.sparkSession = catCtx.getSparkSession();
        this.catCtx = catCtx;
    }


    public Dataset<Row> constructStreams(String archiveQuery) {
        LOGGER.info("DPL-interpreter initialize Archive stream");

        // resolve archive Query which is then used with archiveDatasource
        LOGGER.info("DPL-interpreter archiveQuery = " + archiveQuery);

        Dataset<Row> archiveDS = null;    // Archive-stream
        LOGGER.info("Construct-stream config=" + config);
        // table name per paragraph
        if (config.getBoolean("dpl.pth_06.enabled")) {
            LOGGER.info("Archive query used:" + archiveQuery);
            // Execute search against ArchiveDataSource
            archiveDS = archiveStreamConsumerDataset(archiveQuery);
        }

        LOGGER.info("Archive dataset: " + archiveDS);
        return archiveDS;

    }


    /**
     * Setup source stream for query
     * @param query
     * @return streaming dataset
     */
    private Dataset<Row> archiveStreamConsumerDataset(String query) {
        DataStreamReader reader;

        LOGGER.info("ArchiveStreamConsumerDatasource initialized with query:" + query);

        // setup s3 credentials
        final SparkContext sc = sparkSession.sparkContext();
        final S3CredentialWallet s3CredentialWallet = new S3CredentialWallet(sc);
        String s3identity = s3CredentialWallet.getIdentity();
        String s3credential = s3CredentialWallet.getCredential();

        // setup fallback for globally configured credential
        if (s3identity == null || s3credential == null) {
            s3identity = config.getString("fs.s3a.access.key");
            s3credential = config.getString("fs.s3a.secret.key");
        }



        reader = sparkSession
                    .readStream()
                    .format("com.teragrep.pth06.ArchiveSourceProvider")
                    .option("num_partitions", config.getString("dpl.pth_06.partitions"))
                    .option("S3endPoint", config.getString("fs.s3a.endpoint"))
                    .option("S3identity", s3identity)
                    .option("S3credential", s3credential)
                    .option("DBusername", config.getString("dpl.pth_06.archive.db.username"))
                    .option("DBpassword", config.getString("dpl.pth_06.archive.db.password"))
                    .option("DBurl", config.getString("dpl.pth_06.archive.db.url"))
                    .option("DBstreamdbname", config.getString("dpl.pth_06.archive.db.streamdb.name"))
                    .option("DBjournaldbname", config.getString("dpl.pth_06.archive.db.journaldb.name"))
                    .option("hideDatabaseExceptions", config.getString("dpl.pth_06.archive.db.hideDatabaseExceptions"))
                    .option("skipNonRFC5424Files", config.getString("dpl.pth_06.archive.s3.skipNonRFC5424Files"))
                    .option("queryXML", query);
        // Add auditInformation options if exists
        if( catCtx != null && catCtx.getAuditInformation() != null) {
            reader = reader
                    .option("TeragrepAuditQuery", catCtx.getAuditInformation().getQuery())
                    .option("TeragrepAuditReason", catCtx.getAuditInformation().getReason())
                    .option("TeragrepAuditUser", catCtx.getAuditInformation().getUser())
                    .option("TeragrepAuditPluginClassName", catCtx.getAuditInformation().getTeragrepAuditPluginClassName());
        }

        if (config.getBoolean("dpl.pth_06.archive.enabled")) {
            reader = reader
                    .option("archive.enabled", "true");
        }
        else {
            reader = reader
                    .option("archive.enabled", "false");
        }

        if (config.getBoolean("dpl.pth_06.kafka.enabled")) {
            String s3identityWithoutDomain = s3identity;

            int domainIndex = s3identityWithoutDomain.indexOf("@");
            if (domainIndex != -1) {
                // found
                s3identityWithoutDomain = s3identityWithoutDomain.substring(0, domainIndex);
            }
            String jaasconfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+s3identityWithoutDomain+"\" password=\""+s3credential+"\";";

            reader = reader
                    .option("kafka.enabled", "true")
                    .option("kafka.bootstrap.servers", config.getString("dpl.pth_06.kafka.bootstrap.servers"))
                    .option("kafka.sasl.mechanism", config.getString("dpl.pth_06.kafka.sasl.mechanism"))
                    .option("kafka.security.protocol", config.getString("dpl.pth_06.kafka.security.protocol"))
                    .option("kafka.sasl.jaas.config", jaasconfig)
                    .option("kafka.max.poll.records", config.getString("dpl.pth_06.kafka.max.poll.records"))
                    .option("kafka.fetch.max.bytes", config.getString("dpl.pth_06.kafka.fetch.max.bytes"))
                    .option("kafka.fetch.max.wait.ms", config.getString("dpl.pth_06.kafka.fetch.max.wait.ms"))
                    .option("kafka.max.partition.fetch.bytes", config.getString("dpl.pth_06.kafka.max.partition.fetch.bytes"))
                    .option("kafka.continuousProcessing", config.getString("dpl.pth_06.kafka.continuousProcessing"));
        }

        // transition time from kafka to archive
        if (config.getBoolean("dpl.pth_06.transition.enabled")) {
            long transitionHoursAgo = config.getLong("dpl.pth_06.transition.hoursago");
            long transitionSecondsAgo = transitionHoursAgo * 3600;
            Instant now = Instant.now();
            long epochAtHoursAgo = now.minusSeconds(transitionSecondsAgo).getEpochSecond();
            long epochHour = epochAtHoursAgo - epochAtHoursAgo % 3600;

            reader = reader
                    .option("archive.includeBeforeEpoch", String.valueOf(epochHour))
                    .option("kafka.includeEpochAndAfter", String.valueOf(epochHour));
        }

        return reader.load();
    }

}
