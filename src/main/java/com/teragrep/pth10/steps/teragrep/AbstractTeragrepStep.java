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

package com.teragrep.pth10.steps.teragrep;

import com.teragrep.pth10.ast.DPLParserCatalystContext;
import com.teragrep.pth10.steps.AbstractStep;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class AbstractTeragrepStep extends AbstractStep {
    public enum TeragrepCommandMode {
        EXEC_SYSLOG_STREAM,
        GET_TERAGREP_VERSION,
        EXEC_HDFS_SAVE,
        EXEC_HDFS_LOAD,
        EXEC_HDFS_LIST,
        EXEC_HDFS_DELETE,
        EXEC_KAFKA_SAVE,
        EXEC_BLOOM_UPDATE,
        EXEC_BLOOM_CREATE
    }

    // consts
    protected final static String FALLBACK_S3_IDENTITY_CONFIG_ITEM = "fs.s3a.access.key";
    protected final static String FALLBACK_S3_CREDENTIAL_CONFIG_ITEM = "fs.s3a.secret.key";
    protected final static String S3_CREDENTIAL_ENDPOINT_CONFIG_ITEM = "fs.s3a.endpoint";
    protected final static String KAFKA_BOOTSTRAP_SERVERS_CONFIG_ITEM = "dpl.pth_10.transform.teragrep.kafka.save.bootstrap.servers";
    protected final static String KAFKA_SASL_MECHANISM_CONFIG_ITEM = "dpl.pth_10.transform.teragrep.kafka.save.sasl.mechanism";
    protected final static String KAFKA_SECURITY_PROTOCOL_CONFIG_ITEM = "dpl.pth_10.transform.teragrep.kafka.save.security.protocol";
    protected final static String DATABASE_USERNAME_CONFIG_ITEM = "dpl.pth_06.archive.db.username";
    protected final static String DATABASE_PASSWORD_CONFIG_ITEM = "dpl.pth_06.archive.db.password";
    protected final static String DEFAULT_KAFKA_TOPIC_TEMPLATE = "teragrep.%s.%s";

    // Bloom filter consts
    protected final static String DATABASE_DEFAULT_URL_CONFIG_ITEM = "dpl.archive.db.bloomdb.url";
    protected final static String SMALL_FILTER_TABLE_NAME = "filter_expected_100000_fpp_001";
    protected final static String MEDIUM_FILTER_TABLE_NAME = "filter_expected_1000000_fpp_003";
    protected final static String LARGE_FILTER_TABLE_NAME = "filter_expected_2500000_fpp_005";
    protected final static int SMALL_EXPECTED_NUM_ITEMS = 100000;
    protected final static int MEDIUM_EXPECTED_NUM_ITEMS = 1000000;
    protected final static int LARGE_EXPECTED_NUM_ITEMS = 2500000;
    protected final static Double SMALL_FALSE_POSITIVE_PROBABILITY = 0.01;
    protected final static Double MEDIUM_FALSE_POSITIVE_PROBABILITY = 0.03;
    protected final static Double LARGE_FALSE_POSITIVE_PROBABILITY = 0.05;

    // params for different teragrep subcommands
    protected String host = "127.0.0.1";
    protected int port = 601;
    protected String path = null;
    protected String kafkaTopic = null;
    protected String retentionSpan = null;
    protected boolean overwrite = false;
    protected TeragrepCommandMode cmdMode = null;
    protected boolean aggregatesUsed = false;
    protected DPLParserCatalystContext catCtx = null;
    protected Config zeppelinConfig = null;
    protected String hdfsPath = null;

    public AbstractTeragrepStep(Dataset<Row> dataset) {
        super(dataset);
    }

    public void setCmdMode(TeragrepCommandMode cmdMode) {
        this.cmdMode = cmdMode;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public void setRetentionSpan(String retentionSpan) {
        this.retentionSpan = retentionSpan;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setAggregatesUsed(boolean aggregatesUsed) {
        this.aggregatesUsed = aggregatesUsed;
    }

    public void setCatCtx(DPLParserCatalystContext catCtx) {
        this.catCtx = catCtx;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public void setZeppelinConfig(Config zeppelinConfig) {
        this.zeppelinConfig = zeppelinConfig;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }


    public String getRetentionSpan() {
        return retentionSpan;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public String getHost() {
        return host;
    }

    public TeragrepCommandMode getCmdMode() {
        return cmdMode;
    }

    public DPLParserCatalystContext getCatCtx() {
        return catCtx;
    }

    public Config getZeppelinConfig() {
        return zeppelinConfig;
    }

    public boolean isAggregatesUsed() {
        return aggregatesUsed;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }
}
