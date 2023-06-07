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

import com.teragrep.pth10.ast.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TeragrepStep extends AbstractTeragrepStep{
    private static final Logger LOGGER = LoggerFactory.getLogger(TeragrepStep.class);
    public TeragrepStep(Dataset<Row> dataset) {
        super(dataset);
    }

    @Override
    public Dataset<Row> get() {
        if (this.dataset == null) {
            return null;
        }

        // Perform different subcommands based on the given command mode
        Dataset<Row> rv;
        if (this.cmdMode == TeragrepCommandMode.GET_TERAGREP_VERSION) {
            rv = TeragrepInfoOperations.getTeragrepVersionAsDataframe(this.dataset, this.catCtx);
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_SYSLOG_STREAM) {
            if (aggregatesUsed) {
                throw new UnsupportedOperationException("Syslog streaming is not yet supported with aggregated datasets.");
            } else {
                rv = TeragrepSyslogOperations.sendDataframeAsSyslog(this.dataset, this.host, this.port);
            }
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_HDFS_SAVE) {
            rv = TeragrepHdfsOperations.saveDataToHdfs(this.dataset, this.catCtx, this.overwrite,
                    this.path, this.retentionSpan, "hdfs-save");
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_HDFS_LOAD) {
            rv = TeragrepHdfsOperations.loadDataFromHdfs(this.catCtx, this.path);
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_HDFS_LIST) {
            rv = TeragrepHdfsOperations.listDataFromHdfs(this.catCtx, this.path);
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_HDFS_DELETE) {
            rv = TeragrepHdfsOperations.deleteDataFromHdfs(this.catCtx, this.path);
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_KAFKA_SAVE) {
            rv = TeragrepKafkaOperations.saveDataToKafka(this.dataset, this.hdfsPath,
                    this.catCtx, this.zeppelinConfig, this.kafkaTopic);
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_BLOOM_CREATE) {
            rv = TeragrepBloomFilterOperations.createBloomFilter(this.dataset, this.catCtx, this.zeppelinConfig);
        }
        else if (this.cmdMode == TeragrepCommandMode.EXEC_BLOOM_UPDATE) {
            rv = TeragrepBloomFilterOperations.updateBloomFilter(this.dataset, this.catCtx, this.zeppelinConfig);
        }
        else {
            throw new UnsupportedOperationException("Selected command is not supported. " +
                    "Supported commands: get system version, exec syslog stream, exec hdfs save, exec hdfs load, exec hdfs list, " +
                    "exec hdfs delete, kafka save, exec bloom create, exec bloom update.");
        }

        return rv;
    }
}
